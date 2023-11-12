package routines

import (
	"bufio"
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	FILE_PARTITION_BUF_SIZE    int = 32 * 1024
	MAPLE_TASK_TIMEOUT_MINUTES int = 5
)

// hosted by leader, does the following:
// 1. accepts and queue client submitted Maple/Juice jobs
// 2. partitions input file for each Maple task
// 3. assigns keys for each Juice task
// 4. re-schedule in case of failure

type MRJobManager struct {
	jobQueue                chan *util.JobRequest
	filePartitionBuf        []byte
	workerNode2Tasks        map[string][]string // worker node ip -> task ids
	mapLock                 sync.Mutex
	transmissionIdGenerator *util.TransmissionIdGenerator
}

func NewMRJobManager() *MRJobManager {
	return &MRJobManager{
		jobQueue:                make(chan *util.JobRequest, 100),
		filePartitionBuf:        make([]byte, FILE_PARTITION_BUF_SIZE),
		transmissionIdGenerator: util.NewTransmissionIdGenerator("MR-JM-" + SelfNodeId),
	}
}

func (this *MRJobManager) SubmitJob(jobRequest *util.JobRequest) error {
	if SelfNodeId != LeaderId {
		return errors.New("Please contact leader for Maple Juice job submission")
	}

	jobRequest.ErrorMsgChan = make(chan error, 1)
	this.jobQueue <- jobRequest

	timeout := time.After(5 * time.Minute)

	for {
		time.Sleep(2 * time.Second) // check every 2 seconds
		select {
		case <-timeout:
			return errors.New("Job execution times out")
		case err := <-jobRequest.ErrorMsgChan: // job completes with/ without error
			if err != nil {
				return err
			}
			return nil
		}
	}
}

// register this rpc service and start main thread
func (this *MRJobManager) Register() {
	rpc.Register(this)

	go this.listenForMembershipChange()

	// todo: add graceful termination
	go func() {
		for {
			select {
			case request := <-this.jobQueue:
				this.executeJob(request)
			}
		}
	}()
}

func (this *MRJobManager) executeJob(job *util.JobRequest) {
	if SelfNodeId != LeaderId {
		return
	}

	if job.IsMaple {
		this.executeMapleJob(&job.MapleJob, &job.ErrorMsgChan)
	} else {
		this.executeJuiceJob(&job.JuiceJob, &job.ErrorMsgChan)
	}
}

func (this *MRJobManager) executeMapleJob(job *util.MapleJobRequest, errorMsgChan *chan error) {
	// stage 1: fetch input file to local
	inputFileName := job.SrcSdfsFileName
	err := SDFSGetFile(inputFileName, inputFileName, RECEIVER_MR_JOB_MANAGER)
	if err != nil {
		*errorMsgChan <- err
		return
	}

	// stage 2: profile input file
	lineCount, err := util.GetFileLineCount(inputFileName)
	if err != nil {
		*errorMsgChan <- err
		return
	}

	//stage3: partition input file and start Maple workers
	linesPerWorker := lineCount / job.TaskNum
	remainder := lineCount % job.TaskNum
	file, err := os.Open(config.LocalFileDir + inputFileName)
	if err != nil {
		*errorMsgChan <- err
		return
	}
	defer file.Close()

	isTaskCompleted := make([]bool, job.TaskNum)
	taskResultChans := make([]chan error, job.TaskNum)
	for idx := range taskResultChans {
		taskResultChans[idx] = make(chan error, 1)
	}

	scanner := bufio.NewScanner(file)

	for taskNumber := 0; taskNumber < job.TaskNum; taskNumber++ {
		lineNum := linesPerWorker
		if remainder > 0 {
			lineNum += 1
			remainder -= 1
		}
		partitionName := util.MapleInputPartitionName(job.SrcSdfsFileName, taskNumber)
		err := util.PartitionFile(scanner, lineNum, config.JobManagerFileDir+partitionName)
		if err != nil {
			*errorMsgChan <- err
			return
		}

		go this.startMapleWorker(taskNumber, job, &taskResultChans[taskNumber])
	}

	// stage 4: track Maple worker progress and reschedule for failed tasks
	jobCompleted := false

	for !jobCompleted {
		time.Sleep(1 * time.Second) // lets check for every second
		jobCompleted = true
		for taskNumber := 0; taskNumber < job.TaskNum; taskNumber++ {
			if isTaskCompleted[taskNumber] {
				continue
			}
			taskId := fmtTaskId(job.SrcSdfsFileName, true, taskNumber)

			select {
			case err := <-taskResultChans[taskNumber]:
				this.removeTask(taskId)
				if err != nil {
					// task failed, reschedule
					jobCompleted = false
					go this.startMapleWorker(taskNumber, job, &taskResultChans[taskNumber])
				} else {
					// task completed
					isTaskCompleted[taskNumber] = true
				}
			default:
				jobCompleted = false
			}
		}
	}

	*errorMsgChan <- nil
}

func (this *MRJobManager) startMapleWorker(taskNumber int, job *util.MapleJobRequest, resultChan *chan error) {

	taskId := fmtTaskId(job.SrcSdfsFileName, true, taskNumber)
	workerIp := this.assignTask(taskId)
	if len(workerIp) == 0 {
		*resultChan <- errors.New("Cannot find free worker") // this should never happen unless all worker nodes died
		return
	}
	taskArg := &util.MapleTaskArg{
		InputFileName:       util.MapleInputPartitionName(job.SrcSdfsFileName, taskNumber),
		ExcecutableFileName: job.ExcecutableFileName,
		OutputFilePrefix:    job.OutputFilePrefix,
	}

	// send parition to worker
	taskArg.TransmissionId = this.transmissionIdGenerator.NewTransmissionId(taskArg.InputFileName)
	partitionFilePath := taskArg.InputFileName
	err := SendFile(partitionFilePath, partitionFilePath, workerIp, taskArg.TransmissionId, RECEIVER_MR_NODE_MANAGER)
	if err != nil {
		*resultChan <- err
		return
	}

	// instruct job start
	client := dial(workerIp, config.RpcServerPort)
	if client == nil {
		log.Printf("Cannot connect to node %s while starting Maple worker", workerIp)
		*resultChan <- errors.New("Cannot connect to node")
		return
	}

	defer client.Close()

	retFlag := ""

	call := client.Go("MRNodeManager.StartMapleTask", taskArg, &retFlag, nil)
	if call.Error != nil {
		log.Printf("Encountered error while starting Maple task %s via RPC", taskId)
		*resultChan <- call.Error
		return
	}

	timeout := time.After(time.Duration(MAPLE_TASK_TIMEOUT_MINUTES) * time.Minute)

	select {
	case <-timeout:
		*resultChan <- errors.New("Timeout executing Maple task" + taskId)
		return
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok {
			log.Println("MR Job Master: Channel closed for async rpc call")
			*resultChan <- errors.New("Unexpected connection break down")
			return
		} else {
			if retFlag == "ACK" {
				*resultChan <- nil
			} else {
				errMsg := fmt.Sprintf("MR Job Master: Maple task %s failed", taskId)
				log.Print(errMsg)
				*resultChan <- errors.New(errMsg)
			}
			return
		}
	}
}

func (this *MRJobManager) executeJuiceJob(job *util.JuiceJobRequest, errorMsgChan *chan error) {

}

func (this *MRJobManager) listenForMembershipChange() {
	for {
		select {
		case event := <-util.MRJobManagerMembershipEventChan:
			nodeIp := NodeIdToIP(event.NodeId)
			if event.IsNewJoin() {
				this.mapLock.Lock()
				this.workerNode2Tasks[nodeIp] = make([]string, 0)
				this.mapLock.Unlock()
			} else if event.IsOffline() {
				this.mapLock.Lock()
				_, exists := this.workerNode2Tasks[nodeIp]
				if exists {
					delete(this.workerNode2Tasks, nodeIp)
				}
				this.mapLock.Unlock()
			}
		}
	}
}

func fmtTaskId(fileName string, isMaple bool, taskNumber int) string {
	taskName := "maple-task"
	if !isMaple {
		taskName = "juice-task"
	}
	return fmt.Sprintf("%s-%s-%d", fileName, taskName, taskNumber)
}

// find the least busy worker to assign the task, return worker ip
func (this *MRJobManager) assignTask(taskId string) string {
	assigneeIP := ""
	taskNum := 0
	this.mapLock.Lock()
	defer this.mapLock.Unlock()

	for nodeIp, tasks := range this.workerNode2Tasks {
		if len(assigneeIP) == 0 || len(tasks) < taskNum {
			assigneeIP = nodeIp
			taskNum = len(tasks)
		}
	}

	return assigneeIP
}

// remove a task either due to completion or failure
func (this *MRJobManager) removeTask(taskId string) {
	this.mapLock.Lock()
	defer this.mapLock.Unlock()

	for nodeIp, tasks := range this.workerNode2Tasks {
		for idx, task := range tasks {
			if task == taskId {
				tasks = append(tasks[:idx], tasks[idx+1:]...)
				this.workerNode2Tasks[nodeIp] = tasks
				return
			}
		}
	}
}
