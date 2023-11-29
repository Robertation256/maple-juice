package routines

import (
	"bufio"
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TASK_MAX_RETY_NUM int = 3 // maximum number of retry for each maple/juice sub task


	FILE_PARTITION_BUF_SIZE    int = 32 * 1024
	MAPLE_TASK_TIMEOUT_MINUTES int = 5
	JUICE_TASK_TIMEOUT_MINUTES int = 5
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
		workerNode2Tasks:        make(map[string][]string),	
		transmissionIdGenerator: util.NewTransmissionIdGenerator("MR-JM-" + SelfNodeId),
	}
}

func (this *MRJobManager) SubmitJob(jobRequest *util.JobRequest, reply *string) error {
	if SelfNodeId != LeaderId {
		return errors.New("Please contact leader for Maple Juice job submission")
	}

	jobRequest.ErrorMsgChan = make(chan error, 1)
	this.jobQueue <- jobRequest

	timeout := time.After(10 * time.Minute)

	for {
		select {
		case <-timeout:
			return errors.New("Job execution times out")
		case err := <-jobRequest.ErrorMsgChan: // job completes with/ without error
			if err != nil {
				return err
			}
			*reply = "ACK"
			return nil
		}
	}
}

// register this rpc service and start main thread
func (this *MRJobManager) Register() {
	rpc.Register(this)
	err := util.EmptyFolder(config.JobManagerFileDir)
	if err != nil {
		log.Print("Failed to clean up job manager file folder", err)
	}

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
	log.Printf("Finished fetching input file")

	// stage 2: profile input file
	lineCount, err := util.GetFileLineCount(config.JobManagerFileDir + inputFileName)
	if err != nil {
		*errorMsgChan <- err
		return
	}

	log.Printf("Maple input file %s contains %d lines", inputFileName, lineCount)

	// this should never happen
	if lineCount < job.TaskNum {
		log.Print("WARN: Maple input file contains less lines than the number of tasks, auto reducing task number...")
		job.TaskNum = lineCount
	}

	//stage3: partition input file and start Maple workers
	linesPerWorker := lineCount / job.TaskNum
	remainder := lineCount % job.TaskNum
	file, err := os.Open(config.JobManagerFileDir + inputFileName)
	if err != nil {
		*errorMsgChan <- err
		return
	}
	defer file.Close()

	isTaskCompleted := make([]bool, job.TaskNum)
	retryNum := make([]int, job.TaskNum)
	taskResultChans := make([]chan error, job.TaskNum)
	for idx := 0; idx<job.TaskNum; idx++ {
		taskResultChans[idx] = make(chan error, 1)
		retryNum[idx] = 0
	}

	scanner := bufio.NewScanner(file)

	for taskNumber := 0; taskNumber < job.TaskNum; taskNumber++ {
		lineNum := linesPerWorker
		if remainder > 0 {
			lineNum += 1
			remainder -= 1
		}
		partitionName := util.FmtMapleInputPartitionName(job.SrcSdfsFileName, taskNumber)
		err := util.PartitionFile(scanner, lineNum, config.JobManagerFileDir+partitionName)
		if err != nil {
			*errorMsgChan <- err
			return
		}
		log.Printf("Starting initial maple task %d", taskNumber)
		
		go this.startMapleWorker(taskNumber, job, &taskResultChans[taskNumber])
	}

	// stage 4: track Maple worker progress and reschedule for failed tasks
	jobCompleted := false

	for !jobCompleted {
		time.Sleep(1 * time.Second) // check every second
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
					log.Print(fmt.Sprintf("Maple task %d completed with error: ", taskNumber), err)
					if (retryNum[taskNumber] >= TASK_MAX_RETY_NUM){
						*errorMsgChan <- errors.New(fmt.Sprintf("Failing Maple task:  task %d failed after %d retries", taskNumber, retryNum[taskNumber]))
						return
					}
					// reschedule
					
					jobCompleted = false
					retryNum[taskNumber]++
					go this.startMapleWorker(taskNumber, job, &taskResultChans[taskNumber])
				} else {
					// task completed
					isTaskCompleted[taskNumber] = true
					log.Printf("Maple sub task %d completed", taskNumber)
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
		InputFileName:       util.FmtMapleInputPartitionName(job.SrcSdfsFileName, taskNumber),
		ExcecutableFileName: job.ExcecutableFileName,
		OutputFilePrefix:    job.OutputFilePrefix,
	}

	// send parition to worker
	taskArg.TransmissionId = this.transmissionIdGenerator.NewTransmissionId(taskArg.InputFileName)
	partitionFileName := taskArg.InputFileName
	workerAddr := workerIp + ":" + strconv.Itoa(config.FileReceivePort)
	log.Printf("Send out partition %s with transmission id %s", partitionFileName, taskArg.TransmissionId)
	err := SendFile(config.JobManagerFileDir+partitionFileName,
		partitionFileName, workerAddr, taskArg.TransmissionId, RECEIVER_MR_NODE_MANAGER, WRITE_MODE_TRUNCATE)
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

	// stage 1: list all files related to each key
	filePrefix := job.SrcSdfsFilePrefix
	filePrefix = strings.Replace(filePrefix, ".", "\\.", -1)	// escape dots in regex

	// Maple outputs should be <file_name>-p<partition_num>-<key> 
	// file name and key should not contain dash
	regexStr := filePrefix + "-p\\d+-.+"						
	_, err := regexp.Compile(regexStr)
	if err != nil {
		*errorMsgChan <- err 
		return
	}

	matchedFiles, err := SDFSSearchFileByRegex(regexStr)
	if err != nil {
		*errorMsgChan <- err 
		return
	}

	log.Printf("Matched %d files", len(*matchedFiles))

	// group file names by key
	keyToFiles := make(map[string][]string) 
	for _, fileName := range *matchedFiles{
		log.Printf("Matched files: %s", fileName)
		splitted := strings.Split(fileName, "-")
		if len(splitted) > 0 {
			key := splitted[len(splitted)-1]
			files, exists := keyToFiles[key]
			if ! exists {
				files = make([]string, 0)
			}
			files = append(files, fileName)
			keyToFiles[key] = files
		}
	} 

	keys := make([]string, 0)
	for k := range keyToFiles {
		keys = append(keys, k)
	}

	
	// stage 2: assign keys to worker nodes
	if len(keys) == 0 {
		log.Print("Juice input files not found")
		*errorMsgChan <- errors.New("Juice input files not found")
		return
	}

	if len(keys) < job.TaskNum {
		log.Print("WARN: Juice input contains less keys than the number of tasks, auto reducing task number... ")
		job.TaskNum = len(keys)
	}

	

	var partitions []map[string][]string 
	if job.IsHashPartition {
		partitions = partitionByHash(keyToFiles, job.TaskNum)
	} else {
		partitions = partitionByRange(keyToFiles, job.TaskNum)
	}

	// stage 3: start juice workers
	isTaskCompleted := make([]bool, job.TaskNum)
	taskResultChans := make([]chan error, job.TaskNum)
	retryNum := make([]int, job.TaskNum)
	for idx := range taskResultChans {
		taskResultChans[idx] = make(chan error, 1)
		retryNum[idx] = 0
	}
	for taskNumber, partition := range partitions {
		go this.startJuiceWorker(taskNumber, partition, job, &taskResultChans[taskNumber])
	}

	// stage 4: track Juice worker progress and reschedule for failed tasks
	jobCompleted := false
	for !jobCompleted {
		jobCompleted = true
		for taskNumber := 0; taskNumber < job.TaskNum; taskNumber++ {
			if isTaskCompleted[taskNumber] {
				continue
			}
			taskId := fmtTaskId(job.SrcSdfsFilePrefix, false, taskNumber)

			select {
			case err := <-taskResultChans[taskNumber]:
				this.removeTask(taskId)
				if err != nil {
					log.Print(fmt.Sprintf("Juice task %d completed with error: ", taskNumber), err)
					if (retryNum[taskNumber] >= TASK_MAX_RETY_NUM){
						*errorMsgChan <- errors.New(fmt.Sprintf("Failing Juice task:  task %d failed after %d retries", taskNumber, retryNum[taskNumber]))
						return
					}
					// reschedule
					log.Print("Juice task " + taskId + " failed, rescheduling ...", err)
					jobCompleted = false
					retryNum[taskNumber]++
					go this.startJuiceWorker(taskNumber, partitions[taskNumber], job, &taskResultChans[taskNumber])
				} else {
					// task completed
					isTaskCompleted[taskNumber] = true
				}
			default:
				jobCompleted = false
			}
		}
		time.Sleep(1 * time.Second) // lets check for every second
	}

	if job.DeleteInput {
		cleanUpJuiceInput(job.SrcSdfsFilePrefix)
	}

	*errorMsgChan <- nil
}


func cleanUpJuiceInput(filePrefix string) error {
	filePrefix = strings.Replace(filePrefix, ".", "\\.", -1)
	fileNames, err := SDFSSearchFileByRegex(filePrefix+"-p\\d+-.+")
	if err != nil {
		return err 
	}

	var err1 error
	for _, fileName := range *fileNames{
		err1 = SDFSDeleteFile(fileName)
	}

	return err1
}

func (this *MRJobManager) startJuiceWorker(taskNumber int, parition map[string][]string, job *util.JuiceJobRequest, resultChan *chan error) {

	taskId := fmtTaskId(job.SrcSdfsFilePrefix, false, taskNumber)
	workerIp := this.assignTask(taskId)
	if len(workerIp) == 0 {
		*resultChan <- errors.New("Cannot find free worker") // this should never happen unless all worker nodes died
		return
	}

	taskArg := &util.JuiceTaskArg{
		InputFilePrefix: job.SrcSdfsFilePrefix,
		KeyToFileNames: parition,
		ExcecutableFileName: job.ExcecutableFileName,
		OutputFilePrefix: job.OutputFileName,
	}


	// instruct juice job start
	client := dial(workerIp, config.RpcServerPort)
	if client == nil {
		log.Printf("Cannot connect to node %s while starting Juice worker", workerIp)
		*resultChan <- errors.New("Cannot connect to node")
		return
	}

	defer client.Close()

	retFlag := ""

	call := client.Go("MRNodeManager.StartJuiceTask", taskArg, &retFlag, nil)
	if call.Error != nil {
		log.Printf("Encountered error while starting Juice task %s via RPC", taskId)
		*resultChan <- call.Error
		return
	}

	timeout := time.After(time.Duration(JUICE_TASK_TIMEOUT_MINUTES) * time.Minute)

	select {
	case <-timeout:
		*resultChan <- errors.New("Timeout executing Juice task" + taskId)
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
				errMsg := fmt.Sprintf("MR Job Master: Juice task %s failed", taskId)
				log.Print(errMsg)
				*resultChan <- errors.New(errMsg)
			}
			return
		}
	}
}

func (this *MRJobManager) listenForMembershipChange() {
	for {
		select {
		case event := <-util.MRJobManagerMembershipEventChan:
			nodeIp := util.NodeIdToIP(event.NodeId)
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

	taskList, exists := this.workerNode2Tasks[assigneeIP]
	if exists {
		taskList = append(taskList, taskId)
		this.workerNode2Tasks[assigneeIP] = taskList
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


func partitionByHash(keyToFiles map[string][]string, taskNum int) []map[string][]string {
	var fnvHash hash.Hash32 = fnv.New32a()
	buckets := make([]map[string][]string, taskNum)
	result := make([]map[string][]string, 0)
	for idx := range buckets {
		buckets[idx] = make(map[string][]string)
	}

	for key, files := range keyToFiles {
		fnvHash.Reset()
		fnvHash.Write([]byte(key))
		bucketId := int(fnvHash.Sum32())%taskNum
		buckets[bucketId][key] = files
	}

	for _, partition := range buckets {
		// it's possible that we miss some buckets
		if len(partition) > 0 {
			result = append(result, partition)
		}
	}
	return result
}


func partitionByRange(keyToFiles map[string][]string, taskNum int) []map[string][]string {
	if taskNum > len(keyToFiles){
		taskNum = len(keyToFiles)
	}

	keys := make([]string, 0)
	for key := range keyToFiles {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	result := make([]map[string][]string, taskNum)
	for idx := range result {
		result[idx] = make(map[string][]string)
	}

	d := len(keyToFiles) / taskNum
	r := len(keyToFiles) % taskNum
	k := 0

	for i:=0; i<len(result); i++ {
		bucketSize := d
		if r > 0 {
			bucketSize += 1
			r -= 1
		}

		for ; bucketSize > 0; bucketSize-- {
			key := keys[k]
			k += 1
			result[i][key] = keyToFiles[key]
		}
	}

	return result
}
