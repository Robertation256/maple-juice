package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"log"
	"math/rand"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const (
	LOCAL_WRITE_COMPLETE int = 1
	GLOBAL_WRITE_COMPLETE   int = 2

	MAX_TASK_PREEMPTION_NUM uint32 = 4

	FILE_READ_TIMEOUT_SECONDS int = 120
	FILE_WRITE_TIMEOUT_SECONDS int = 240
	FILE_DELETE_TIMEOUT_SECONDS int = 60
)

type FileMaster struct {
	// // main queue: store the requests that are waiting in the order they are received
	// Queue []*Request
	// // write queue: keep track of write requests that are waiting
	// WriteQueue []*Request
	// // number of nodes that are currently reading the file
	// CurrentRead int
	// // number of nodes that are currently writing the file
	// CurrentWrite int
	scheduler *util.FileOperationScheduler
	Filename     string
	// list of servant ip addresses
	servants []string			
	servantsLock sync.RWMutex
	// address of self. used to prevent errors in case fm = client
	SelfAddr string


	FileServerPort  int
	SdfsFolder      string
	LocalFileFolder string
	FileServer      *FileService

	transmissionIdGenerator *util.TransmissionIdGenerator
}


type Request struct {
	// type of request: read (R), write (W), delete (D)
	Type string
	// a flag indicating whether the request is still in queue
	InQueue bool
	// how many rounds a write has been waiting for consecutive read
	WaitRound int
}

func NewFileMaster(filename string, servants []string, fileServerPort int, sdfsFolder string, localFileFlder string, fileServer *FileService) *FileMaster {
	//FileMasterProgressTracker = NewProgressManager()
	selfAddr := util.NodeIdToIP(SelfNodeId)
	return &FileMaster{
		// CurrentRead:     0,
		// CurrentWrite:    0,
		scheduler: util.NewFileOperationScheduler(MAX_TASK_PREEMPTION_NUM),
		Filename:        filename,
		servants:        servants,
		SelfAddr:        selfAddr,
		FileServerPort:  fileServerPort,
		SdfsFolder:      sdfsFolder,
		LocalFileFolder: localFileFlder,
		FileServer:      fileServer,
		transmissionIdGenerator: util.NewTransmissionIdGenerator("FM-"+SelfNodeId),
	}
}

func (this *FileMaster) GetServantIps() []string {
	this.servantsLock.RLock()
	defer this.servantsLock.RUnlock()
	return this.servants
}

func (this *FileMaster) UpdateServantIps(ips []string) {
	this.servantsLock.Lock()
	defer this.servantsLock.Unlock()
	this.servants = ips
}



// func (fm *FileMaster) CheckQueue() {

// 	if len(fm.Queue) > 0 {
// 		if !fm.Queue[0].InQueue {
// 			// if head of queue has InQueue = false. pop it and move on to the next one
// 			fm.Queue = fm.Queue[1:]
// 			fm.CheckQueue()
// 			return
// 		}
// 		if fm.Queue[0].Type == "R" {
// 			// satisfy read condition (current reader < 2 and no writer)
// 			if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
// 				// no write has been waiting for more than 4 consecutive read
// 				if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
// 					fm.Queue[0].InQueue = false
// 					fm.Queue = fm.Queue[1:]
// 					return
// 				} else {
// 					// if a write has been waiting for 4 consecutive read, pop that write request
// 					// note now queue might have a request that have InQueue = false
// 					fm.WriteQueue[0].InQueue = false
// 					fm.WriteQueue = fm.WriteQueue[1:]
// 				}
// 			}
// 		} else {
// 			// write request or delete request
// 			if fm.CurrentRead == 0 && fm.CurrentWrite == 0 {
// 				// write condition satisifed: no reader and no writer (write-write and read-write are both conflict operations)
// 				fm.WriteQueue[0].InQueue = false
// 				fm.WriteQueue = fm.WriteQueue[1:]

// 				fm.Queue[0].InQueue = false
// 				fm.Queue = fm.Queue[1:]
// 			} else if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
// 				// write is blocked because there is exactly 1 reader
// 				// then allow another read request, if write has not been waiting for more than 4 consecutive rounds
// 				for _, request := range fm.Queue {
// 					if request.Type == "R" && fm.WriteQueue[0].WaitRound < 4 {
// 						request.InQueue = false
// 					}
// 				}
// 			}
// 		}
// 	}
// }

func (fm *FileMaster) ReadFile(args *RWArgs) error {
	timeout := time.Duration(FILE_READ_TIMEOUT_SECONDS)*time.Second
	operation := func() error {
		return fm.executeRead(args)
	}

	task := util.NewFileOperation(util.FILE_OP_READ, operation, &timeout)
	fm.scheduler.AddTask(task)

	return <- task.ResponseChan

	// var request *Request = nil
	// for {
	// 	// if the request is not in queue and read condition (reader < 2 and no writer) satisfied
	// 	if request == nil && fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
	// 		// no write has been waiting for more than 4 consecutive read
	// 		if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
	// 			return fm.executeRead(args)
	// 		}
	// 	} else if request == nil {
	// 		// initial condition to execute the read is not satifised. add to queue
	// 		request = &Request{
	// 			Type:    "R",
	// 			InQueue: true,
	// 		}
	// 		fm.Queue = append(fm.Queue, request)
	// 	} else if request != nil && !request.InQueue {
	// 		// request has been poped from queue, execute read
	// 		return fm.executeRead(args)
	// 	}
	// }
}

func (fm *FileMaster) executeRead(args *RWArgs) error {
	// fm.CurrentRead += 1
	// // every request in the wait queue has been forced to wait another one round because of
	// // the read that is currently executing
	// for _, writeRequest := range fm.WriteQueue {
	// 	writeRequest.WaitRound += 1
	// }

	localFilePath := fm.SdfsFolder + fm.Filename

	sendArgs := &SendArgs{
		LocalFilePath: localFilePath,
		RemoteFileName: args.LocalFilename,
		RemoteAddr: args.ClientAddr+":"+strconv.Itoa(config.FileReceivePort),
		TransmissionId: args.TransmissionId,
		ReceiverTag: args.ReceiverTag,
		WriteMode: args.WriteMode,
	}

	var reply string
	servants := fm.GetServantIps()

	numServants := len(servants)
	// select a random servant to send client the file
	servant := servants[rand.Intn(numServants)]
	var err error
	needToResend := false
	client := dial(servant, fm.FileServerPort)
	if client == nil {
		log.Println("Error dialing servant:")
		needToResend = true
	} else {
		err = client.Call("FileService.SendFileToClient", sendArgs, &reply)
		if err != nil {
			// some error occured, this might be casued by servant doesn't have the replica yet, etc.
			log.Println("Servant failed to send file: ", err)
			needToResend = true
		}
	}

	if needToResend {
		// if the servant failed to send the file for some reason, the file master will do it instead
		err = SendFile(localFilePath, args.LocalFilename, args.ClientAddr+":"+strconv.Itoa(config.FileReceivePort), args.TransmissionId, args.ReceiverTag, args.WriteMode)
	}

	// TODO: check if sendfile returned any error?


	// fm.CurrentRead -= 1
	// fm.CheckQueue()
	return err
}

func (fm *FileMaster) ReplicateFile(clientAddr string) error {
	timeout := time.Duration(FILE_READ_TIMEOUT_SECONDS)*time.Second
	operation := func() error {
		return fm.executeReplicate(clientAddr)
	}

	task := util.NewFileOperation(util.FILE_OP_READ, operation, &timeout)
	fm.scheduler.AddTask(task)

	return <- task.ResponseChan
	// var request *Request = nil
	// for {
	// 	// if the request is not in queue and read condition (reader < 2 and no writer) satisfied
	// 	if request == nil && fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
	// 		// no write has been waiting for more than 4 consecutive read
	// 		if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
	// 			return fm.executeReplicate(clientAddr)
	// 		}
	// 	} else if request == nil {
	// 		// initial condition to execute the read is not satifised. add to queue
	// 		request = &Request{
	// 			Type:    "R",
	// 			InQueue: true,
	// 		}
	// 		fm.Queue = append(fm.Queue, request)
	// 	} else if request != nil && !request.InQueue {
	// 		// request has been poped from queue, execute read
	// 		return fm.executeReplicate(clientAddr)
	// 	}
	// }
}

func (fm *FileMaster) executeReplicate(clientAddr string) error {
	// fm.CurrentRead += 1
	// // every request in the wait queue has been forced to wait another one round because of
	// // the read that is currently executing
	// for _, writeRequest := range fm.WriteQueue {
	// 	writeRequest.WaitRound += 1
	// }

	// copy to servant's sdfs folder
	localFilePath := fm.SdfsFolder + fm.Filename
	SendFile(localFilePath, fm.Filename, clientAddr+":"+strconv.Itoa(config.FileReceivePort), "ignore", RECEIVER_SDFS_FILE_SERVER, WRITE_MODE_TRUNCATE)

	// fm.CurrentRead -= 1
	// fm.CheckQueue()
	return nil
}

func (fm *FileMaster) WriteFile(clientFilename string, reply *string) error {
	transmissionId := fm.transmissionIdGenerator.NewTransmissionId(clientFilename)
	*reply = transmissionId

	timeout := time.Duration(FILE_WRITE_TIMEOUT_SECONDS)*time.Second
	startSig := make(chan struct{}, 1)
	operation := func() error {
		startSig <- struct{}{}
		return fm.executeWrite(transmissionId)
	}

	task := util.NewFileOperation(util.FILE_OP_WRITE, operation, &timeout)
	fm.scheduler.AddTask(task)

	//block until write task is started
	<-startSig
	return nil

	// var request *Request = nil
	// for {
	// 	// requests just come in, and the condition for write is satisfied
	// 	if request == nil && fm.CurrentWrite == 0 && fm.CurrentRead == 0 {
	// 		// if there is no other write pending, simply execute
	// 		if len(fm.WriteQueue) == 0 {
	// 			return fm.executeWrite(clientFilename, reply)
	// 		}
	// 	} else if request == nil {
	// 		// otherwise add to queue
	// 		request = &Request{
	// 			Type:    "W",
	// 			InQueue: true,
	// 		}
	// 		fm.Queue = append(fm.Queue, request)
	// 		fm.WriteQueue = append(fm.WriteQueue, request)
	// 	} else if request != nil && !request.InQueue {
	// 		return fm.executeWrite(clientFilename, reply)
	// 	}
	// }
}

func (fm *FileMaster) executeWrite(transmissionId string) error {
	// fm.CurrentWrite += 1
	servants := fm.GetServantIps()
	timeout := time.After(120 * time.Second)
	var response error
	for {
		select {
		case <-timeout:
			log.Print("Client did not finish uploading file to master in 120s")
			// note: we need expire a transmission id token in future work
			return errors.New("Client did not finish uploading file to master in 120s")
		default:
			if FileTransmissionProgressTracker.IsLocalCompleted(transmissionId){	// received file, send it to servants
				remainingServants := len(servants)
				resChan := make(chan error, remainingServants)
				for _, servant := range servants {
					// todo: add resolution when a servant failed
					go func(servantIp string){
						resChan <- SendFile(config.SdfsFileDir + fm.Filename, fm.Filename, servantIp+":"+strconv.Itoa(config.FileReceivePort), transmissionId, RECEIVER_SDFS_FILE_SERVER, WRITE_MODE_TRUNCATE)
					}(servant)
				}

				for remainingServants > 0 {
					select {
					case err := <-resChan:
						if err != nil {
							response = err
						}
						remainingServants--;
					}
				}

				log.Print("Global write completed")
				FileTransmissionProgressTracker.GlobalComplete(transmissionId)
				return response
			}
		}
		time.Sleep(100 * time.Millisecond) // check if client finished uploading every 100ms
	}


	// fm.CurrentWrite -= 1
	// fm.CheckQueue()
}

func (fm *FileMaster) DeleteFile() error {
	timeout := time.Duration(FILE_DELETE_TIMEOUT_SECONDS)*time.Second
	operation := func() error {
		return fm.executeDelete()
	}

	task := util.NewFileOperation(util.FILE_OP_WRITE, operation, &timeout)
	fm.scheduler.AddTask(task)

	return <- task.ResponseChan
	
	// var request *Request = nil
	// for {
	// 	// requests just come in, and the condition for delete is satisfied
	// 	if request == nil && fm.CurrentWrite == 0 && fm.CurrentRead == 0 {
	// 		// if there is no other write pending, simply execute
	// 		if len(fm.WriteQueue) == 0 {
	// 			return fm.executeDelete()
	// 		}
	// 	} else if request == nil {
	// 		// otherwise add to queue. treat delete same as write, so add it to write queue too
	// 		request = &Request{
	// 			Type:    "D",
	// 			InQueue: true,
	// 		}
	// 		fm.Queue = append(fm.Queue, request)
	// 		fm.WriteQueue = append(fm.WriteQueue, request)
	// 	} else if request != nil && !request.InQueue {
	// 		return fm.executeDelete()
	// 	}
	// }
}

func (fm *FileMaster) executeDelete() error {

	metadataClient := dialMetadataService()
	if metadataClient == nil {
		return errors.New("Cannot connect to metadata service")
	}

	defer metadataClient.Close()

	fileName := fm.Filename
	reply := ""

	// request a tombstone first so that metadata service stops repairing this file
	err := metadataClient.Call("FileMetadataService.RequestTombstone", &fileName, &reply)
	if err != nil {
		return err
	}


	servants := fm.GetServantIps()
	util.DeleteFile(fm.Filename, fm.SdfsFolder)
	fm.FileServer.ChangeReportStatusPendingDelete(fm.Filename)

	calls := make([]*rpc.Call, len(servants))

	// todo: add tcp connection pool
	for idx, servant := range servants {
		client := dial(servant, fm.FileServerPort)
		if client == nil {
			log.Println("Error dialing servant at " + servant)
			return errors.New("Error dialing servant at " + servant)
		}
		deleteArgs := DeleteArgs{
			Filename: fm.Filename,
		}
		var reply string
		calls[idx] = client.Go("FileService.DeleteLocalFile", deleteArgs, &reply, nil)
	}

	// iterate and look for completed rpc calls
	for {
		complete := true
		for i, call := range calls {
			if call != nil {
				select {
				case _, ok := <-call.Done: // check if channel has output ready
					if !ok {
						log.Println("Channel closed for async rpc call")
					}
					calls[i] = nil
				default:
					complete = false
				}
			}
		}
		if complete {
			break
		}
	}

	fm.FileServer.RemoveFromReport(fm.Filename)

	// release the tombstone so that later files with the same name is not ignored
	err = metadataClient.Call("FileMetadataService.ReleaseTombstone", &fileName, &reply)
	if err != nil {
		return err
	}

	log.Printf("Global delete completed for file %s", fm.Filename)
	return nil

}