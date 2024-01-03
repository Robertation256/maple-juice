package dfs

import (
	"maple-juice/config"
	"maple-juice/membership"
	"maple-juice/util"
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


func NewFileMaster(filename string, servants []string, fileServerPort int, sdfsFolder string, localFileFlder string, fileServer *FileService) *FileMaster {
	selfAddr := util.NodeIdToIP(membership.SelfNodeId)
	return &FileMaster{
		scheduler: util.NewFileOperationScheduler(MAX_TASK_PREEMPTION_NUM),
		Filename:        filename,
		servants:        servants,
		SelfAddr:        selfAddr,
		FileServerPort:  fileServerPort,
		SdfsFolder:      sdfsFolder,
		LocalFileFolder: localFileFlder,
		FileServer:      fileServer,
		transmissionIdGenerator: util.NewTransmissionIdGenerator("FM-"+membership.SelfNodeId),
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


func (fm *FileMaster) ReadFile(args *RWArgs) error {
	timeout := time.Duration(FILE_READ_TIMEOUT_SECONDS)*time.Second
	operation := func() error {
		return fm.executeRead(args)
	}

	task := util.NewFileOperation(util.FILE_OP_READ, operation, &timeout)
	fm.scheduler.AddTask(task)

	return <- task.ResponseChan
}

func (fm *FileMaster) executeRead(args *RWArgs) error {
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
	client := util.Dial(servant, fm.FileServerPort)
	if client == nil {
		log.Println("Error dialing servant:")
		needToResend = true
	} else {
		err = client.Call("FileService.SendFileToClient", sendArgs, &reply)
		if err != nil {
			// some error occured, this might be casued by servant doesn't have the replica yet, etc.
			log.Println("Info: servant file replication in progress. Switching back to file master to file transfer")
			needToResend = true
		}
	}

	if needToResend {
		// if the servant failed to send the file for some reason, the file master will do it instead
		err = SendFile(localFilePath, args.LocalFilename, args.ClientAddr+":"+strconv.Itoa(config.FileReceivePort), args.TransmissionId, args.ReceiverTag, args.WriteMode)
	}

	// TODO: check if sendfile returned any error
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
}

func (fm *FileMaster) executeReplicate(clientAddr string) error {

	localFilePath := fm.SdfsFolder + fm.Filename
	SendFile(localFilePath, fm.Filename, clientAddr+":"+strconv.Itoa(config.FileReceivePort), "ignore", RECEIVER_SDFS_FILE_SERVER, WRITE_MODE_TRUNCATE)

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
}

func (fm *FileMaster) DeleteFile() error {
	timeout := time.Duration(FILE_DELETE_TIMEOUT_SECONDS)*time.Second
	operation := func() error {
		return fm.executeDelete()
	}

	task := util.NewFileOperation(util.FILE_OP_WRITE, operation, &timeout)
	fm.scheduler.AddTask(task)

	return <- task.ResponseChan

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
		client := util.Dial(servant, fm.FileServerPort)
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