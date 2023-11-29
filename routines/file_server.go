package routines

import (
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

type FileService struct {
	Port                int
	Filename2FileMaster map[string]*FileMaster
	SdfsFolder          string
	LocalFileFolder     string
	Report              util.FileServerMetadataReport
	reportLock 		    sync.RWMutex
}

type CopyArgs struct {
	LocalFilePath  string
	RemoteFilePath string
	RemoteAddr     string
}

type RWArgs struct {
	TransmissionId string
	LocalFilename  string
	SdfsFilename   string
	ClientAddr     string
	ReceiverTag    uint8
	WriteMode      uint8
}

type CreateFMArgs struct {
	Filename string
	Servants []string
}

type DeleteArgs struct {
	Filename string
}

type SendArgs struct {
	LocalFilePath  string
	RemoteFileName string
	RemoteAddr     string
	TransmissionId string
	ReceiverTag    uint8
	WriteMode      uint8
}

func NewFileService(port int, homedir string, serverHostnames []string) *FileService {
	MEMBERSHIP_SERVER_STARTED.Wait()

	this := new(FileService)
	this.Port = port
	this.Filename2FileMaster = make(map[string]*FileMaster)
	this.SdfsFolder = homedir + "/sdfs/"
	this.LocalFileFolder = homedir + "/local/"
	this.Report = util.FileServerMetadataReport{
		NodeId:      SelfNodeId,
		FileEntries: make([]util.FileInfo, 0),
	}

	util.EmptyFolder(this.SdfsFolder)

	return this
}

func (this *FileService) Register() {
	rpc.Register(this)
}

func (fm *FileService) CheckWriteCompleted(transmissionId *string, reply *string) error {

	timeout := time.After(300 * time.Second)
	for {
		time.Sleep(2 * time.Second)
		select {
		case <-timeout:
			*reply = ""
			return nil
		default:
			if FileTransmissionProgressTracker.IsGlobalCompleted(*transmissionId) { // received file, send it to servants
				*reply = "ACK"
				return nil
			}
		}
	}
}

// reroute to the corresponding file master
func (this *FileService) ReadFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.ReadFile(args)
	} else {
		log.Fatal("No corresponding filemaster for " + args.SdfsFilename)
	}
	return nil
}

// reroute to the corresponding file master
func (this *FileService) WriteFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.WriteFile(args.SdfsFilename, reply)
	} else {
		log.Fatal("No corresponding filemaster for " + args.SdfsFilename)
	}
	return nil
}

// reroute to the corresponding file master
func (this *FileService) ReplicateFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.ReplicateFile(args.ClientAddr)
	} else {
		log.Fatal("No corresponding filemaster for " + args.SdfsFilename)
	}
	return nil
}

// reroute to the corresponding file master
func (this *FileService) DeleteFile(args *DeleteArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.Filename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.DeleteFile()
	} else {
		log.Fatal("No corresponding filemaster for " + args.Filename)
	}
	return nil
}

func (this *FileService) DeleteLocalFile(args *DeleteArgs, reply *string) error {
	this.ChangeReportStatusPendingDelete(args.Filename)
	err := util.DeleteFile(args.Filename, this.SdfsFolder)
	if err != nil {
		return err
	}
	this.RemoveFromReport(args.Filename)
	return nil

}

func (this *FileService) RemoveFromReport(filename string) {
	this.reportLock.Lock()
	defer this.reportLock.Unlock()

	currEntries := this.Report.FileEntries
	for i, fileinfo := range currEntries {
		if fileinfo.FileName == filename {
			this.Report.FileEntries = append(currEntries[:i], currEntries[i+1:]...)
		}
	}
}

func (this *FileService) ChangeReportStatusPendingDelete(filename string) {
	this.reportLock.Lock()
	defer this.reportLock.Unlock()
	
	for i, fileinfo := range this.Report.FileEntries {
		if fileinfo.FileName == filename {
			log.Printf("changed status to pending delete for file %s", fileinfo.FileName)
			this.Report.FileEntries[i].FileStatus = util.PENDING_DELETE
		}
	}
}

func (this *FileService) CreateFileMaster(args *CreateFMArgs, reply *string) error {
	fm := NewFileMaster(args.Filename, args.Servants, this.Port, this.SdfsFolder, this.LocalFileFolder, this)
	go fm.scheduler.StartScheduling()
	this.Filename2FileMaster[args.Filename] = fm
	return nil
}

func (this *FileService) ReportMetadata(args *string, reply *util.FileServerMetadataReport) error {
	// TODO: check all files that are pending and change status
	this.reportLock.RLock()
	defer this.reportLock.RUnlock()

	for i, fileInfo := range this.Report.FileEntries {
		if fileInfo.FileStatus == util.PENDING_FILE_UPLOAD || fileInfo.FileStatus == util.WAITING_REPLICATION {
			// check if file is in sdfs folder
			_, err := os.Stat(this.SdfsFolder + fileInfo.FileName)
			if err == nil {
				log.Println("file status changed to complete")
				// file is in folder
				this.Report.FileEntries[i].FileStatus = util.COMPLETE
			}
		}
	}

	reply.NodeId = SelfNodeId
	reply.FileEntries = this.Report.FileEntries

	return nil
}

func (this *FileService) UpdateMetadata(nodeToFiles *util.NodeToFiles, reply *string) error {
	updatedFileEntries := (*nodeToFiles)[SelfNodeId]
	fileToClusters := util.Convert2(nodeToFiles)

	this.reportLock.Lock()
	
	filename2fileInfo := make(map[string]util.FileInfo)
	for _, fileInfo := range this.Report.FileEntries {
		filename2fileInfo[fileInfo.FileName] = fileInfo
	}

	// replication instructed by metadata service
	filesToRepair := make([]string, 0)

	for _, updatedFileInfo := range updatedFileEntries {
		currFileInfo, ok := filename2fileInfo[updatedFileInfo.FileName]
		needToCreateFm := false
		if ok {
			if currFileInfo.IsMaster {
				// is master of this file, update servant list
				fileMaster, exists1 := this.Filename2FileMaster[updatedFileInfo.FileName]
				cluster, exists2 := (*fileToClusters)[updatedFileInfo.FileName]
				if exists1 && exists2{
					fileMaster.UpdateServantIps(cluster.GetServantIps())
				}
			} else if !currFileInfo.IsMaster && updatedFileInfo.IsMaster {
				// promoted to master
				// set is Master to true and create a new filemaster
				for idx, fileInfo := range this.Report.FileEntries {
					if fileInfo.FileName == currFileInfo.FileName {
						this.Report.FileEntries[idx].IsMaster = true
						break
					}
				}
				needToCreateFm = true
			}
		} else {
			// new file
			addToReport := true
			if updatedFileInfo.IsMaster {
				needToCreateFm = true
			} else if updatedFileInfo.FileStatus == util.WAITING_REPLICATION {
				cluster := (*fileToClusters)[updatedFileInfo.FileName]

				if cluster.Master == nil {
					log.Print("Warn: master is nil. Servant cannot replicate")
				}

				// failure repair
				// when master's status == PENDING_FILE_UPLOAD, it indicates a new file is uploaded to sdfs
				// fm will handle writing to all services, so there is no need to do anything
				if cluster.Master != nil && cluster.Master.FileStatus == util.COMPLETE {
					filesToRepair = append(filesToRepair, updatedFileInfo.FileName)
				} else if cluster.Master != nil && cluster.Master.FileStatus == util.PENDING_DELETE {
					log.Println("here in delete")
					// if master is in the process of executing a delete, do not add to self report
					addToReport = false
				}

			}
			if addToReport {
				this.Report.FileEntries = append(this.Report.FileEntries, *updatedFileInfo)
			}
		}

		if needToCreateFm {
			createArgs := &CreateFMArgs{
				Filename: updatedFileInfo.FileName,
				Servants: util.GetServantIps(fileToClusters, updatedFileInfo.FileName),
			}
			var reply string
			err := this.CreateFileMaster(createArgs, &reply)
			if err != nil {
				log.Println("Error when creating file master ", err)
				return err
			}
		}
	}

	this.reportLock.Unlock()


	calls := make([]*rpc.Call, 0)
	for _, fileName := range filesToRepair {
		cluster, exists := (*fileToClusters)[fileName]
		if !exists || cluster == nil || cluster.Master == nil{
			log.Printf("Failed to look up file master while repairing for file: %s", fileName)
			continue
		}
		args := &RWArgs{
			LocalFilename: fileName,
			SdfsFilename:  fileName,
			ClientAddr:    util.NodeIdToIP(SelfNodeId),
		}
		masterIp := util.NodeIdToIP(cluster.Master.NodeId)
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", masterIp, config.RpcServerPort))
		if err != nil {
			log.Println("Error dailing master when trying to retrieve replica", err)
			return err
		}
		var reply string
		call := client.Go("FileService.ReplicateFile", args, &reply, nil)
		calls = append(calls, call)
	}

	replicationTimeout := time.After(60 * time.Second)
	for {
		complete := true
		for i, call := range calls {
			select {
			case <-replicationTimeout:
				*reply = "FAILED"
				log.Print("Timeout repairing files")
				return errors.New("Timeout repairing files")
			default:
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
		}
		if complete {
			break
		}
	}

	*reply = "ACK"
	return nil
}

func (this *FileService) SendFileToClient(args *SendArgs, reply *string) error {
	return SendFile(args.LocalFilePath, args.RemoteFileName, args.RemoteAddr, args.TransmissionId, args.ReceiverTag, args.WriteMode)
}
