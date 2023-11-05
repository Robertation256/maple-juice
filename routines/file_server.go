package routines

import (
	"net/http"
	"net/rpc"
	"net"
	"golang.org/x/crypto/ssh"
	"cs425-mp2/config"
	"cs425-mp2/util"
	"fmt"
	"log"
	"os"
)

type FileService struct {
	Port 					int
	SshConfig 				*ssh.ClientConfig
	Filename2FileMaster 	map[string]*FileMaster
	SdfsFolder 				string
	LocalFileFolder			string
	Report 					util.FileServerMetadataReport
}

type CopyArgs struct {
	LocalFilePath 	string
	RemoteFilePath 	string
	RemoteAddr 		string
}

type RWArgs struct {
	LocalFilename 	string
	SdfsFilename 	string
	ClientAddr 		string
}

type CreateFMArgs struct {
	Filename 	string
	Servants 	[]string
}

type DeleteArgs struct {
	Filename string
}

func NewFileService(port int, homedir string) *FileService {
	MEMBERSHIP_SERVER_STARTED.Wait()

	this := new(FileService)
	this.Port = port
	this.SshConfig = &ssh.ClientConfig{
		User: config.SshUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.SshPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	this.Filename2FileMaster = make(map[string]*FileMaster)
	this.SdfsFolder = homedir + "/sdfs/"
	this.LocalFileFolder = homedir + "/local/"
	this.Report =  util.FileServerMetadataReport{
		NodeId: SelfNodeId,
		FileEntries: make([]util.FileInfo, 0),
	}

	return this
}

func (this *FileService) Start(){
	// TODO: integrate this with the grep server
	rpc.Register(this)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", this.Port))
	if err != nil {
		log.Fatal("Failed to start file server", err)
	}

	go http.Serve(l, nil)
}

func (this *FileService) Register(){
	rpc.Register(this)
}

// reroute to the corresponding file master
func (this *FileService) ReadFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.ReadFile(args.LocalFilename, args.ClientAddr)
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
		fm.WriteFile(args.LocalFilename, args.ClientAddr)
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

func (this *FileService) CopyFileToRemote(args *CopyArgs, reply *string) error {
	return util.CopyFileToRemote(args.LocalFilePath, args.RemoteFilePath, args.RemoteAddr, this.SshConfig)
}

func (this *FileService) DeleteLocalFile(args *DeleteArgs, reply *string) error {
	return util.DeleteFile(args.Filename, this.SdfsFolder)
}

func (this *FileService) CreateFileMaster(args *CreateFMArgs, reply *string) error{
	fm := NewFileMaster(args.Filename, args.Servants, this.SshConfig, this.Port, this.SdfsFolder, this.LocalFileFolder)
	this.Filename2FileMaster[args.Filename] = fm
	return nil
}

func (this *FileService) ReportMetadata(args *string, reply *util.FileServerMetadataReport) error{
	// TODO: check all files that are pending and change status

	for _, fileInfo := range this.Report.FileEntries {
		if fileInfo.FileStatus == util.PENDING_FILE_UPLOAD || fileInfo.FileStatus == util.WAITING_REPLICATION {
			// check if file is in sdfs folder
			_, err := os.Stat(this.SdfsFolder + fileInfo.FileName)
			if err == nil {
				// file is in folder
				fileInfo.FileStatus = util.COMPLETE
			}
		}
	}

	reply.NodeId = SelfNodeId
	reply.FileEntries = this.Report.FileEntries

	log.Printf("Node %s reported self metadata info", reply.NodeId)

	return nil
}

func (this *FileService) UpdateMetadata(fileToClusters *util.FileNameToCluster, reply *string) error {
	nodeToFiles := util.Convert(fileToClusters)
	updatedFileEntries := (*nodeToFiles)[SelfNodeId]

	filename2fileInfo := make(map[string]util.FileInfo)
	for _, fileInfo := range this.Report.FileEntries {
		filename2fileInfo[fileInfo.FileName] = fileInfo
	}

	for _, updatedFileInfo := range updatedFileEntries {
		currFileInfo, ok := filename2fileInfo[updatedFileInfo.FileName]
		needToCreateFm := false
		if ok {
			// promoted to master
			if !currFileInfo.IsMaster && updatedFileInfo.IsMaster {
				// set is Master to true and create a new filemaster
				currFileInfo.IsMaster = true
				needToCreateFm = true
			}
		} else {
			// new file
			if (updatedFileInfo.IsMaster) {
				needToCreateFm = true
			} else if (updatedFileInfo.FileStatus == util.WAITING_REPLICATION) {
				cluster := (*fileToClusters)[updatedFileInfo.FileName]
				// failure repair
				// when master's status == PENDING_FILE_UPLOAD, it indicates a new file is uploaded to sdfs
				// fm will handle writing to all services, so there is no need to do anything
				if (cluster.Master.FileStatus == util.COMPLETE) {
					masterIp := NodeIdToIP(cluster.Master.NodeId)
					client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", masterIp, config.RpcServerPort))
					if err != nil {
						log.Println("Error dailing master when trying to retrieve replica", err)
						return err
					}
					args := &RWArgs {
						LocalFilename: updatedFileInfo.FileName,
						SdfsFilename: updatedFileInfo.FileName,
						ClientAddr: NodeIdToIP(SelfNodeId),
					}
					var reply string
					client.Go("FileService.ReplicateFile", args, &reply, nil)
				}
			}
			this.Report.FileEntries = append(this.Report.FileEntries, *updatedFileInfo)
		}

		if (needToCreateFm) {
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

	return nil
}