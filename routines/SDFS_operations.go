package routines


import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const (
	FILE_PUT int = 1
	FILE_GET int = 2
	FILE_DELETE int = 3
	FILE_LIST int = 4 


	FILE_METADATA_SERVICE_QUERY_TIMEOUT_SECONDS int = 10
)

// fetch file from SDFS, blocks until an error/completion/timeout is reached
func SDFSGetFile(remoteFileName string, localFileName string, receiverTag uint8) error {

	if len(localFileName) == 0 || len(remoteFileName) == 0 {
		return errors.New("Invalid parameteres for DFS GET command")
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_GET, remoteFileName, fileMetadata)
	if err != nil {
		return err
	}

	master := fileMetadata.Master
	if master.FileStatus != util.COMPLETE {
		return errors.New("File master is not ready: file upload in progress")
	}

	fileMasterIP := NodeIdToIP(master.NodeId)
	port := config.RpcServerPort

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if (err != nil) {
		return err
	}

	transmissionId := transmissionIdGenerator.NewTransmissionId(remoteFileName)

	getArgs := &RWArgs{
		TransmissionId: transmissionId,
		LocalFilename: localFileName,
		SdfsFilename: remoteFileName,
		ClientAddr: NodeIdToIP(SelfNodeId),
		ReceiverTag: receiverTag,
	}

	var reply string
	responseErr := client.Call("FileService.ReadFile", getArgs, &reply)

	if responseErr != nil {
		return responseErr
	}

	timeout := time.After(180 * time.Second)

	for {
		select {
		case <-timeout:
			return errors.New("SDFS GET timeout")
		default:
			if FileTransmissionProgressTracker.IsLocalCompleted(transmissionId) {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
}


func SDFSPutFile(remoteFileName string, localFilePath string) (*DfsResponse, error) {
	if len(localFilePath) == 0 || len(remoteFileName) == 0 {
		return nil, errors.New("Invalid parameteres for DFS PUT command")
	}

	_, err0 := os.Stat(localFilePath)
	if err0 != nil {
		return nil, err0
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_PUT, remoteFileName, fileMetadata)
	if err != nil {
		return nil, err
	}

	master := fileMetadata.Master

	fileMasterIP := NodeIdToIP(master.NodeId)
	port := config.RpcServerPort

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if (err != nil) {
		return nil, err
	}

	putArgs := &RWArgs{
		SdfsFilename: remoteFileName,
		ClientAddr: NodeIdToIP(SelfNodeId),
	}

	transmissionId := "" 
	responseErr := client.Call("FileService.WriteFile", putArgs, &transmissionId)


	if responseErr != nil {
		return nil, responseErr
	} 

	// We are given a token by the scheduler, proceed with uploading files
	err1 := SendFile(localFilePath, remoteFileName, fileMasterIP+":"+strconv.Itoa(config.FileReceivePort), transmissionId, RECEIVER_SDFS_FILE_SERVER)

	if err1 != nil {
		return nil, err1
	}

	reply := ""
	err2 := client.Call("FileService.CheckWriteCompleted", &transmissionId, &reply)

	if err2 != nil  || reply != "ACK" {
		return nil, errors.New("Encountered timeout while checking write completion")
	}

	return fileMetadata, nil 
}


func SDFSDeleteFile(remoteFileName string) error {

	if len(remoteFileName) == 0 {
		return errors.New("Invalid parameteres for DFS DELETE command")
	}


	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_DELETE, remoteFileName, fileMetadata)
	if err != nil {
		return err
	}


	master := fileMetadata.Master
	fileMasterIP := NodeIdToIP(master.NodeId)
	port := config.RpcServerPort


	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if (err != nil) {
		return err
	}
	deletArgs := &DeleteArgs{
		Filename: remoteFileName,
	}
	var reply string
	responseErr := client.Call("FileService.DeleteFile", deletArgs, &reply)

	return responseErr
}



func SDFSListFile(remoteFileName string) (*DfsResponse, error) {

	if len(remoteFileName) == 0 {
		return nil, errors.New("Invalid parameteres for DFS LIST command")
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_LIST, remoteFileName, fileMetadata)
	if err != nil {
		return nil, err
	} 

	return fileMetadata, nil
}




func queryMetadataService(requestType int, fileName string, reply *DfsResponse) error {
	client := dialMetadataService()
	if client == nil {
		return errors.New("Failed to query file metadata service")
	}

	request := &DfsRequest{
		FileName: fileName,
		RequestType: requestType,
	}


	call := client.Go("FileMetadataService.HandleDfsClientRequest", request, reply, nil)
	requestTimeout := time.After(time.Duration(FILE_METADATA_SERVICE_QUERY_TIMEOUT_SECONDS) * time.Second)

	select {
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok || reply == nil{
			log.Println("RPC call corrupted")
			return errors.New("RPC call corrupted")
		}
	case <- requestTimeout:
		return errors.New("Request timeout")
	}
	return nil
}


func dialMetadataService() *rpc.Client {
	leaderId := LeaderId

	if len(leaderId) == 0{
		log.Println("Leader election in progress, DFS service not available")
		return nil
	}

	leaderIp := NodeIdToIP(leaderId)
	client := dial(leaderIp, config.RpcServerPort)
	if client == nil {
		log.Printf("Failed to establish connection with DFS metadata service at %s:%d", leaderId, config.RpcServerPort)
	}
	return client
}
