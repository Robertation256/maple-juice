package dfs

import (
	"errors"
	"fmt"
	"log"
	"maple-juice/config"
	"maple-juice/leaderelection"
	"maple-juice/membership"
	"maple-juice/util"
	"net/rpc"
	"os"
	"regexp"
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

// fetch one file from SDFS and overwrite local file if one exisits
func SDFSGetFile(remoteFileName string, localFileName string, receiverTag uint8) error {
	return sdfsFetch(remoteFileName, localFileName, receiverTag, WRITE_MODE_TRUNCATE)
}

// fetch multiple files from SDFS and concat into one local file
func SDFSFetchAndConcat(remoteFileNames []string, localFileName string, receiverTag uint8) error {
	for _, remoteFile := range remoteFileNames {
		err := sdfsFetch(remoteFile, localFileName, receiverTag, WRITE_MODE_APPEND)
		if err != nil {
			return err
		}
	}
	return nil 
}


// fetch all files matching a prefix from SDFS and concat into one local file
func SDFSFetchAndConcatWithPrefix(prefix string, localFileName string, receiverTag uint8) error {
	if len(prefix) == 0{
		return errors.New("Empty prefix")
	}

	regex := prefix + ".*"
	_, err := regexp.Compile(regex)
	if err != nil {
		return errors.New("Illegal regex character in prefix")
	}

	matchedFiles, err := SDFSSearchFileByRegex(regex)

	if err != nil {
		return err 
	}

	err = SDFSFetchAndConcat(*matchedFiles, localFileName, receiverTag)
	return err 
}

// fetch one file from SDFS, blocks until an error/completion/timeout is reached
func sdfsFetch(remoteFileName string, localFileName string, receiverTag uint8, writeMode uint8) error {
	if len(localFileName) == 0 || len(remoteFileName) == 0  || (writeMode != WRITE_MODE_APPEND && writeMode != WRITE_MODE_TRUNCATE) {
		return errors.New("Invalid parameteres for DFS GET command")
	}

	fileMetadata := &DfsResponse{}
	maxWaitRound := 5
	var master util.FileInfo
	
	for {
		err := queryMetadataService(FILE_GET, remoteFileName, fileMetadata)
		if err != nil {
			return err
		}
	
		master = fileMetadata.Master
		if master.FileStatus != util.COMPLETE {
			if maxWaitRound > 0 {
				maxWaitRound--
				time.Sleep(1 * time.Second)
			} else {
				return errors.New("Cannot fetch sdfs file: file upload is in progress, please wait and retry later")
			}
		} else {
			break
		}
	}


	fileMasterIP := util.NodeIdToIP(master.NodeId)
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
		ClientAddr: util.NodeIdToIP(membership.SelfNodeId),
		ReceiverTag: receiverTag,
		WriteMode: writeMode,
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

	fileMasterIP := util.NodeIdToIP(master.NodeId)
	port := config.RpcServerPort

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if (err != nil) {
		return nil, err
	}

	putArgs := &RWArgs{
		SdfsFilename: remoteFileName,
		ClientAddr: util.NodeIdToIP(membership.SelfNodeId),
	}

	transmissionId := "" 
	responseErr := client.Call("FileService.WriteFile", putArgs, &transmissionId)


	if responseErr != nil {
		return nil, responseErr
	} 

	// We are given a token by the scheduler, proceed with uploading files
	err1 := SendFile(localFilePath, remoteFileName, fileMasterIP+":"+strconv.Itoa(config.FileReceivePort), transmissionId, RECEIVER_SDFS_FILE_SERVER, WRITE_MODE_TRUNCATE)

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
	fileMasterIP := util.NodeIdToIP(master.NodeId)
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

// return a list of SDFS file names matching regex
func SDFSSearchFileByRegex(regex string) (*[]string, error) {
	if len(regex) == 0 {
		return nil, errors.New("Invalid parameteres for DFS SEARCH command")
	}

	reply := &[]string{}
	client := dialMetadataService()
	if client == nil {
		return nil, errors.New("Failed to query file metadata service")
	}

	defer client.Close()


	call := client.Go("FileMetadataService.HandleFileSearchRequest", &regex, reply, nil)
	requestTimeout := time.After(time.Duration(FILE_METADATA_SERVICE_QUERY_TIMEOUT_SECONDS) * time.Second)

	select {
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok || reply == nil{
			log.Println("RPC call corrupted")
			return nil, errors.New("RPC call corrupted")
		}
	case <- requestTimeout:
		return nil, errors.New("Request timeout")
	}
	return reply, nil
}




func queryMetadataService(requestType int, fileName string, reply *DfsResponse) error {
	client := dialMetadataService()
	if client == nil {
		return errors.New("Failed to query file metadata service")
	}

	defer client.Close()

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
		if call.Error != nil{
			// call returned error
			return call.Error
		}
	case <- requestTimeout:
		return errors.New("Request timeout")
	}
	return nil
}


func dialMetadataService() *rpc.Client {
	leaderId := leaderelection.LeaderId

	if len(leaderId) == 0{
		log.Println("Leader election in progress, DFS service not available")
		return nil
	}

	leaderIp := util.NodeIdToIP(leaderId)
	client := util.Dial(leaderIp, config.RpcServerPort)
	if client == nil {
		log.Printf("Failed to establish connection with DFS metadata service at %s:%d", leaderId, config.RpcServerPort)
	}
	return client
}
