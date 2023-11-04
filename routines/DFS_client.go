package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"time"
)


const (
	FILE_PUT int = 1
	FILE_GET int = 2
	FILE_DELETE int = 3
	FILE_LIST int = 4 


	FILE_METADATA_SERVICE_QUERY_TIMEOUT_SECONDS int = 10
)


type DfsRequest struct {
	RequestType int
	FileName string
}



// return type for DFS client metadata query
type DfsResponse struct {
	FileName string
	Master   util.FileInfo
	Servants []util.FileInfo
}



// parse and dispatch cmd line
func ProcessDfsCmd(cmd string, args []string){
	switch (cmd) {
	case "put":
		PutFile(args)
	case "get":
		GetFile(args)
	case "delete":
		DeleteFile(args)
	case "ls":
		ListFile(args)
	case "store":
		// todo: handle store
	
	default:
		log.Printf("Unsupported DFS command: (%s)", cmd)	
	}


}


func GetFile(args []string){
	if len(args) != 2 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}

	localFileName := args[0]
	remoteFileName := args[1]

	if len(localFileName) == 0 || len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}


	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_GET, remoteFileName, fileMetadata)

	if err != nil {
		log.Printf("Encountered error while quering file metadata service: %d", err.Error())
		return 
	}

	master := fileMetadata.Master
	if master.FileStatus != util.COMPLETE {
		log.Printf("File master is not ready: file upload in progress")
		return
	}

	// fileMasterIP := NodeIdToIP(master.NodeId)
	// port := config.RpcServerPort

	
	// todo: plugin into file server rpc call

}


func PutFile(args []string){
	if len(args) != 2 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}

	localFileName := args[0]
	remoteFileName := args[1]

	if len(localFileName) == 0 || len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_PUT, remoteFileName, fileMetadata)

	if err != nil {
		log.Printf("Encountered error while quering file metadata service: %d", err.Error())
		return 
	}

	// master := fileMetadata.Master

	// fileMasterIP := NodeIdToIP(master.NodeId)
	// port := config.RpcServerPort


	
	// todo: plugin into file server rpc call


	// todo: plugin into file server rpc call
}


func DeleteFile(args []string){
	if len(args) != 1 {
		log.Printf("Invalid parameteres for DFS PUT command")
		return
	}

	remoteFileName := args[0]

	if len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS DELETE command")
		return
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_DELETE, remoteFileName, fileMetadata)

	if err != nil {
		log.Printf("Encountered error while query file metadata service: %d", err.Error())
		return 
	}


	// master := fileMetadata.Master
	// fileMasterIP := NodeIdToIP(master.NodeId)
	// port := config.RpcServerPort


	// todo: plugin into file server rpc call
}



func ListFile(args []string){
	if len(args) != 1 {
		log.Printf("Invalid parameteres for DFS PUT command")
		return
	}

	remoteFileName := args[0]

	if len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS LIST command")
		return
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_LIST, remoteFileName, fileMetadata)
	if err != nil {
		log.Printf("Encountered error while query file metadata service: %d", err.Error())
	} else {
		fmt.Print(fileMetadata.toString())
	}
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




func (this *DfsResponse)toString()string{
	ret := "---------------------\nFile name: " + this.FileName + "\n"

	ret += this.Master.ToString()
	
	for _, servant := range this.Servants{
		ret += servant.ToString()
	}

	return ret
}










