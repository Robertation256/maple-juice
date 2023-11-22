package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"
)


var isInitialized bool = false

var transmissionIdGenerator *util.TransmissionIdGenerator



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


func InitializeClient(){
	transmissionIdGenerator = util.NewTransmissionIdGenerator("SDFS-client-"+SelfNodeId)
	isInitialized = true
}


// parse and dispatch cmd line
func ProcessDfsCmd(cmd string, args []string){
	if !isInitialized {
		log.Print("Client is not initialized")
		return
	}
	switch (cmd) {
	case "put":
		putFile(args)
	case "get":
		getFile(args)
	case "delete":
		deleteFile(args)
	case "ls":
		listFile(args)
	case "multiread":
		multiread(args)

	case "store":
		// todo: handle store
		outputStore(args)
	default:
		log.Printf("Unsupported DFS command: (%s)", cmd)	
	}
}

// fetch file from SDFS, blocks until an error/completion/timeout is reached
func getFile(args []string) error {
	if len(args) != 2 {
		log.Printf("Invalid parameteres for DFS GET command")
		return errors.New("Invalid parameteres for DFS GET command")
	}

	remoteFileName := args[0]
	localFileName := args[1]

	if util.FileInSdfsFolder(remoteFileName) {
		// if file is on local machine's sdfs folder, simply execute a cp command
		err := util.CopyFileFromSdfsToLocal(remoteFileName, localFileName)
		if err == nil {
			log.Print("Done\n\n")
		}
		return err
	}

	return SDFSGetFile(remoteFileName, localFileName, RECEIVER_SDFS_CLIENT)
}


func putFile(args []string){
	if len(args) != 2 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}

	localFileName := args[0]
	remoteFileName := args[1]

	response, err := SDFSPutFile(remoteFileName, config.LocalFileDir+localFileName)

	if err != nil {
		log.Print("Encountered error executing SDFS PUT", err)
		return
	}


	fmt.Printf("\n\nDONE\n")
	fmt.Printf("\nSDFS PUT operation completed with data stored at\n")
	fmt.Printf("File Master: %s\n", response.Master.NodeId)

	for _, servant := range response.Servants{
		fmt.Printf("File Servant: %s\n", servant.NodeId)
	}
	fmt.Print("\n\n\n")
}


func deleteFile(args []string){
	if len(args) != 1 {
		log.Printf("Invalid parameteres for DFS PUT command")
		return
	}

	remoteFileName := args[0]

	responseErr := SDFSDeleteFile(remoteFileName)

	if responseErr != nil {
		fmt.Printf("File Master responsed with error: %s", responseErr.Error())
	} else {
		log.Print("Done\n\n")
	}
}



func listFile(args []string){
	if len(args) != 1 {
		log.Printf("Invalid parameteres for DFS PUT command")
		return
	}

	remoteFileName := args[0]

	response, err := SDFSListFile(remoteFileName)

	if err != nil {
		log.Printf("Encountered error while query file metadata service: %s", err.Error())
	} else {
		fmt.Print(response.toString())
	}
}


func multiread(args []string){
	if len(args) < 2 {
		log.Printf("Invalid parameteres for DFS multiread command")
	}


	remoteFileName := args[0]
	machineIds := make([]int, 0)

	for i:=1; i<len(args); i++ {
		id, err := strconv.Atoi(args[i])
		if err != nil  || id < 1 || id > len(config.ServerHostnames) {
			log.Printf("Invalid machine Id")
		}
		machineIds = append(machineIds, id-1)	// switch from 1-index to 0-index
	}


	for _, machineId := range machineIds {
		hostName := config.ServerHostnames[machineId]
		fmt.Printf("Instructing read for host %s\n", hostName)
		fmt.Printf("Hostname is %s", hostName)
		go func() {
			client := dial(hostName, config.RpcServerPort)

			if client == nil {
				log.Printf("Unable to connect to %s:%d", hostName, config.RpcServerPort)
				return
			}

			reply := ""
			call := client.Go("DfsRemoteReader.Read", &remoteFileName, &reply, nil)
			timeout := time.After(300 * time.Second)

			select {
			case <-timeout:
				log.Printf("DFS file GET times out at %s", hostName)
				return
			case _, ok := <-call.Done: 
				if !ok {
					log.Println("Channel closed for async rpc call")
				} else {
					if call.Error == nil {
						log.Printf("DFS GET completed at %s", hostName)
					} else {
						log.Printf("DFS GET failed at %s. Error: %s", hostName, call.Error.Error())
					}
				}
			}	
		}()
	}
}

func outputStore(args []string) {
	sdfsFolder := config.SdfsFileDir
	files, err := ioutil.ReadDir(sdfsFolder)
	if err != nil {
		fmt.Println("Error reading sdfs folder: ", err)
		return
	}
	for _, file := range files {
		fmt.Println(file.Name())
	}
}	






func (this *DfsResponse)toString()string{
	ret := "---------------------\nFile name: " + this.FileName + "\n"

	ret += this.Master.ToString()
	
	for _, servant := range this.Servants{
		ret += servant.ToString()
	}

	return ret
}










