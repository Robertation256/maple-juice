package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"log"
	"net/rpc"
	"strconv"
)

//maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_filename> <input_has_header>
func ProcessMapleCmd(args []string) error {
	if (len(args) != 5){
		log.Print("Invalid maple command")
		return errors.New("Invalid maple command")
	}

	taskNum, err := strconv.Atoi(args[1]);
	if (err != nil){
		log.Print("Invalid maple task number")
		return errors.New("Invalid maple task number")
	}

	handleInputHeader, err := strconv.Atoi(args[4]);
	if (err != nil || (handleInputHeader != 0 && handleInputHeader != 1)){
		log.Print("Invalid input_has_header flag")
		return errors.New("Invalid input_has_header flag")
	}

	mapleExeName := args[0]
	sdfsIntermediateFileName := args[2];
	sdfsSrcFileName := args[3]
	if(len(mapleExeName)==0 || len(sdfsIntermediateFileName)==0 || len(sdfsSrcFileName)==0){
		log.Print("file names cannot be empty")
		return errors.New("file names cannot be empty")
	}

	jobRequest := &util.JobRequest{
		IsMaple: true,
		MapleJob: util.MapleJobRequest{
			ExcecutableFileName: mapleExeName,
			TaskNum: taskNum,
			SrcSdfsFileName: sdfsSrcFileName,
			OutputFilePrefix: sdfsIntermediateFileName,
			PreserveInputHeader: handleInputHeader==1,
		},
	}

	// dial job manager and submit job via rpc

	client := dialMRJobManager()
	if client == nil {
		return errors.New("Cannot connect to Maple Juice Job Manager")
	}

	defer client.Close()
	reply := ""
	responseErr := client.Call("MRJobManager.SubmitJob", jobRequest, &reply)

	if responseErr != nil {
		log.Print("Encountered error while executing Maple job", responseErr)
	} else {
		log.Print("Finished executing Maple job")
	}

	return responseErr
}

// juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> 
// delete_input={0,1} is_hash={0,1}}
func ProcessJuiceCmd(args []string) error {
	if (len(args) != 6){
		log.Print("Invalid juice command")
		return errors.New("Invalid juice command")
	}

	taskNum, err := strconv.Atoi(args[1]);
	if (err != nil){
		log.Print("Invalid juice task number")
		return errors.New("Invalid juice task number")
	}

	deleteInput, err := strconv.Atoi(args[4]);
	if (err != nil || (deleteInput != 0 && deleteInput != 1)){
		log.Print("Invalid delete_input flag")
		return errors.New("Invalid delete_input flag")
	}

	isHash, err := strconv.Atoi(args[5]);
	if (err != nil || (isHash != 0 && isHash != 1)){
		log.Print("Invalid is_hash flag")
		return errors.New("Invalid is_hash flag")
	}

	juiceExeName := args[0]
	sdfsIntermediatePrefix := args[2];
	sdfsDstFileName := args[3]
	if(len(juiceExeName)==0 || len(sdfsIntermediatePrefix)==0 || len(sdfsDstFileName)==0){
		log.Print("file names cannot be empty")
		return errors.New("file names cannot be empty")
	}

	jobRequest := &util.JobRequest{
		IsMaple: false,
		JuiceJob: util.JuiceJobRequest{
			ExcecutableFileName: juiceExeName,
			TaskNum: taskNum,
			SrcSdfsFilePrefix: sdfsIntermediatePrefix,
			OutputFileName: sdfsDstFileName,
			DeleteInput: deleteInput==1,
			IsHashPartition: isHash==1,
		},
	}

	// dial job manager and submit job via rpc

	client := dialMRJobManager()
	if client == nil {
		return errors.New("Cannot connect to Maple Juice Job Manager")
	}

	defer client.Close()
	reply := ""
	responseErr := client.Call("MRJobManager.SubmitJob", jobRequest, &reply)

	if responseErr != nil {
		log.Print("Encountered error while executing Juice job", responseErr)
	} else {
		log.Print("Finished executing Juice job")
	}
	return responseErr
}


func dialMRJobManager() *rpc.Client {
	leaderId := LeaderId

	if len(leaderId) == 0{
		log.Println("Leader election in progress, Maple Juice service not available")
		return nil
	}

	leaderIp := util.NodeIdToIP(leaderId)
	client := dial(leaderIp, config.RpcServerPort)
	if client == nil {
		log.Printf("Failed to establish connection with Maple Juice Job Manager at %s:%d", leaderId, config.RpcServerPort)
	}
	return client
}