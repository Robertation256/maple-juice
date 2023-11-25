package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"time"
)


// responsible for locally executing Maple / Juice task as instructed by the MR Job Manager

type MRNodeManager struct {}


func (this *MRNodeManager) Register() {
	rpc.Register(this)
}

//execute a Maple task locally
func (this *MRNodeManager) StartMapleTask(args *util.MapleTaskArg, reply *string) error {
	// fetch executable from SDFS
	executableFileName := args.ExcecutableFileName
	inputFileName := args.InputFileName
	transmissionId := args.TransmissionId

	defer cleanUp(inputFileName)

	err := SDFSGetFile(executableFileName, executableFileName, RECEIVER_MR_NODE_MANAGER)
	if err != nil {
		log.Print("Encountered error fetching executatble from SDFS", err)
		return err
	}

	// wait for input file's arrival
	log.Printf("Waiting for input file %s with transmission id %s", inputFileName, transmissionId)
	for {
		if FileTransmissionProgressTracker.IsLocalCompleted(transmissionId) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	log.Print("Start running maple executatble...")


	// execute executable on input file
	executableFilePath := config.NodeManagerFileDir + executableFileName
	inputFilePath := config.NodeManagerFileDir + inputFileName

	// go run executable.go -in <input_file_path>
	cmdArgs := []string {"run", executableFilePath, "-in", inputFilePath, "-prefix", args.OutputFilePrefix}

	cmd := exec.Command("go", cmdArgs...)
	output, err := cmd.CombinedOutput()
	
	if err != nil || cmd.ProcessState.ExitCode() != 1 {
		log.Println("Error while executing Maple executable", err)
		return errors.New("Error executing Maple executable")
	} 

	// executable output should be a comma separated list of output files
	splitted := strings.Split(string(output), ",")
	outputFileNames := make([]string, len(splitted))
	for idx, outputFileName := range splitted {
		outputFileName = strings.Trim(outputFileName, " \n\r")
		outputFileNames[idx] = outputFileName
		_, err1 := os.Stat(config.NodeManagerFileDir + outputFileName)
		if err1 != nil {
			return errors.New("Executable failed to produce valid output")
		}
	}

	uploadTimeout := time.After(300 * time.Second)
	remainingFiles := len(outputFileNames)
	responseChan := make(chan error, remainingFiles)

	for _, fileName := range outputFileNames {
		go func(file string){
			_, err := SDFSPutFile(file, config.NodeManagerFileDir+file)
			responseChan <- err
		}(fileName)
	}

	for remainingFiles > 0 {
		select {
		case <- uploadTimeout:
			log.Print("Timeout uploading Maple output to SDFS")
			return errors.New("Timeout uploading Maple output to SDFS")
		case err := <- responseChan:
			if err != nil {
				// todo: clean up files uploaded to SDFS
				log.Print("Encounterd error uploading Maple output to SDFS")
				return err 
			} else {
				remainingFiles -= 1
			}
		}
	}

	*reply = "ACK"
	return nil
}


func cleanUp(filePrefix string){
	if len(filePrefix) == 0 {
		return
	}

	// clean up local input and output files
	exec.Command("rm", "-f", config.NodeManagerFileDir + filePrefix + "*")

	// clean up executable if any
	exec.Command("rm", "-f", config.NodeManagerFileDir + "*.go")
}



func (this *MRNodeManager) StartJuiceTask(args *util.JuiceTaskArg, reply *string) error {
	// fetch executable and input key partitions from SDFS
	executableFileName := args.ExcecutableFileName
	parition := args.KeyToFileNames

	executableFetchResChan := make(chan error, 1)


	go func(){
		executableFetchResChan <- SDFSGetFile(executableFileName, executableFileName, RECEIVER_MR_NODE_MANAGER)
	}()

	for key, files := range parition {
		localFileName := fmtJuiceInputFileName(args.InputFilePrefix, key)
		os.Remove(config.NodeManagerFileDir + localFileName)
		err := SDFSFetchAndConcat(files, localFileName, RECEIVER_MR_NODE_MANAGER)
		if err != nil {
			return err
		}
	}

	// wait for all file's arrival
	timeout := time.After(1 * time.Minute)
	select{
	case <-timeout:
		return errors.New("Timeout fetching executable from SDFS")
	case err := <- executableFetchResChan:
		if err != nil {
			return err
		}
	}

	executionErrorChan := make(chan error, len(parition))
	executableFilePath := config.NodeManagerFileDir + args.ExcecutableFileName
	// execute excutable on all key partitions and send result file to SDFS
	for key := range parition {
		go func(k string){
			localFilePath := config.NodeManagerFileDir + fmtJuiceInputFileName(args.InputFilePrefix, k)
			cmdArgs := []string {"run", executableFilePath, "-in", localFilePath, "-dest", args.OutputFilePrefix + "-" + k}
			cmd := exec.Command("go", cmdArgs...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				executionErrorChan <- err
				return
			}
			os.Remove(localFilePath)
			outputFileName := string(output)
			expectedOutputFileName := args.OutputFilePrefix + "-" + k 
			if outputFileName != expectedOutputFileName {
				log.Printf("WARN: Juice executable not producing file with expected name")
			}

			_, err1 := SDFSPutFile(expectedOutputFileName, config.NodeManagerFileDir + outputFileName)
			executionErrorChan <- err1
		}(key)
	}


	// track execution progress
	remainingKey := len(parition)
	executionTimeout := time.After(10 * time.Minute)

	for remainingKey > 0 {
		select{
		case <- executionTimeout:
			return errors.New("Juice task execution timeout")
		case err := <- executionErrorChan:
			if err != nil {
				return err 
			} else {
				remainingKey -= 1
			}
		}
	}

	*reply = "ACK"
	return nil
}


func fmtJuiceInputFileName(filePrefix string, key string) string {
	return fmt.Sprintf("juice_input-%s-%s", filePrefix, key)
}

