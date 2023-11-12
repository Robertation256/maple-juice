package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"errors"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"time"
)

var MRNodeManagerFileProgressTracker *util.TransmissionProgressManager = util.NewTransmissionProgressManager()

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
	for {
		if MRNodeManagerFileProgressTracker.IsLocalCompleted(transmissionId) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}


	// execute executable on input file
	executableFilePath := config.NodeManagerFileDir + executableFileName
	inputFilePath := config.NodeManagerFileDir + inputFileName

	// go run executable.go -in <input_file_path>
	cmdArgs := []string {"run", executableFilePath, "-in", inputFilePath}

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
		go func(){
			_, err := SDFSPutFile(fileName, config.NodeManagerFileDir+fileName)
			responseChan <- err
		}()
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



func (this *MRNodeManager) StartJuiceTask(args *util.MapleTaskArg, reply *string) error {
	return nil
}


