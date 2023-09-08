package util

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
)

type GrepService struct {
	logFileDir   string
	LogFileName string
}


func NewGrepService(logFileDir string) *GrepService {
	filePaths, err := os.ReadDir(logFileDir)
	if err != nil {
		log.Fatal("Error reading log file directory: ", err)
	}

	filesNames := make([]string, len(filePaths))

	for i := 0; i < len(filePaths); i++ {
		filesNames[i] = filePaths[i].Name()
	}

	this := new(GrepService)
	this.logFileDir = logFileDir

	if len(filesNames) < 1 {
		log.Fatal("Log file does nof exist")
	}
	this.LogFileName = filesNames[0]
	return this
}

// execute grep command over local log file
func (this *GrepService) GrepLocal(args *Args, reply *string) error {
	grepOptions, _ := ParseUserInput(args.Input)
	
	*reply = ""
	fileName := this.LogFileName

	cmdArgs := append(grepOptions, this.logFileDir+"/"+fileName)
	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput()
	// exit code 1 means a match was not found
	if err != nil && cmd.ProcessState.ExitCode() != 1 {
		log.Println("Error while executing grep", err)
		return err
	}
	*reply += fmt.Sprintf("%s:%s", fileName, string(output))

	return nil
}

type Args struct {
	Input string
}

func LoadIps(homeDir string) []string {
	var s []byte
	var err error
	s, err = os.ReadFile(homeDir + "/config.txt")
	if err != nil {
		log.Fatal("Error reading remote server config file", err)
	}

	ips := strings.Split(string(s), ",")

	if len(ips) == 0 {
		log.Fatal("Remote server address config is empty")
	}

	return ips
}

func CloseClients(clients []*rpc.Client) {
	for _, c := range clients {
		if c != nil {
			c.Close()
		}
	}
}

// send grep command to all hosts in memeber list
func GrepAllMachines(ips []string, clients []*rpc.Client, input string) string {
	grepResults := make([]string, len(ips))

	for idx := range grepResults {
		grepResults[idx] = ""
	}

	calls := make([]*rpc.Call, len(ips))
	args := Args{Input: input}
	for index, ip := range ips {
		// start connection if it is not previously established
		if clients[index] == nil {
			c, err := rpc.DialHTTP("tcp", ip+":8000")
			if err == nil {
				clients[index] = c
			}
		}

		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("GrepService.GrepLocal", args, &(grepResults[index]), nil)
			calls[index] = call
		}
	}

	// iterate and look for completed rpc calls
	for {
		complete := true
		for i, call := range calls {
			if call != nil {
				select {
					case _, ok := <-call.Done:	// check if channel has output ready
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

	// aggregate server results
	var totalLineCount int64 = 0
	ret := ""
	for _, v := range grepResults {
		ret += v
		count, countErr := ExtractLineCount(v)
		if countErr != nil {
			log.Fatal("Error extracting number", countErr)
		}
		totalLineCount += int64(count)
	}
	ret += fmt.Sprintf("Total:%d", totalLineCount)

	return ret
}
