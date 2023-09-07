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
	logFileNames []string
}

func NewGrepService(logFileDir string, localPort string) *GrepService {
	filePaths, err := os.ReadDir(logFileDir)
	if err != nil {
		log.Fatal("Error reading log file directory", err)
	}

	filesNames := make([]string, len(filePaths))

	for i := 0; i < len(filePaths); i++ {
		filesNames[i] = filePaths[i].Name()
	}

	this := new(GrepService)
	this.logFileDir = logFileDir
	hardCodedName := fmt.Sprintf("%s.txt", localPort)
	this.logFileNames = []string{hardCodedName}
	return this
}

func (this *GrepService) GrepLocal(args *Args, reply *string) error {
	grepOptions := parseUserInput(args.Input)
	
	*reply = ""

	for _, fileName := range this.logFileNames {
		// todo: remove cmd /K for linux
		cmdArgs := append(grepOptions, this.logFileDir+"/"+fileName)
		cmd := exec.Command("grep", cmdArgs...)
		output, err := cmd.CombinedOutput()
		// exit code 1 means a match was not found
		if err != nil && cmd.ProcessState.ExitCode() != 1 {
			log.Println("Error while executing grep", err)
			return err
		}
		*reply += fmt.Sprintf("%s:%s", fileName, string(output))
	}
	return nil
}

type Args struct {
	Input string
}

func LoadIps() []string {
	var s []byte
	var err error
	s, err = os.ReadFile("../config.txt")
	if err != nil {
		s, err = os.ReadFile("./config.txt")
		if err != nil {
			log.Fatal("Error reading remote server config file", err)
		}
	}

	ips := strings.Split(string(s), ",")

	if len(ips) == 0 {
		log.Fatal("Remote server ip config is empty")
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

func GrepAllMachines(ips []string, clients []*rpc.Client, input string) string {
	grepResults := make([]string, len(ips))

	for idx := range grepResults {
		grepResults[idx] = ""
	}

	calls := make([]*rpc.Call, len(ips))
	args := Args{Input: input}
	for index, ip := range ips {
		// try start first time connection / reconnect for broken ones
		if clients[index] == nil {
			c, err := rpc.DialHTTP("tcp", ip)
			if err == nil {
				clients[index] = c
			}
		}

		if clients[index] != nil {
			call := clients[index].Go("GrepService.GrepLocal", args, &(grepResults[index]), nil)
			calls[index] = call
		}
	}

	// iterate and look for completed rpc calls
	for { // todo: add timeout in case some rpc takes too long to return
		complete := true
		for i, call := range calls {
			if call != nil {
				select {
				case _, ok := <-call.Done:
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

	ret := ""
	for _, v := range grepResults {
		ret += v
	}
	return ret
}
