package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
)

type GrepService struct {
	logFileDir   string
	logFileNames []string
}

func NewGrepService(logFileDir string) *GrepService {
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
	this.logFileNames = filesNames
	return this
}

func (this *GrepService) GrepLocal(args *Args, reply *string) error {
	pattern := args.Input
	*reply = ""

	for _, fileName := range this.logFileNames {
		// todo: remove cmd /K for linux
		out, err := exec.Command("cmd", "/K", "grep", "-c", pattern, this.logFileDir+"/"+fileName).Output()
		if err != nil {
			log.Println("Encountered error while executing grep", err)
			return err
		}
		*reply += fmt.Sprintf("%s\t\t%s\n", fileName, string(out))
	}
	return nil
}

type Args struct {
	Input string
}

func LoadIps() []string {
	s, err := os.ReadFile("./config.txt")
	if err != nil {
		log.Fatal("Error reading remote server config file", err)
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

func main() {
	var localPort string
	var input string
	var ret string

	header := "file name\tcount\n"
	ips := LoadIps()

	// designate port for testing on a single machine
	fmt.Println("Enter port:")
	fmt.Scanln(&localPort)
	// localPort := strings.Split(ips[0], ":")[1]

	clients := make([]*rpc.Client, len(ips)-1) // stores clients with established connections
	grepResults := make([]string, len(ips)-1)

	for idx := range grepResults {
		grepResults[idx] = ""
	}

	defer CloseClients(clients)

	grepService := NewGrepService("./logs")
	rpc.Register(grepService)
	rpc.HandleHTTP()

	// assume the first line in config is the local machine
	l, err := net.Listen("tcp", ":"+localPort)
	fmt.Printf("HTTP-RPC server is listening on port %s\n", localPort)

	if err != nil {
		log.Fatal("Failed to start local server", err)
	}

	go http.Serve(l, nil)

	for {
		ret = header
		calls := make([]*rpc.Call, len(ips)-1)
		fmt.Println("Enter a pattern:")
		fmt.Scanln(&input)
		args := Args{Input: input}

		for index, ip := range ips[1:] {
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

		// compute local result
		localResult := ""
		grepService.GrepLocal(&args, &localResult)
		ret += localResult

		// iterate and look for completed rpc calls
		for { // todo: add timeout in case some rpc takes too long to return
			complete := true
			for i, call := range calls {
				if call != nil {
					select {
					case _, ok := <-call.Done:
						if ok {
							// remove, since rpc is done
							calls[i] = nil
						}
					default:
						complete = false
					}
				}
			}
			if complete {
				break
			}
		}

		for idx, v := range grepResults {
			ret += v
			grepResults[idx] = ""
		}

		fmt.Println(ret)
	}
}
