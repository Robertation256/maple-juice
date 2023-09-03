package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/xinshuoLei/cs425-mp1/grep"
	"github.com/xinshuoLei/cs425-mp1/test"
)

func main() {
	var localPort string
	var input string
	var ret string

	header := "file name\tcount\n"
	ips := grep.LoadIps()

	// designate port for testing on a single machine
	fmt.Println("Enter port:")
	fmt.Scanln(&localPort)
	// localPort := strings.Split(ips[0], ":")[1]

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections
	grepResults := make([]string, len(ips))

	for idx := range grepResults {
		grepResults[idx] = ""
	}

	defer grep.CloseClients(clients)

	grepService := grep.NewGrepService("./logs", localPort)
	logService := new(test.LogService)
	logService.LogFileDir = "./logs"

	rpc.Register(grepService)
	rpc.Register(logService)
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
		calls := make([]*rpc.Call, len(ips))
		fmt.Println("Enter a pattern:")
		fmt.Scanln(&input)
		args := grep.Args{Input: input}

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

		for idx, v := range grepResults {
			ret += v
			grepResults[idx] = ""
		}

		fmt.Println(ret)
	}
}
