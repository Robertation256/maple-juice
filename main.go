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

	ips := grep.LoadIps()

	// designate port for testing on a single machine
	fmt.Println("Enter port:")
	fmt.Scanln(&localPort)
	// localPort := strings.Split(ips[0], ":")[1]

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

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
		ret = ""
		fmt.Println("Enter a pattern:")
		fmt.Scanln(&input)
		ret = grep.GrepAllMachines(ips, clients, input)

		fmt.Println(ret)
	}
}
