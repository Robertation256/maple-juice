package main

import (
	"cs425-mp1/util"
	"cs425-mp1/test"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"bufio"
	"os"
)


func main() {
	var localPort string
	var ret string

	ips := util.LoadIps()

	// designate port for testing on a single machine
	// fmt.Println("Enter port:")
	// fmt.Scanln(&localPort)
	// localPort := strings.Split(ips[0], ":")[1]

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

	defer util.CloseClients(clients)

	grepService := util.NewGrepService("~/log")
	logService := new(test.LogService)
	logService.LogFileDir = "~/test_log"

	rpc.Register(grepService)
	rpc.Register(logService)
	rpc.HandleHTTP()

	// assume the first line in config is the local machine
	l, err := net.Listen("tcp", ips[0])
	fmt.Printf("HTTP-RPC server is listening on port %s\n", localPort)

	if err != nil {
		log.Fatal("Failed to start local server", err)
	}

	go http.Serve(l, nil)

	for {
		ret = ""
		fmt.Println("Enter a grep command:")
		in := bufio.NewReader(os.Stdin)
		input, _ := in.ReadString('\n')
		ret = util.GrepAllMachines(ips, clients, input)

		fmt.Println(ret)
	}
}
