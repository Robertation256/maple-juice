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
	"time"
)


func main() {
	localPort := "8000"
	var ret string

	ips := util.LoadIps()

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

	defer util.CloseClients(clients)

	grepService := util.NewGrepService("../log")
	logService := new(test.LogService)
	logService.LogFileDir = "../test_log"

	rpc.Register(grepService)
	rpc.Register(logService)
	rpc.HandleHTTP()

	// assume the first line in config is the local machine
	hostname, hostNameErr := os.Hostname()
	if hostNameErr != nil {
		log.Fatal("Failed to get hostname", hostNameErr)
	}
	fmt.Println(hostname)
	l, err := net.Listen("tcp", hostname+":"+localPort)
	fmt.Printf("HTTP-RPC server is listening on port %s\n", localPort)

	if err != nil {
		log.Fatal("Failed to start local server", err)
	}

	go http.Serve(l, nil)

	for {
		ret = ""
		fmt.Println("\n\n----------------------\n")
		fmt.Println("Enter a grep command:")
		in := bufio.NewReader(os.Stdin)
		input, _ := in.ReadString('\n')
		start := time.Now()
		ret = util.GrepAllMachines(ips, clients, input)
		elasped := time.Now().Sub(start)
		fmt.Println(ret)
		fmt.Printf("Elapsed time: %s", elasped.Round(time.Millisecond))
	}
}
