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
	var localPort string
	var ret string

	ips := util.LoadIps()

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

	defer util.CloseClients(clients)

	grepService := util.NewGrepService("../log")

	testService := new(test.LogService)	// service used for test
	testService.LogFileDir = "../test_log"

	rpc.Register(grepService)
	rpc.Register(testService)
	rpc.HandleHTTP()

	// first entry is the address of the local machine
	l, err := net.Listen("tcp", ips[0])
	fmt.Printf("HTTP-RPC server started at%s\n", ips[0])

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
