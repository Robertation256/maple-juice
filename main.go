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

	homeDir, homeDirErr := os.UserHomeDir()
	if homeDirErr != nil {
		log.Fatal("Error getting user's home directory:", homeDirErr)
	}

	var isTestMode string
	fmt.Println("test mode? [Y/n]")
	fmt.Scanln(&isTestMode)
	logFolder := homeDir + "/log"
	if isTestMode == "Y" {
		logFolder = homeDir + "/test_log"
		fmt.Println("Running in test mode & using ~/test_log as log folder")
	}

	var ret string

	ips := util.LoadIps(homeDir)

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

	defer util.CloseClients(clients)

	grepService := util.NewGrepService(logFolder)
	logService := new(test.LogService)
	logService.LogFileDir = homeDir + "/test_log"
	logService.LogFilename = grepService.LogFileName

	rpc.Register(grepService)
	rpc.Register(logService)
	rpc.HandleHTTP()

	hostname, hostNameErr := os.Hostname()
	if hostNameErr != nil {
		log.Fatal("Failed to get hostname", hostNameErr)
	}

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
		
		_, parseErr := util.ParseUserInput(input)
		// if an error occured while parsing user input, prompt user to try again
		if parseErr != nil {
			fmt.Printf("Invalid input: %s. Please try again", parseErr)
			continue
		}
		
		ret = util.GrepAllMachines(ips, clients, input)
		elasped := time.Now().Sub(start)
		fmt.Println(ret)
		fmt.Printf("Elapsed time: %s", elasped.Round(time.Millisecond))
	}
}
