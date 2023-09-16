package main

import (
	// "cs425-mp2/test"
	"cs425-mp2/routines"
	"cs425-mp2/util"
	"fmt"

	// "fmt"
	// "log"
	// "net"
	// "net/http"
	// "net/rpc"
	// "os"
	"strconv"
	// "time"
)



func main(){
	var isBootstrapServer string 
	var boostrapServicePort string
	var boostrapProtocol string
	var protocol uint8
	var memberListServerPort string
	var localMembershipList *util.MemberList
	var userCmd string 

	var boostrapServerAddr string  

	util.Prompt("Start as boostrap server? [Y/n]",
		&isBootstrapServer,
		func(in string) bool {return in == "Y" || in == "n"},
	)

	if isBootstrapServer == "Y" {
		util.Prompt("Please enter boostrap service port",
			&boostrapServicePort,
			util.IsValidPort)
		util.Prompt("Please enter protocol [G/GS]",
			&boostrapProtocol,
			func(in string) bool {return in == "G" || in == "GS"})
		if boostrapProtocol == "G" {
			protocol = util.G 
		} else {
			protocol = util.GS 
		}
	} else {
		util.Prompt("Please enter boostrap service address (ip:port)",
			&boostrapServerAddr,
			util.IsValidAddress)
	}

	util.Prompt("Please enter membership list server port",
		&memberListServerPort,
		util.IsValidPort)

	p, _ := strconv.Atoi(memberListServerPort)
	port := uint16(p)
	localMembershipList = util.NewMemberList(port)

	if isBootstrapServer == "Y" {
		go routines.StartIntroducer(boostrapServicePort, protocol, localMembershipList)
		go routines.StartMembershipListServer(port, "", localMembershipList)
	} else {
		go routines.StartMembershipListServer(port, boostrapServerAddr, localMembershipList)
	}

	for {
		util.Prompt("Type print to print current membership list", &userCmd,
		func(in string) bool {return in == "print"})
		fmt.Println(localMembershipList.ToString())
	}
}


// func main() {
// 	localPort := "8000"

// 	homeDir, homeDirErr := os.UserHomeDir()
// 	if homeDirErr != nil {
// 		log.Fatal("Error getting user's home directory:", homeDirErr)
// 	}

// 	var isTestMode string
// 	fmt.Println("test mode? [Y/n]")
// 	fmt.Scanln(&isTestMode)
// 	logFolder := homeDir + "/log"
// 	if isTestMode == "Y" {
// 		logFolder = homeDir + "/test_log"
// 		fmt.Println("Running in test mode & using ~/test_log as log folder")
// 	}

// 	var ret string

// 	ips := util.LoadIps(homeDir)

// 	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

// 	defer util.CloseClients(clients)

// 	grepService := util.NewGrepService(logFolder)

// 	testService := new(test.LogService)	// service used for test
// 	testService.LogFileDir = homeDir + "/test_log"
// 	testService.LogFilename = grepService.LogFileName

// 	rpc.Register(grepService)
// 	rpc.Register(testService)
// 	rpc.HandleHTTP()

// 	hostname, hostNameErr := os.Hostname()
// 	if hostNameErr != nil {
// 		log.Fatal("Failed to get hostname", hostNameErr)
// 	}

// 	l, err := net.Listen("tcp", hostname+":"+localPort)
// 	fmt.Printf("HTTP-RPC server is listening on port %s\n", localPort)

// 	if err != nil {
// 		log.Fatal("Failed to start local server", err)
// 	}

// 	go http.Serve(l, nil)

// 	for {
// 		ret = ""
// 		fmt.Println("\n\n----------------------\n")
		
// 		fmt.Println("Enter a grep command:")

// 		in := bufio.NewReader(os.Stdin)
// 		input, _ := in.ReadString('\n')

// 		start := time.Now()
		
// 		_, parseErr := util.ParseUserInput(input)
// 		// if an error occured while parsing user input, prompt user to try again
// 		if parseErr != nil {
// 			fmt.Printf("Invalid input: %s. Please try again", parseErr)
// 			continue
// 		}
		
// 		ret = util.GrepAllMachines(ips, clients, input)

// 		elasped := time.Now().Sub(start)
		
// 		fmt.Println(ret)
// 		fmt.Printf("Elapsed time: %s", elasped.Round(time.Millisecond))
// 	}
// }
