package routines

import (
	"log"
	"fmt"
	"os"
	"cs425-mp2/util"
	"net/rpc"
	"net"
	"net/http"
)


func StartLogServer(){
	localPort := "8000"

	homeDir, homeDirErr := os.UserHomeDir()
	if homeDirErr != nil {
		log.Fatal("Error getting user's home directory:", homeDirErr)
	}

	logFolder := homeDir + "/log"


	var ret string

	ips := util.LoadIps(homeDir)

	clients := make([]*rpc.Client, len(ips)) // stores clients with established connections

	defer util.CloseClients(clients)

	grepService := util.NewGrepService(logFolder)


	rpc.Register(grepService)
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

	// for {
	// 	ret = ""
	// 	fmt.Println("\n\n----------------------\n")
		
	// 	fmt.Println("Enter a grep command:")

	// 	in := bufio.NewReader(os.Stdin)
	// 	input, _ := in.ReadString('\n')

		
	// 	_, parseErr := util.ParseUserInput(input)
	// 	// if an error occured while parsing user input, prompt user to try again
	// 	if parseErr != nil {
	// 		fmt.Printf("Invalid input: %s. Please try again", parseErr)
	// 		continue
	// 	}
		
	// 	ret = util.GrepAllMachines(ips, clients, input)

		
	// 	fmt.Println(ret)
	// }
}

