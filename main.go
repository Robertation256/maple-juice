package main

import (
	"cs425-mp2/config"
	"cs425-mp2/routines"
	"cs425-mp2/util"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)


// file server test -------------

// func main() {
// 	config.InitConfig()

// 	fileService := routines.NewFileService(config.RpcServerPort, config.Homedir)
// 	fileService.Start()

// 	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("fa23-cs425-3801.cs.illinois.edu:%d", config.RpcServerPort))
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}

// 	// putArgs := &routines.PutArgs{
// 	// 	LocalFilePath: config.Homedir + "/cs-425-mp-3/test.txt", 
// 	// 	RemoteFilePath: config.Homedir + "/test-sftp.txt", 
// 	// 	RemoteAddr: "fa23-cs425-3803.cs.illinois.edu",
// 	// }

// 	var reply string

// 	// client.Call("FileService.PutFile", putArgs, &reply)
	
// 	servants := []string{"fa23-cs425-3802.cs.illinois.edu"}
// 	createArgs := &routines.CreateFMArgs{
// 		Filename: "test-folder.txt",
// 		Servants: servants,
// 	}

// 	createArgs1 := &routines.CreateFMArgs{
// 		Filename: "test-another.txt",
// 		Servants: servants,
// 	}

// 	client.Call("FileService.CreateFileMaster", createArgs, &reply)
// 	client.Call("FileService.CreateFileMaster", createArgs1, &reply)

	

// 	// readArgs := &routines.RWArgs{
// 	// 	LocalFilename: "test-another-copy-2.txt",
// 	// 	SdfsFilename: "test-another.txt",
// 	// 	ClientAddr: "fa23-cs425-3803.cs.illinois.edu",
// 	// }

// 	writeArgs := &routines.RWArgs{
// 		LocalFilename: "test-fm.txt",
// 		SdfsFilename: "test-folder.txt",
// 		ClientAddr: "fa23-cs425-3801.cs.illinois.edu:9000",
// 	}

// 	// writeArgs1 := &routines.RWArgs{
// 	// 	Filename: "test-another.txt",
// 	// 	ClientAddr: "fa23-cs425-3801.cs.illinois.edu",
// 	// }

// 	// deleteArgs := &routines.DeleteArgs{
// 	// 	Filename: "test-fm.txt",
// 	// }

// 	client.Call("FileService.WriteFile", writeArgs, &reply)
// 	// client.Call("FileService.ReadFile", readArgs, &reply)
// 	// client.Call("FileService.DeleteFile", deleteArgs, &reply)


// 	//fm := routines.NewFileMaster(config.Homedir + "/" + "test-fm.txt", servants, fileService.SshConfig)
// 	// err := fm.WriteFile("fa23-cs425-3801.cs.illinois.edu:8000")
// 	// fmt.Println(err)
// 	// fm.ReadFile("fa23-cs425-3803.cs.illinois.edu")
// }

// --------------------

func main() {

	var cmd string
	var args []string

	routines.InitSignals()
	config.InitConfig()

	util.CreateProcessLogger(config.LogFilePath)
	defer util.ProcessLogger.Close()
	

	routines.InitLocalMembershipList()

	if config.IsIntroducer {
		go routines.StartIntroducer()
	}

	go routines.StartMembershipListServer()
	go routines.StartLeaderElectionServer()

	// receiver for file server
	go routines.StartFileReceiver(config.Homedir+"/sdfs", config.FileServerReceivePort)


	// register and start up rpc services
	fileMetadataService :=routines.NewFileMetadataService()
	fileMetadataService.Register()
	grepService := routines.NewGrepService()
	grepService.Register()
	routines.NewDfsRemoteReader().Register()
	fileService := routines.NewFileService(config.RpcServerPort, config.Homedir, config.ServerHostnames)
	fileService.Register()
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", config.RpcServerPort))
	if err != nil {
		log.Fatal("Failed to start RPC server", err)
	}
	go http.Serve(l, nil)



	// don't allow commands until all servers properly started
	fmt.Println("Starting servers...\n")
	routines.WaitAllServerStart()

	if config.IsIntroducer {
		fmt.Printf("Introducer service started at: %d.%d.%d.%d:%d\n", routines.LocalMembershipList.SelfEntry.Ip[0],
			routines.LocalMembershipList.SelfEntry.Ip[1],
			routines.LocalMembershipList.SelfEntry.Ip[2],
			routines.LocalMembershipList.SelfEntry.Ip[3],
			config.IntroducerPort)
	}

	fmt.Printf("Local membership service started at: %s\n\n", routines.LocalMembershipList.SelfEntry.Addr())
	validCommands := map[string]string{
		"list_mem":          "list the membership list",
		"list_self":         "list local machine info",
		"leave":             "voluntarily leave the group",
		"enable_suspicion":  "change protocol to GS",
		"disable_suspicion": "change protocol to G",
		"droprate":          "add an artificial drop rate",
		"log":               "print logs from remote servers",
		"store":			 "list local files hosted by DFS",
		"help":              "command manual",
		"put":				 "put localfilename sdfsfilename (from local dir",
		"get":				 "get sdfsfilename localfilename (fetch to local dir)",
		"delete":  			 "delete sdfsfilename",
		"ls":				 "ls sdfsfilename: list all VM addresses where this file is currently replicated (If you are splitting files into blocks, just set the block size to be large enough that each file is one block)",
		"multiread": 		 "launches reads from VMi… VMj simultaneously to filename. (Note that you have to implement this anyway for your report's item (iv) experiments).",

		// debug commands
		"pl": "print leader",
		"pm": "print metadata",
		"send": "test tcp send",
	}

	for {
		util.Prompt(`Enter a command (Type "help" for a list of available commands)`, &cmd, &args,
			func(cmdValue string) bool {
				for k := range validCommands {
					if k == cmdValue {
						return true
					}
				}
				return false
			},
		)

		switch cmd {
		case "list_mem":
			// print membership list
			fmt.Println(routines.LocalMembershipList.ToString())
		case "list_self":
			// print local machine info
			fmt.Println(routines.LocalMembershipList.SelfEntry.ToString())
		case "leave":
			// voluntary leave
			routines.SignalTermination()
			routines.HEARTBEAT_SENDER_TERM.Wait()
			return
		case "enable_suspicion":
			// switch to GS
			if routines.LocalMembershipList.Protocol == util.GS {
				fmt.Println("Suspicion already enabled in current protocol. No changes were made")
			} else {
				routines.LocalMembershipList.UpdateProtocol(util.GS)
				fmt.Println("Switched protocol to GS")
			}
		case "disable_suspicion":
			// switch to G
			if routines.LocalMembershipList.Protocol == util.G {
				fmt.Println("Suspicion already disabled in current protocol. No changes were made")
			} else {
				routines.LocalMembershipList.UpdateProtocol(util.G)
				fmt.Println("Switched protocol to G")
			}
		case "droprate":
			if len(args) == 1 && util.IsValidDropRate(args[0]) {
				routines.ReceiverDropRate, _ = strconv.ParseFloat(args[0], 64)
			} else {
				fmt.Println("Invalid drop rate input, expected floating point number")
			}
		case "log":
			fmt.Println(grepService.CollectLogs())
		
		case "store":
			// todo: prunce out files that are not complete

			localFiles := fileService.Report.FileEntries
			for _, f := range localFiles{
				fmt.Println(f.ToString())
			}

		case "help":
			for k, v := range validCommands {
				fmt.Printf("%s: %s\n", k, v)
			}
			fmt.Println()

		// debug commands
		case "pl":
			fmt.Println(routines.LeaderId)
		case "pm":
			fmt.Println(fileMetadataService.ToString())
		
		case "send":
			routines.SendFile(config.Homedir+"/local/test1.txt", "tcp_sent", config.ServerHostnames[1]+":"+strconv.Itoa(config.FileServerReceivePort))

		default:
			routines.ProcessDfsCmd(cmd, args)
		}
	}
}
