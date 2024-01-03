package main

import (
	"maple-juice/config"
	"maple-juice/util"
	"maple-juice/membership"
	"maple-juice/leaderelection"
	"maple-juice/dfs"
	"maple-juice/logger"
	"maple-juice/maplejuice"
	"maple-juice/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
)



func main() {

	var cmd string
	var args []string

	util.InitSignals()
	config.InitConfig()

	util.CreateProcessLogger(config.LogFilePath)
	defer util.ProcessLogger.Close()
	

	membership.InitLocalMembershipList()

	if config.IsIntroducer {
		go membership.StartIntroducer()
	}

	go membership.StartMembershipListServer()
	go leaderelection.StartLeaderElectionServer()

	go dfs.StartFileReceiver(config.FileReceivePort)


	// register and start up rpc services
	fileMetadataService :=dfs.NewFileMetadataService()
	fileMetadataService.Register()
	grepService := logger.NewGrepService()
	grepService.Register()
	dfs.NewDfsRemoteReader().Register()
	fileService := dfs.NewFileService(config.RpcServerPort, config.Homedir, config.ServerHostnames)
	fileService.Register()
	mrNodeManager := new(maplejuice.MRNodeManager);
	mrNodeManager.Register();
	maplejuice.NewMRJobManager().Register();


	rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", config.RpcServerPort))
	if err != nil {
		log.Fatal("Failed to start RPC server", err)
	}
	go http.Serve(l, nil)



	// don't allow commands until all servers properly started
	fmt.Println("Starting servers...\n")
	util.WaitAllServerStart()
	dfs.InitializeClient()

	if config.IsIntroducer {
		fmt.Printf("Introducer service started at: %d.%d.%d.%d:%d\n", membership.LocalMembershipList.SelfEntry.Ip[0],
			membership.LocalMembershipList.SelfEntry.Ip[1],
			membership.LocalMembershipList.SelfEntry.Ip[2],
			membership.LocalMembershipList.SelfEntry.Ip[3],
			config.IntroducerPort)
	}

	fmt.Printf("Local membership service started at: %s\n\n", membership.LocalMembershipList.SelfEntry.Addr())
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

		"maple": "test maple",
		"juice": "test juice",
		"SELECT": "filter/join sql query. for command format please see SQL_client.go",
		"SPC" : "select percent composition, used for MP4 demo only. for command format please see SQL_client.go",

		// debug commands
		"pl": "print leader",
		"pm": "print metadata",
		"rp": "print local report",
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
			fmt.Println(membership.LocalMembershipList.ToString())
		case "list_self":
			// print local machine info
			fmt.Println(membership.LocalMembershipList.SelfEntry.ToString())
		case "leave":
			// voluntary leave
			util.SignalTermination()
			util.HEARTBEAT_SENDER_TERM.Wait()
			return
		case "enable_suspicion":
			// switch to GS
			if membership.LocalMembershipList.Protocol == util.GS {
				fmt.Println("Suspicion already enabled in current protocol. No changes were made")
			} else {
				membership.LocalMembershipList.UpdateProtocol(util.GS)
				fmt.Println("Switched protocol to GS")
			}
		case "disable_suspicion":
			// switch to G
			if membership.LocalMembershipList.Protocol == util.G {
				fmt.Println("Suspicion already disabled in current protocol. No changes were made")
			} else {
				membership.LocalMembershipList.UpdateProtocol(util.G)
				fmt.Println("Switched protocol to G")
			}
		case "droprate":
			if len(args) == 1 && util.IsValidDropRate(args[0]) {
				membership.ReceiverDropRate, _ = strconv.ParseFloat(args[0], 64)
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
		
		case "maple":
			maplejuice.ProcessMapleCmd(args)
		
		case "juice":
			maplejuice.ProcessJuiceCmd(args)

		case "SELECT":
			query := "SELECT " + strings.Join(args, " ")
			sql.ProcessSqlQuery(query)


		// debug commands
		case "pl":
			fmt.Println(leaderelection.LeaderId)
		case "pm":
			fmt.Println(fileMetadataService.ToString())
		case "rp":
			report := fileService.Report
			for _, report := range report.FileEntries{
				fmt.Println(report.ToString())
			}
			
		default:
			dfs.ProcessDfsCmd(cmd, args)
		}
	}
}
