package main

import (
	"cs425-mp2/routines"
	"cs425-mp2/util"
	"cs425-mp2/config"
	"fmt"
	"strconv"
)


func main() {

	var userCmd string

	routines.InitSignals()
	config.InitConfig()

	util.CreateProcessLogger(config.LogFilePath)
	grepService := routines.NewGrepService()
	go grepService.Start()		

	routines.InitLocalMembershipList()

	if config.IsIntroducer {
		go routines.StartIntroducer()
	} 
	go routines.StartMembershipListServer()
	go routines.StartLeaderElectionServer()


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
		"log":		 		 "print logs from remote servers",
		"help":				 "command manual",

		// debug commands
		"pl": "print leader",
		
	}

	defer util.ProcessLogger.Close()

	for {
		util.Prompt(`Enter a command (Type "help" for a list of available commands)`, &userCmd,
			func(in string) bool {
				for k := range validCommands {
					if k == in {
						return true
					}
				}
				return false
			},
		)

		switch userCmd {
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
			var dropRate string
			util.Prompt(`Enter a drop rate (float between 0 and 1)`, &dropRate, util.IsValidDropRate)
			routines.ReceiverDropRate, _ = strconv.ParseFloat(dropRate, 64)
		case "log":
			fmt.Println(grepService.CollectLogs())
		case "help":
			for k, v := range validCommands {
				fmt.Printf("%s: %s\n", k, v)
			}
			fmt.Println()

		// debug commands
		case"pl":
			fmt.Println(routines.LeaderId)
		}
	}

}
