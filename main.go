package main

import (
	"cs425-mp2/routines"
	"cs425-mp2/util"
	"fmt"
	"strconv"
)

func main() {
	var isBootstrapServer string
	var boostrapServicePort string
	var boostrapProtocol string
	var protocol uint8
	var memberListServerPort string
	var localMembershipList *util.MemberList
	var userCmd string
	var logFile string

	var boostrapServerAddr string

	// todo: fix name and path of log file
	util.Prompt("Enter log filename",
		&logFile,
		func(in string) bool { return true },
	)
	util.CreateProcessLogger(logFile)

	util.Prompt("Start as boostrap server? [Y/n]",
		&isBootstrapServer,
		func(in string) bool { return in == "Y" || in == "n" },
	)

	if isBootstrapServer == "Y" {
		util.Prompt("Please enter boostrap service port",
			&boostrapServicePort,
			util.IsValidPort)
		util.Prompt("Please enter protocol [G/GS]",
			&boostrapProtocol,
			func(in string) bool { return in == "G" || in == "GS" })
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

	validCommands := map[string]string{
		"list_mem":          "list the membership list",
		"list_self":         "list self’s id",
		"leave":             "voluntarily leave the group",
		"enable_suspicion":  "change protocol to GS",
		"disable_suspicion": "change protocol to G",
		"droprate":          "add an artificial drop rate",
	}

	defer util.ProcessLogger.Close()

	for {
		util.Prompt(`Enter a command (Type "help" for a list of available commands)`, &userCmd,
			func(in string) bool {
				if in == "help" {
					return true
				}
				for k, _ := range validCommands {
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
			fmt.Println(localMembershipList.ToString())
		case "list_self":
			// print self's id
			fmt.Println(localMembershipList.SelfEntry.ToString())
		case "leave":
			// leave the group
			// tell sender the status has been changed
			routines.SelfStatusChangedToLeft = true
			// wait until the left message is sent to other processes
			for {
				if routines.LeftMessageSent {
					// terminate main function, which will terminate the program
					// without waiting for other rountines to finish
					return
				}
			}
		case "enable_suspicion":
			// switch to GS
			if localMembershipList.Protocol == util.GS {
				fmt.Println("Suspicion already enabled in current protocol. No changes were made\n")
			} else {
				localMembershipList.UpdateProtocol(util.GS)
				fmt.Println("Switched protocol to GS\n")
			}
		case "disable_suspicion":
			// switch to G
			if localMembershipList.Protocol == util.G {
				fmt.Println("Suspicion already disabled in current protocol. No changes were made\n")
			} else {
				localMembershipList.UpdateProtocol(util.G)
				fmt.Println("Switched protocol to G\n")
			}
		case "droprate":
			var dropRate string
			util.Prompt(`Enter a drop rate (float between 0 and 1)`, &dropRate, util.IsValidDropRate)
			routines.ReceiverDropRate, _ = strconv.ParseFloat(dropRate, 64)
		case "help":
			for k, v := range validCommands {
				fmt.Printf("%s: %s\n", k, v)
			}
			fmt.Println()
		}
	}

}
