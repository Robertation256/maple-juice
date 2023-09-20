package main

import (
	"cs425-mp2/routines"
	"cs425-mp2/util"
	"fmt"
	"strconv"
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
