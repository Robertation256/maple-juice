package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// membership service config
var MembershipServicePort int
var MembershipProtocol string // G/GS
var IsIntroducer bool = false
var IntroducerIp string
var IntroducerPort int

// leader election config
var LeaderElectionServerPort int
var LeaderElectionQuorumSize int

// file server config
var ReplicationFactor int
var FileReceivePort int


// distributed logging and grep configs
var LogServerId string
var LogFilePath string

// RPC server config
var RpcServerPort int
var ServerHostnames []string

// file server configs
var Homedir string
var SdfsFileDir string
var LocalFileDir string

// Maple Juice file directories
var JobManagerFileDir string // for storing partitioned maple input files
var NodeManagerFileDir string

var TemplateFileDir string


func InitConfig() {

	homeDir, homeDirErr := os.UserHomeDir()
	if homeDirErr != nil {
		log.Fatal("Error loading configs: cannot locate home directory", homeDirErr)
	}

	var s []byte
	var err error
	s, err = os.ReadFile(homeDir + "/config.txt")
	if err != nil {
		log.Fatal("Error reading configs ", err)
	}

	entries := strings.Split(string(s), "\n")
	for _, entry := range entries {
		kv := strings.Split(entry, "=")
		if len(kv) != 2 || len(kv[0]) == 0 || len(kv[1]) == 0 {
			continue
		}

		switch kv[0] {
		case "MEMBERSHIP_SERVICE_PORT":
			port, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading membership service port")
			}
			MembershipServicePort = port
		case "MEMBERSHIP_PROTOCOL":
			if kv[1] != "G" && kv[1] != "GS" {
				log.Fatalf("Invalid membership protocol %s", kv[1])
			}
			MembershipProtocol = kv[1]
		case "IS_INTRODUCER":
			if kv[1] == "TRUE" {
				IsIntroducer = true
			}
		case "INTRODUCER_IP":
			if len(kv[1]) > 0 {
				IntroducerIp = kv[1]
			} else {
				log.Fatalf("Invalid introducer ip %s", kv[1])
			}
		case "INTRODUCER_PORT":
			port, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading introducer port")
			}
			IntroducerPort = port

		case "LEADER_ELECTION_SERVER_PORT":
			port, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading leader election server port")
			}
			LeaderElectionServerPort = port
		case "LEADER_ELECTION_QUORUM_SIZE":
			size, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading leader election quorum size")
			}
			LeaderElectionQuorumSize = size

		case "REPLICATION_FACTOR":
			factor, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading file server port")
			}
			ReplicationFactor = factor

		case "LOG_FILE_NAME":
			LogFilePath = homeDir + "/" + kv[1]
		case "LOG_SERVER_ID":
			LogServerId = kv[1]
		case "SERVER_HOSTNAMES":
			hostnames := strings.Split(string(kv[1]), ",")
			if len(hostnames) == 0 {
				log.Fatal("Server hostnames config is empty")
			}
			ret := make([]string, len(hostnames))
			for i := 0; i < len(ret); i++ {
				ret[i] = strings.Trim(hostnames[i], " \n\r")
			}
			ServerHostnames = ret

		case "RPC_SERVER_PORT":
			port, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading rpc server port")
			}
			RpcServerPort = port

		case "FILE_RECEIVE_PORT":
			port, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading file receive port")
			}
			FileReceivePort = port

		}
	}
	Homedir = homeDir
	SdfsFileDir = Homedir + "/sdfs/"
	LocalFileDir = homeDir + "/local/"
	JobManagerFileDir = homeDir + "/mr_job_manager/"
	NodeManagerFileDir = homeDir + "/mr_node_manager/"
	TemplateFileDir = homeDir + "/sql_template/"


	if FileReceivePort == 0 {
		log.Fatal("File receive port is not properly configured")
	}

	PrintConfig()
}

func PrintConfig() {

	configStr := fmt.Sprintf(
		"MEMBERSHIP_SERVICE_PORT: %d\n"+
			"MEMBERSHIP_PROTOCOL: %s\n"+
			"IS_INTRODUCER: %t\n"+
			"INTRODUCER_IP: %s\n"+
			"INTRODUCER_PORT: %d\n"+
			"LEADER_ELECTION_SERVER_PORT: %d\n"+
			"LEADER_ELECTION_QUORUM_SIZE: %d\n"+
			"REPLICATION_FACTOR: %d\n"+
			"LOG_FILE_PATH: %s\n"+
			"LOG_SERVER_ID: %s\n"+
			"LOG_SERVER_HOSTNAMES: %s\n"+
			"RPC_SERVER_PORT: %d\n"+
			"FILE_RECEIVE_PORT: %d\n",

		MembershipServicePort,
		MembershipProtocol,
		IsIntroducer,
		IntroducerIp,
		IntroducerPort,
		LeaderElectionServerPort,
		LeaderElectionQuorumSize,
		ReplicationFactor,
		LogFilePath,
		LogServerId,
		strings.Join(ServerHostnames, ","),
		RpcServerPort,
		FileReceivePort,
	)

	log.Printf("\n---Config loaded---\n%s-------------------\n", configStr)
}
