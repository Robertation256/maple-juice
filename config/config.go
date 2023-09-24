package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// log and grep configs, not intended for membership list service
	LogServerId string
	LogServerPort int 	
	LogServerHostnames []string
	LogFilePath string
}


func NewConfig() *Config {
	config := new(Config)

	homeDir, homeDirErr := os.UserHomeDir()
	if homeDirErr != nil {
		log.Fatal("Error loading configs: cannot locate home directory", homeDirErr)
	}

	var s []byte
	var err error
	s, err = os.ReadFile(homeDir + "/config.txt")
	if err != nil {
		log.Fatal("Error reading configs", err)
	}

	entries := strings.Split(string(s), "\n")
	for _, entry := range entries {
		kv := strings.Split(entry, "=")
		if len(kv) != 2 || len(kv[0]) == 0 || len(kv[1]) == 0 {
			continue
		}
		switch kv[0] {
		case "LOG_SERVER_PORT":
			port, err := strconv.Atoi(kv[1])
			if err != nil {
				log.Fatal("Error loading log server port")
			}
			config.LogServerPort = port
		case "LOG_FILE_NAME":
			config.LogFilePath = homeDir+"/"+kv[1]
		case "LOG_SERVER_ID":
			config.LogServerId = kv[1]
		case "LOG_SERVER_HOSTNAMES":
			hostnames := strings.Split(string(kv[1]), ",")
			if len(hostnames) == 0 {
				log.Fatal("Log server hostnames config is empty")
			}
			ret := make([]string, len(hostnames))
			for i:=0; i<len(ret); i++{
				ret[i] = strings.Trim(hostnames[i], " \n\r")
			}
			config.LogServerHostnames = ret
		}
	}
	log.Printf("Config loaded ------------------\n%s------------------\n", config.ToString())

	return config
}


func (this *Config) ToString() string {
	return fmt.Sprintf(
		"LOG_SERVER_HOSTNAMES: %s\n" +
		"LOG_SERVER_PORT: %d\n" +
		"LOG_FILE_PATH: %s\n" +
		"LOG_SERVER_ID: %s\n",
		strings.Join(this.LogServerHostnames, ","),
		this.LogServerPort,
		this.LogFilePath,
		this.LogServerId,
	)
}
