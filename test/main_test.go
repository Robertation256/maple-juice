package test

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"strings"
	"testing"
	"cs425-mp1/util"
)

var ips = util.LoadIps()
var clients = make([]*rpc.Client, len(ips))
var LogServiceNew = &LogService{LogFileDir: "./logs"}

func TestGrepBasic(t *testing.T) {

	defer util.CloseClients(clients)

	var localFileNames []string
	pattern := "111"

	// TODO: refactor log generation & write into a function so it can be reused
	for index, ip := range ips {
		// try start first time connection / reconnect for broken ones
		if clients[index] == nil {
			c, err := rpc.DialHTTP("tcp", ip)
			if err == nil {
				clients[index] = c
			}
		}

		fileContent := ""
		lineNumbers := 2 + rand.Intn(8)

		for i := 0; i < lineNumbers; i++ {
			fileContent += GenerateRandomString(10)
			addPattern := rand.Intn(3)
			if addPattern == 1 {
				fileContent += pattern
			}
			fileContent += "\n"
		}

		fileName := strings.Split(ip, ":")[1]
		fileName += ".txt"
		args := Args{FileContent: fileContent, FileName: fileName}
		var result string

		if clients[index] != nil {
			err := clients[index].Call("LogService.GenerateLog", args, &result)
			if err != nil {
				t.Fatal("Generate log error:", err)
			}

			// write a copy of the generated test log file to a local folder
			localFilePath := fmt.Sprintf("./test_logs/%s", fileName)
			writeErr := writeToFile(localFilePath, fileContent)
			if writeErr != nil {
				t.Fatal("Error writing log file to local folder", writeErr)
			}

			localFileNames = append(localFileNames, fileName)
		}
	}

	cmd := "grep -c "+ pattern
	distributedRes := util.GrepAllMachines(ips, clients, cmd)

	localRes, status := localGrepMultipleFiles(cmd, localFileNames)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	if distributedRes != localRes {
		t.Fatalf("Incorrect result. Should be %s\n but got %s\n", localRes, distributedRes)
	} else {
		fmt.Printf("Got correct result:\n%s", localRes)
	}
}
