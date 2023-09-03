package test

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"strings"
	"testing"

	"github.com/xinshuoLei/cs425-mp1/grep"
)

var ips = grep.LoadIps()
var clients = make([]*rpc.Client, len(ips))
var LogServiceNew = &LogService{LogFileDir: "./logs"}

func TestGrepBasic(t *testing.T) {

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
		pattern := "111"
		patternCount := 0

		for i := 0; i < lineNumbers; i++ {
			fileContent += GenerateRandomString(10)
			addPattern := rand.Intn(3)
			if addPattern == 1 {
				fileContent += pattern
				patternCount++
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
		}

		grepArgs := grep.Args{Input: pattern}
		var grepResult string
		if clients[index] != nil {
			grepErr := clients[index].Call("GrepService.GrepLocal", grepArgs, &grepResult)
			if grepErr != nil {
				t.Fatal("Grep error:", grepErr)
			}
			// don't know why but the result contains two new line character
			correctResult := fmt.Sprintf("%s\t\t%d\n\n", fileName, patternCount)
			if correctResult != grepResult {
				t.Fatalf("Incorrect result. Got %s, but should be %s", grepResult, correctResult)
			} else {
				fmt.Printf("Correct result for %s: %d\n", ip, patternCount)
			}
		}
	}
}
