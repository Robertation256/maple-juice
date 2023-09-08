package test

import (
	"fmt"
	"net/rpc"
	"os"
	"testing"

	"cs425-mp1/util"
)

var homeDir string
var homeDirErr error
var ips []string
var clients []*rpc.Client



// This function runs before any test functions are executed.
func TestMain(m *testing.M) {
	// setup
	homeDir, homeDirErr = os.UserHomeDir()
	if homeDirErr != nil {
		fmt.Println("Error getting user's home directory:", homeDirErr)
		os.Exit(1)
	}
	ips = util.LoadIps(homeDir)
	clients = make([]*rpc.Client, len(ips))

	// Run the actual tests.
	exitCode := m.Run()

	// clean up
	fmt.Println("Clean up")
	util.CloseClients(clients)
	os.Exit(exitCode)
}

// Each machine contains a log file from 2-10 lines
// the pattern is 111 and has a 0.1 probablity to appear on each line
func TestGrepRare(t *testing.T) {

	pattern := "111"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      2,
		maxLineNumber:      10,
		minLineLength:      5,
		maxLineLength:      15,
		pattern:            pattern,
		patternProbability: 0.1,
		machineProbability: 1,
	}

	fmt.Println(homeDir)
	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)
	fmt.Println(localFileNames)

	input := fmt.Sprintf("grep -c %s", pattern)
	distributedRes := util.GrepAllMachines(ips, clients, input)

	// localRes, status := localGrepMultipleFiles(pattern, localFileNames)
	// if status != "ok" {
	// 	t.Fatal("Local grep error:", status)
	// }

	// if distributedRes != localRes {
	// 	t.Fatalf("Incorrect result. Should be %s\n but got %s\n", localRes, distributedRes)
	// } else {
	// 	fmt.Printf("Got correct result:\n%s", localRes)
	// }
	fmt.Println(distributedRes)
}

// Each machine contains a log file from 10-20 lines
// the pattern is 123 and has a 0.8 probablity to appear on each line
func TestGrepFrequentPattern(t *testing.T) {

	pattern := "123"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      10,
		maxLineNumber:      20,
		minLineLength:      5,
		maxLineLength:      15,
		pattern:            pattern,
		patternProbability: 0.8,
		machineProbability: 1,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	distributedRes := util.GrepAllMachines(ips, clients, pattern)

	localRes, status := localGrepMultipleFiles(pattern, localFileNames)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	if distributedRes != localRes {
		t.Fatalf("Incorrect result. Should be %s\n but got %s\n", localRes, distributedRes)
	} else {
		fmt.Printf("Got correct result:\n%s", localRes)
	}
}

// Each machine contains a log file from 5-20 lines
// the pattern is 123456 and has a 0.5 probablity to appear on each line
func TestGrepSomewhatFrequentPattern(t *testing.T) {

	pattern := "123456"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      5,
		maxLineNumber:      20,
		minLineLength:      5,
		maxLineLength:      15,
		pattern:            pattern,
		patternProbability: 0.5,
		machineProbability: 1,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	distributedRes := util.GrepAllMachines(ips, clients, pattern)

	localRes, status := localGrepMultipleFiles(pattern, localFileNames)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	if distributedRes != localRes {
		t.Fatalf("Incorrect result. Should be %s\n but got %s\n", localRes, distributedRes)
	} else {
		fmt.Printf("Got correct result:\n%s", localRes)
	}
}

// Each machine contains a log file from 5-20 lines
// the pattern is 1321 and has a 0.7 probablity to appear on each machine
func TestPatternOnSomeMachines(t *testing.T) {

	pattern := "1321"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      5,
		maxLineNumber:      20,
		minLineLength:      5,
		maxLineLength:      15,
		pattern:            pattern,
		patternProbability: 0.5,
		machineProbability: 0.7,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	distributedRes := util.GrepAllMachines(ips, clients, pattern)

	localRes, status := localGrepMultipleFiles(pattern, localFileNames)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	if distributedRes != localRes {
		t.Fatalf("Incorrect result. Should be %s\n but got %s\n", localRes, distributedRes)
	} else {
		fmt.Printf("Got correct result:\n%s", localRes)
	}
}

// Each machine contains a log file from 5-20 lines
// the pattern is 178 and only appear on one machine
func TestPatternOnOneMachine(t *testing.T) {

	pattern := "178"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      5,
		maxLineNumber:      20,
		minLineLength:      5,
		maxLineLength:      15,
		pattern:            pattern,
		patternProbability: 0.5,
		machineProbability: -1, // a special case
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	distributedRes := util.GrepAllMachines(ips, clients, pattern)

	localRes, status := localGrepMultipleFiles(pattern, localFileNames)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	if distributedRes != localRes {
		t.Fatalf("Incorrect result. Should be %s\n but got %s\n", localRes, distributedRes)
	} else {
		fmt.Printf("Got correct result:\n%s", localRes)
	}
}
