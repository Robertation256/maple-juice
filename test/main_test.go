package test

import (
	"fmt"
	"net/rpc"
	"os"
	"testing"

	"github.com/xinshuoLei/cs425-mp1/grep"
)

var ips = grep.LoadIps()
var clients = make([]*rpc.Client, len(ips))
var LogServiceNew = &LogService{LogFileDir: "./logs"}

// This function runs before any test functions are executed.
func TestMain(m *testing.M) {

	// Run the actual tests.
	exitCode := m.Run()

	// clean up
	fmt.Println("Clean up")
	grep.CloseClients(clients)
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

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t)

	distributedRes := grep.GrepAllMachines(ips, clients, pattern)

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

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t)

	distributedRes := grep.GrepAllMachines(ips, clients, pattern)

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

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t)

	distributedRes := grep.GrepAllMachines(ips, clients, pattern)

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

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t)

	distributedRes := grep.GrepAllMachines(ips, clients, pattern)

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

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t)

	distributedRes := grep.GrepAllMachines(ips, clients, pattern)

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
