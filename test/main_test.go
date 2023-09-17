package test

import (
	"fmt"
	"net/rpc"
	"os"
	"testing"
	"cs425-mp2/util"

)

var homeDir string
var homeDirErr error
var ips []string
var clients []*rpc.Client



// This function runs before any test functions are executed.
func TestMain(m *testing.M) {
	// setup
	fmt.Println("Setting up test cases...")

	homeDir, homeDirErr = os.UserHomeDir()
	if homeDirErr != nil {
		fmt.Println("Error getting user's home directory:", homeDirErr)
		os.Exit(1)
	}
	ips = util.LoadIps(homeDir)
	clients = make([]*rpc.Client, len(ips))

	fmt.Println("Running tests")
	// Run the actual tests.
	exitCode := m.Run()

	// clean up
	fmt.Println("Cleaning up...")
	util.CloseClients(clients)
	os.Exit(exitCode)
}

// Test whether ExtractLineCount gives correct result when the 
// input format is correct
func TestExtractLineCountCorrectFormat(t *testing.T) {

	answerMap := make(map[string]int32)
	answerMap["some:2"] = 2
	answerMap["another:123"] = 123
	answerMap["vm3.log:89999"] = 89999
	answerMap[" \n"] = 0


	for arg, answer := range answerMap {
		funcAnswer, err := util.ExtractLineCount(arg)
		if err != nil {
			t.Fatal("Got error: ", err)
		}
		if funcAnswer != answer {
			t.Fatalf("Incorrect result for %s, should be %d but got %d", arg, answer, funcAnswer)
		}
	}
}


// Test whether ExtractLineCount throws error on invalid input
func TestExtractLineCountIncorrectFormat(t *testing.T) {

	args := []string{"some2", "some:another", "another:", "vm1.log:"}

	for _, arg := range args {
		_, err := util.ExtractLineCount(arg)
		if err == nil {
			t.Fatalf("Excepted error for incorrect input " + arg)
		}
	}

}

// Test whether PareseInput gives correct result when the 
// input format is correct
func TestParseInputCorrectFormat(t *testing.T) {

	answerMap := make(map[string][]string)
	answerMap["grep -c 22"] = []string{"-c", "22"}
	answerMap["grep -c -E sth"] = []string{"-c", "-E", "sth"}
	answerMap["grep -i -c -E aQ"] = []string{"-i", "-c", "-E", "aQ"}
	answerMap[`grep -i -c -E "sth, another!"`] =  []string{"-i", "-c", "-E", "sth, another!"}


	for arg, answer := range answerMap {
		funcAnswer, err := util.ParseUserInput(arg)
		if err != nil {
			t.Fatal("Got error: ", err)
		}
		if !compareArrays(answer, funcAnswer) {
			t.Fatalf("Incorrect result for %s, should be %s but got %s", arg, answer, funcAnswer)
		}
	}
}

// Test whether PareseInput throws error on invalid input
func TestParseInputIncorrectFormat(t *testing.T) {

	args := []string{"grep", "grep 111", "cd lol"}

	for _, arg := range args {
		_, err := util.ParseUserInput(arg)
		if err == nil {
			t.Fatalf("Excepted error for incorrect input " + arg)
		}
	}

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
		lowerOnly: false,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c %s\n", pattern)
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	compareGrepResult(t, localRes, distributedRes)
	
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
		lowerOnly: false,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c %s\n", pattern)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	compareGrepResult(t, localRes, distributedRes)
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
		lowerOnly: false,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c %s\n", pattern)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	compareGrepResult(t, localRes, distributedRes)
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
		patternProbability: 0.8,
		machineProbability: 0.3,
		lowerOnly: false,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c %s\n", pattern)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	compareGrepResult(t, localRes, distributedRes)
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
		machineProbability: -1, // a special case. only one machine has pattern
		lowerOnly: false,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c %s\n", pattern)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)
	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	compareGrepResult(t, localRes, distributedRes)
}

// Each machine contains a log file from 100-150 lines
// the pattern is W, since the log files are only generated with lowercase letters
// a total count of 0 will mean the -i flag is not applied
func TestGrepAdditionalFlag(t *testing.T) {
	pattern := "W"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      100,
		maxLineNumber:      150,
		minLineLength:      60,
		maxLineLength:      100,
		pattern:            "", // don't force any pattern
		patternProbability: 0,
		machineProbability: 0, 
		lowerOnly: true,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c -i %s\n", pattern)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)

	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	totalCount := compareGrepResult(t, localRes, distributedRes)
	if totalCount == "0" {
		t.Fatal("Flag not applied")
	}
}

// Each machine contains a log file from 100-150 lines
// the pattern to seach for is one or more repetition of 123
// and the pattern inserted in files is 123123
func TestGrepRegEx(t *testing.T) {
	pattern := "(123)+"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      100,
		maxLineNumber:      150,
		minLineLength:      60,
		maxLineLength:      100,
		pattern:            "123123", // don't force any pattern
		patternProbability: 0.8,
		machineProbability: 1, 
		lowerOnly: true,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	input := fmt.Sprintf("grep -c -E %s\n", pattern)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)

	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	totalCount := compareGrepResult(t, localRes, distributedRes)
	if totalCount == "0" {
		t.Fatal("Flag not applied")
	}
}

// Each machine contains a log file from 100-150 lines
// the pattern to seach for 1, 23, which contains space and must be enclosed with double quotes in the input
func TestGrepPatternWithSpace(t *testing.T) {
	pattern := "1, 23"

	randomFileArgs := RandomFileArgs{
		minLineNumber:      100,
		maxLineNumber:      150,
		minLineLength:      60,
		maxLineLength:      100,
		pattern:            pattern, // don't force any pattern
		patternProbability: 0.7,
		machineProbability: 1, 
		lowerOnly: true,
	}

	localFileNames := PrepareLogFiles(randomFileArgs, ips, clients, t, homeDir)

	// enclose pattern with double quotes, mimic user input
	input := fmt.Sprintf("grep -c %s\n", `"1, 23"`)
	_, parseErr := util.ParseUserInput(input)
	if parseErr != nil {
		t.Fatal("Error parsing pattern", parseErr)
	}
	distributedRes := util.GrepAllMachines(ips, clients, input)

	localRes, status := localGrepMultipleFiles(input, localFileNames, homeDir)

	if status != "ok" {
		t.Fatal("Local grep error:", status)
	}

	totalCount := compareGrepResult(t, localRes, distributedRes)
	if totalCount == "0" {
		t.Fatal("Could not match space or pattern enclosed with double quotes")
	}
}