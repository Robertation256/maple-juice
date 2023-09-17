package test

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"os/exec"
	"testing"
	"strconv"
	"strings"

	"cs425-mp2/util"
)

type LogService struct {
	LogFileDir string
	LogFilename string
}

type Args struct {
	FileContent string
}

type RandomFileArgs struct {
	minLineNumber      int
	maxLineNumber      int
	minLineLength      int
	maxLineLength      int
	pattern            string
	patternProbability float64
	machineProbability float64
	lowerOnly bool
}

// Takes a string as file content and write it to the log folder
func (service *LogService) GenerateLog(args *Args, reply *string) error {
	filePath := fmt.Sprintf("%s/%s", service.LogFileDir, service.LogFilename)
	err := writeToFile(filePath, args.FileContent)
	if err != nil {
		return err
	}
	*reply += service.LogFilename
	return nil
}

func writeToFile(filePath string, fileContent string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	file.WriteString(fileContent)
	return nil
}

// Perform a grep on a list of files specified by fileNames
func localGrepMultipleFiles(input string, fileNames []string, homeDir string) (string, string) {

	grepOptions, _ := util.ParseUserInput(input)

	testLogPath := fmt.Sprintf("%s/test_log_copy", homeDir)
	cmdArgs := append(grepOptions, fileNames...)
	// add the flag to force inclusion of filename, in case there is only one vm alive
	cmdArgs = append([]string{"-H"}, cmdArgs...)
	grepCmd := exec.Command("grep", cmdArgs...)
	grepCmd.Dir = testLogPath
	var out bytes.Buffer
	var stderr bytes.Buffer
	grepCmd.Stdout = &out
	grepCmd.Stderr = &stderr
	err := grepCmd.Run()
	// exit code 1 means a match was not found
	if err != nil && grepCmd.ProcessState.ExitCode() != 1 {
		return "", fmt.Sprint(err) + ": " + stderr.String()
	}
	return out.String(), "ok"
}

func GenerateRandomString(length int, lowerOnly bool) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if lowerOnly {
		letterBytes = "abcdefghijklmnopqrstuvwxyz"
	}
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func GenerateRandomFile(minLineNumber int, maxLineNumber int, minLineLength int, maxLineLength int,
	pattern string, patternProbability float64, lowerOnly bool) string {
	fileContent := ""
	lineNumbers := minLineNumber + rand.Intn(maxLineNumber-minLineNumber)
	

	for i := 0; i < lineNumbers; i++ { // for each line in the generated file
		lineLength := minLineLength + rand.Intn(maxLineLength-minLineLength)
		currentLine := GenerateRandomString(lineLength, lowerOnly)
		if rand.Float64() < patternProbability { // pattern should be inserted
			// find a random location in the file to insert the pattern
			insertPosition := rand.Intn(lineLength) 
			currentLine = currentLine[:insertPosition] + pattern + currentLine[insertPosition:]
		}
		fileContent += currentLine
		fileContent += "\n"
	}
	return fileContent
}

func PrepareLogFiles(args RandomFileArgs, ips []string, clients []*rpc.Client, t *testing.T, homeDir string) []string {

	var localFileNames []string
	oneMachineHasPattern := false
	for index, ip := range ips {
		// try start first time connection / reconnect for broken ones
		if clients[index] == nil {
			c, err := rpc.DialHTTP("tcp", ip+":8000")
			if err == nil {
				clients[index] = c
			}
		}

		if clients[index] == nil {
			continue
		}

		patternProbability := args.patternProbability

		// check if pattern should appear on machine
		randFloat := rand.Float64()
		if randFloat >= args.machineProbability && args.machineProbability != -1 {
			patternProbability = 0
		}

		if args.machineProbability == -1 && oneMachineHasPattern {
			patternProbability = 0
		} else {
			oneMachineHasPattern = true
		}

		fileContent := GenerateRandomFile(args.minLineNumber, args.maxLineNumber,
			args.minLineLength, args.maxLineLength, args.pattern, patternProbability, args.lowerOnly)
		args := Args{FileContent: fileContent}
		

		// use rpc call to generate log on remote machines
		var logFilename string
		err := clients[index].Call("LogService.GenerateLog", args, &logFilename)
		if err != nil {
			t.Fatal("Generate log error:", err)
		}

		// write a copy of the generated test log file to a local folder
		localFilePath := fmt.Sprintf("%s/test_log_copy/%s", homeDir, logFilename)
		writeErr := writeToFile(localFilePath, fileContent)
		if writeErr != nil {
			t.Fatal("Error writing log file to local folder", writeErr)
		}

		localFileNames = append(localFileNames, logFilename)
	}
	return localFileNames
}

func compareGrepResult(t *testing.T, localRes string, distributedRes string) string {
	splittedDistributedRes := strings.Split(distributedRes, "Total:")
	if len(splittedDistributedRes) < 2 {
		t.Fatal("Response doesn't include total line count")
	}
	distributedIndividual := splittedDistributedRes[0]
	distributedTotal := splittedDistributedRes[1]


	// other than the last Total: linecount line, the result from distributed grep
	// should be the exact same as the result from local grep
	if distributedIndividual != localRes {
		t.Fatalf("Incorrect line count for each file. Should be %s\n but got %s\n", localRes, distributedIndividual)
	} else {
		fmt.Printf("Got correct line count for each file:\n%s", localRes)
	}

	// check if total line count matches the sum of all individual file line count
	distributedSplittedLines := strings.Split(distributedIndividual, "\n")
	var correctCount int32 = 0
	for _, line := range distributedSplittedLines {
		count, countErr := util.ExtractLineCount(line)
		if countErr != nil {
			t.Fatal("Error extracting line count", countErr)
		} else {
			correctCount += count
		}
	}
	correctCountStr := strconv.FormatInt(int64(correctCount), 10)
	if string(correctCountStr) != distributedTotal {
		t.Fatalf("Incorrect total line count. Should be %s but got %s\n", correctCountStr, distributedTotal)
	} else {
		fmt.Printf("Got correct total line count:%s\n", distributedTotal)
	}
	return distributedTotal
}

func compareArrays(a []string , b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, _ := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
