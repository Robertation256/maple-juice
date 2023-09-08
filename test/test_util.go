package test

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

var (
	_, b, _, _     = runtime.Caller(0)
	testFolderPath = filepath.Dir(b)
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
}

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

func localGrepMultipleFiles(cmd string, fileNames []string) (string, string) {

	testLogPath := fmt.Sprintf("%s/test_logs", testFolderPath)
	grepCmd := exec.Command(cmd, fileNames...)
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

func GenerateRandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func GenerateRandomFile(minLineNumber int, maxLineNumber int, minLineLength int, maxLineLength int,
	pattern string, patternProbability float64) string {
	fileContent := ""
	lineNumbers := minLineNumber + rand.Intn(maxLineNumber-minLineNumber)

	for i := 0; i < lineNumbers; i++ {
		lineLength := minLineLength + rand.Intn(maxLineLength-minLineLength)
		currentLine := GenerateRandomString(lineLength)
		if rand.Float64() < patternProbability {
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
			c, err := rpc.DialHTTP("tcp", ip)
			if err == nil {
				clients[index] = c
			}
		}

		if clients[index] == nil {
			continue
		}

		// check if pattern should appear on machine
		if rand.Float64() >= args.machineProbability && args.machineProbability != -1 {
			args.patternProbability = 0
		}

		if args.machineProbability == -1 && oneMachineHasPattern {
			args.patternProbability = 0
		} else {
			oneMachineHasPattern = true
		}

		fileContent := GenerateRandomFile(args.minLineNumber, args.maxLineNumber,
			args.minLineLength, args.maxLineLength, args.pattern, args.patternProbability)
		args := Args{FileContent: fileContent}

		var logFilename string
		err := clients[index].Call("LogService.GenerateLog", args, &logFilename)
		if err != nil {
			t.Fatal("Generate log error:", err)
		}
		fmt.Println(logFilename)

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
