package test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

var (
	_, b, _, _     = runtime.Caller(0)
	testFolderPath = filepath.Dir(b)
)

type LogService struct {
	LogFileDir string
}

type Args struct {
	FileContent string
	FileName    string
}

func (service *LogService) GenerateLog(args *Args, reply *string) error {
	filePath := fmt.Sprintf("./%s/%s", service.LogFileDir, args.FileName)
	err := writeToFile(filePath, args.FileContent)
	if err != nil {
		return err
	}
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

func localGrepMultipleFiles(pattern string, fileNames []string) (string, string) {

	testLogPath := fmt.Sprintf("%s/test_logs", testFolderPath)
	grepArgs := append([]string{"-c", pattern}, fileNames...)
	grepCmd := exec.Command("grep", grepArgs...)
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
