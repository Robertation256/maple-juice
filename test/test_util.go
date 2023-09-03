package test

import (
	"fmt"
	"math/rand"
	"os"
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
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	file.WriteString(args.FileContent)

	return nil
}

func GenerateRandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
