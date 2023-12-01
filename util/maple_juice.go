package util

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

var lineCountFileBuf []byte = make([]byte, 1024)

type JobRequest struct {
	IsMaple      bool
	ErrorMsgChan chan error
	MapleJob     MapleJobRequest
	JuiceJob     JuiceJobRequest
}

type MapleJobRequest struct {
	ExcecutableFileName string
	TaskNum             int
	SrcSdfsFileName     string
	OutputFilePrefix    string
}

type JuiceJobRequest struct {
	ExcecutableFileName string
	TaskNum             int
	SrcSdfsFilePrefix   string
	OutputFileName      string
	DeleteInput         bool
	IsHashPartition     bool 	// partition by hash or by range
}

type SimpleJobQueue struct {
	lock  sync.RWMutex
	queue []JobRequest
}

type MapleTaskArg struct {
	InputFileName       string
	TransmissionId      string
	ExcecutableFileName string
	OutputFilePrefix    string
}

type JuiceTaskArg struct {
	InputFilePrefix string
	KeyToFileNames      map[string][]string		// each key might have multiple file partitions
	ExcecutableFileName string
	OutputFilePrefix    string
}

func NewQueue() *SimpleJobQueue {
	return &SimpleJobQueue{
		queue: make([]JobRequest, 0),
	}
}

func (this *SimpleJobQueue) Push(job *JobRequest) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.queue = append(this.queue, *job)
}

func (this *SimpleJobQueue) Pop() *JobRequest {
	this.lock.Lock()
	defer this.lock.Unlock()
	if len(this.queue) == 0 {
		return nil
	}
	ret := this.queue[0]
	this.queue = this.queue[1:]
	return &ret
}

func GetFileLineCount(filePath string) (int, error) {

	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := file.Read(lineCountFileBuf)
		count += bytes.Count(lineCountFileBuf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

// caveat: need to resize buffer with lines over 64K
func PartitionFile(scanner *bufio.Scanner, lineNum int, outputFilePath string, header string) error {
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	outputFile.WriteString(header)

	for lineNum > 0 {
		if !scanner.Scan() {
			return scanner.Err()
		}
		lineNum -= 1
		line := scanner.Text() + "\n"
		_, err := outputFile.WriteString(line)
		if err != nil {
			return err
		}
	}
	return nil
}

func FmtMapleInputPartitionName(fileName string, taskId int) string {
	return fmt.Sprintf("%s-p%d", fileName, taskId)
}
