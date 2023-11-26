package routines

import (
	"bytes"
	"cs425-mp4/config"
	"cs425-mp4/util"
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	FILE_TRANSFER_BUFFER_SIZE int = 20*1024

	RECEIVER_SDFS_FILE_SERVER uint8 = 10
	RECEIVER_SDFS_CLIENT uint8 = 11
	RECEIVER_MR_JOB_MANAGER uint8 = 12
	RECEIVER_MR_NODE_MANAGER uint8 = 13

	WRITE_MODE_APPEND uint8 = 1		// append recieved file to an existing one
	WRITE_MODE_TRUNCATE uint8 = 0	// create or truncate file
)

var FileTransmissionProgressTracker *util.TransmissionProgressManager = util.NewTransmissionProgressManager()

type FileHeader struct {
	TransmissionIdLength uint64 	
	TransmissionId string
	FileNameLength uint64 
	FileName string
	ReceiverTag uint8
	WriteMode uint8
}


func (this *FileHeader) ToPayload() []byte{
	buf := bytes.NewBuffer(make([]byte, 0))

	idLengthArr := make([]byte, 8)
	fileNameLengthArr := make([]byte, 8)
	
	binary.LittleEndian.PutUint64(idLengthArr, this.TransmissionIdLength)
	binary.LittleEndian.PutUint64(fileNameLengthArr, this.FileNameLength)

	buf.Write(idLengthArr)
	buf.Write([]byte(this.TransmissionId))
	buf.Write(fileNameLengthArr)
	buf.Write([]byte(this.FileName))
	buf.WriteByte(this.ReceiverTag)
	buf.WriteByte(this.WriteMode)
	return buf.Bytes()
}


func StartFileReceiver(port int){
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port)) 
    if err != nil {
        log.Fatal("Failed to start file transfer server", err)
    }
    log.Println("File transfer server started on: " + listener.Addr().String())

	for {
		conn, err := listener.Accept()
        if err != nil {
            log.Println("File transfer server: error accepting tcp connection:", err)
            continue
        }

		go receiveFile(conn)
	}


}


func receiveFile(conn net.Conn){
	defer conn.Close()

	buf := make([]byte, FILE_TRANSFER_BUFFER_SIZE)

	var file *os.File
	var transmissionId *string

	total := 0

	for {
		n, err := conn.Read(buf)
		total += n
		if (file != nil){
			// log.Printf("Downloading file %s ----------- %d kb", file.Name(), total/1024)
		}
		if err == io.EOF {
			FileTransmissionProgressTracker.Complete(*transmissionId, LOCAL_WRITE_COMPLETE)
			// log.Printf("Completed receving file: %s", file.Name())
			return
		}
		if err != nil {
			log.Println("File transfer server: encountered error while receving file", err)
			return
		}

		if file == nil{
			f, tid := initializeFile(&buf, n)

			if f == nil {
				return
			}
			file = f
			transmissionId = tid
			defer file.Close()
		} else {
			_, err := file.Write(buf[:n])
			if err != nil {
				log.Print("File transfer server: failed to write to file.", err)
				return
			}
		}
	}
}

// parse out file header, create local file and return file pointer and transmission id
func initializeFile(buf *[]byte, size int) (*os.File, *string) {
	transmissionIdLength := binary.LittleEndian.Uint64((*buf)[:8])
	if int(transmissionIdLength) > 200 {
		log.Printf("Corrupted file header: transmissionId length exceed 200 characters with size: %d", int(transmissionIdLength))
		return nil, nil
	}
	transmissionId := string((*buf)[8:8+int(transmissionIdLength)])

	nameLength := binary.LittleEndian.Uint64((*buf)[8 + int(transmissionIdLength): 16 + int(transmissionIdLength)])
	fileName := string((*buf)[16 + int(transmissionIdLength): 16 + int(transmissionIdLength) + int(nameLength)])
	receiverTag := uint8((*buf)[16 + int(transmissionIdLength) + int(nameLength)])
	writeMode := uint8((*buf)[17 + int(transmissionIdLength) + int(nameLength)])
	headerSize := 18 + int(transmissionIdLength) + int(nameLength)

	targetFolder := ""

	switch receiverTag {
	case RECEIVER_SDFS_FILE_SERVER:
		targetFolder = config.SdfsFileDir
	case RECEIVER_SDFS_CLIENT:
		targetFolder = config.LocalFileDir
	case RECEIVER_MR_JOB_MANAGER:
		targetFolder = config.JobManagerFileDir
	case RECEIVER_MR_NODE_MANAGER:
		targetFolder = config.NodeManagerFileDir
	default:
		log.Printf("Unknown reciever tag %d", receiverTag)
		return nil, nil
	}


	filePath := strings.TrimSpace(targetFolder + "/" + fileName)

	var file *os.File
	var err error
	switch writeMode {
	case WRITE_MODE_TRUNCATE:
		file, err = os.Create(filePath)
	case WRITE_MODE_APPEND:
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
	
	if err != nil {
		log.Print("File transfer server: failed to create file.", err)
		return nil, &transmissionId
	}

	_, fileWriteErr := file.Write((*buf)[headerSize:size])

	if fileWriteErr != nil {
		log.Print("File transfer server: failed to complete initial write to file.", fileWriteErr)
		return nil, &transmissionId
	}


	return file, &transmissionId
}



func SendFile(localFilePath string, remoteFileName, remoteAddr string, transmissionId string, receiverTag uint8, writeMode uint8) error {

	var total uint64 = 0

	localFile, err := os.Open(localFilePath)
	if err != nil {
		log.Print("Error opening file", err)
		return err
	}
	defer localFile.Close()

	conn, err := net.Dial("tcp", remoteAddr)
    if err != nil {
		return err
    }
    defer conn.Close()

	buf := make([]byte, FILE_TRANSFER_BUFFER_SIZE)

	header := FileHeader{
		TransmissionIdLength: uint64(len(transmissionId)),
		TransmissionId: transmissionId,
		FileNameLength: uint64(len([]byte(remoteFileName))),
		FileName: remoteFileName,
		ReceiverTag: receiverTag,
		WriteMode: writeMode,
	}

	conn.Write(header.ToPayload())

	for {
		n, err := localFile.Read(buf)
		total += uint64(n)

		if err == io.EOF {

			// log.Printf("Finished sending file, remaining bytes is %d", n)
            return nil 	// we are finished
        }
		if err != nil {
			return err
		}

		// log.Printf("Writing file  %d kb written", total/1024)
		conn.Write(buf[:n])
	}	
}

