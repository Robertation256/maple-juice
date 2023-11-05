package routines

import (
	"bytes"
	"cs425-mp2/config"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strconv"
	"os"
	"strings"
)

const (
	FILE_TRANSFER_BUFFER_SIZE int = 1024*1024
)

type FilerHeader struct {
	FileSize uint64 	// file size in bytes
	FileNameLength uint64 //length of name
	FileName string
}


func (this *FilerHeader) ToPayload() []byte{
	buf := bytes.NewBuffer(make([]byte, 0))

	fileSizeArr := make([]byte, 8)
	fileNameLengthArr := make([]byte, 8)
	

	binary.LittleEndian.PutUint64(fileSizeArr, this.FileSize)
	binary.LittleEndian.PutUint64(fileNameLengthArr, this.FileNameLength)

	buf.Write(fileSizeArr)
	buf.Write(fileNameLengthArr)
	buf.Write([]byte(this.FileName))
	return buf.Bytes()
}


func StartFileTransferServer(receiverFileFolder string){
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(config.FileTransferPort))
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

		go receiveFile(conn, receiverFileFolder)
	}


}




func receiveFile(conn net.Conn, targetFolder string){
	defer conn.Close()

	buf := make([]byte, FILE_TRANSFER_BUFFER_SIZE)
	
	bytesRemained := 0

	var file *os.File





	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("File transfer server: encountered error while receving file", err)
			return
		}

		if file == nil{
			f, remain := initializeFile(targetFolder, &buf, n)
			if f == nil {
				return
			}
			file = f
			bytesRemained = remain
			defer file.Close()
		} else {
			bytesRemained -= n
			_, err := file.Write(buf[:n])
			if err != nil {
				log.Printf("File transfer server: failed to write to file.", err)
				return
			}

			log.Printf("Receiving file  %d bytes remains", bytesRemained)
			if bytesRemained <= 0 {
				log.Printf("Completed receving file to %d", file.Name())
				return
			}

		}
	}
}

// parse out file header, create local file and return file pointer,  and remaining file size
func initializeFile(targetFolder string, buf *[]byte, size int) (*os.File, int) {

	

	fileSize := binary.LittleEndian.Uint64((*buf)[:8])
	nameLength := binary.LittleEndian.Uint64((*buf)[8:16])
	fileName := string((*buf)[16:16+int(nameLength)])

	headerSize := 16 + int(nameLength)
	dataSize := len(*buf) - (headerSize)	// data size in this buffer

	remainingBytesToRead := int(fileSize) - dataSize

	filePath := targetFolder + fileName
	file, err := os.Create(strings.TrimSpace(filePath))
	if err != nil {
		log.Printf("File transfer server: failed to create file.", err)
		return nil, 0
	}

	_, fileWriteErr := file.Write((*buf)[headerSize:size])

	if fileWriteErr != nil {
		log.Printf("File transfer server: failed to complete initial write to file.", fileWriteErr)
		return nil, 0
	}


	return file, remainingBytesToRead
}



func SendFile(){}

