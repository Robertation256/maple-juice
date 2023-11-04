package routines

import (
	"fmt"
	"net/rpc"
	"log"
	"golang.org/x/crypto/ssh"
	"cs425-mp2/util"
	"os"
	"strings"
)

type FileMaster struct {
	Queue        []*Request
	WriteQueue   []*Request
	CurrentRead  int
	CurrentWrite int
	Filename     string
	Servants     []string
	SelfAddr 	 string
	// TODO: remove this and integrate filemaster into file server
	SshConfig 	 *ssh.ClientConfig
}

type Request struct {
	Type      string
	InQueue   bool
	WaitRound int
}

func NewFileMaster(filename string, servants []string, sshConfig *ssh.ClientConfig) *FileMaster {
	selfAddr, _ := os.Hostname()
	return &FileMaster{
		CurrentRead:  0,
		CurrentWrite: 0,
		Filename:     filename,
		Servants: servants,
		SshConfig: sshConfig,
		SelfAddr: selfAddr,
	}
}

func (fm *FileMaster) CheckQueue() {
	if len(fm.Queue) > 0 {
		if !fm.Queue[0].InQueue {
			fm.Queue = fm.Queue[1:]
			fm.CheckQueue()
			return
		}
		if len(fm.Queue) == 0 {
			return
		}
		if fm.Queue[0].Type == "R" {
			// satisfy read condition
			if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
				// no write has been waiting for more than 4 consecutive read
				if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
					fm.Queue[0].InQueue = false
					fm.Queue = fm.Queue[1:]
					return
				} else {
					// note now queue might have a request that have InQueue = false
					fm.WriteQueue[0].InQueue = false
					fm.WriteQueue = fm.WriteQueue[1:]
				}
			}
		} else {
			// write request
			if fm.CurrentRead == 0 && fm.CurrentWrite == 0 {
				fm.WriteQueue[0].InQueue = false
				fm.WriteQueue = fm.WriteQueue[1:]

				fm.Queue[0].InQueue = false
				fm.Queue = fm.Queue[1:]
			} else if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
				// write is blocked because there is exactly 1 reader
				// then allow another read request
				for _, request := range fm.Queue {
					if request.Type == "R" && fm.WriteQueue[0].WaitRound < 4 {
						request.InQueue = false
					}
				}
			}
		}
	}
}

func (fm *FileMaster) ReadFile(clientAddr string) error {
	var request *Request = nil
	for {
		if request == nil && fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
			if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
				return fm.executeRead(clientAddr)
			}
		} else if request == nil {
			request = &Request{
				Type:    "R",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
		} else if request != nil && !request.InQueue {
			return fm.executeRead(clientAddr)
		}
	}
}

func (fm *FileMaster) executeRead(clientAddr string) error {
	fm.CurrentRead += 1
	for _, writeRequest := range fm.WriteQueue {
		writeRequest.WaitRound += 1
	}

	fmt.Println("read" + fm.Filename)
	// TODO: change this to send file from servant
	util.CopyFileToRemote(fm.Filename, fm.Filename, clientAddr, fm.SshConfig)


	fm.CurrentRead -= 1
	fm.CheckQueue()
	return nil
}

func (fm *FileMaster) WriteFile(clientAddr string) error {
	var request *Request = nil
	for {
		if request == nil && fm.CurrentWrite == 0 && fm.CurrentRead == 0 {
			return fm.executeWrite(clientAddr)
		} else if request == nil {
			request = &Request{
				Type:    "W",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
			fm.WriteQueue = append(fm.WriteQueue, request)
		} else if request != nil && !request.InQueue {
			return fm.executeWrite(clientAddr)
		}
	}
}

func (fm *FileMaster) executeWrite(clientAddr string) error {
	fm.CurrentWrite += 1

	// time.Sleep(4 * time.Second)
	fmt.Println("write" + fm.Filename)

	clientIp := clientAddr
	if (strings.Contains(clientAddr, ":")) {
		clientIp = strings.Split(clientAddr, ":")[0]
	}
	if (clientIp != fm.SelfAddr) {
		// if client is not self, get the file from client
		client, err := rpc.DialHTTP("tcp", clientAddr)
		if err != nil {
			log.Fatal("Error dialing client:", err)
		}
		fromClientArgs := CopyArgs{
			LocalFilePath: fm.Filename, 
			RemoteFilePath: fm.Filename, 
			RemoteAddr: clientAddr,
		}
		var reply string
		initialCopyErr := client.Call("FileService.CopyFileToRemote", fromClientArgs, &reply)
		client.Close()

		if initialCopyErr != nil {
			log.Fatal("Error copying from client", err)
		}
	}

	// copy the file to each servant
	// TODO: change these to async calls
	for _, servant := range fm.Servants {
		servantErr := util.CopyFileToRemote(fm.Filename, fm.Filename, servant, fm.SshConfig)
		if servantErr != nil {
			log.Fatal("Error sending file to servant", servantErr)
		}
	}

	fm.CurrentWrite -= 1
	fm.CheckQueue()
	return nil
}