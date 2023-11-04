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
	// main queue: store the requests that are waiting in the order they are received
	Queue        []*Request
	// write queue: keep track of write requests that are waiting
	WriteQueue   []*Request
	// number of nodes that are currently reading the file
	CurrentRead  int
	// number of nodes that are currently writing the file
	CurrentWrite int
	Filename     string
	// list of servant ip addresses
	Servants     []string
	// address of self. used to prevent errors in case fm = client
	SelfAddr 	 string
	// TODO: remove this and integrate filemaster into file server
	SshConfig 	 *ssh.ClientConfig
}

type Request struct {
	// type of request: read (R) or write (W)
	Type      string
	// a flag indicating whether the request is still in queue
	InQueue   bool
	// how many rounds a write has been waiting for consecutive read
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
			// if head of queue has InQueue = false. pop it and move on to the next one
			fm.Queue = fm.Queue[1:]
			fm.CheckQueue()
			return
		}
		if fm.Queue[0].Type == "R" {
			// satisfy read condition (current reader < 2 and no writer)
			if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
				// no write has been waiting for more than 4 consecutive read
				if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
					fm.Queue[0].InQueue = false
					fm.Queue = fm.Queue[1:]
					return
				} else {
					// if a write has been waiting for 4 consecutive read, pop that write request
					// note now queue might have a request that have InQueue = false
					fm.WriteQueue[0].InQueue = false
					fm.WriteQueue = fm.WriteQueue[1:]
				}
			}
		} else {
			// write request
			if fm.CurrentRead == 0 && fm.CurrentWrite == 0 {
				// write condition satisifed: no reader and no writer (write-write and read-write are both conflict operations)
				fm.WriteQueue[0].InQueue = false
				fm.WriteQueue = fm.WriteQueue[1:]

				fm.Queue[0].InQueue = false
				fm.Queue = fm.Queue[1:]
			} else if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
				// write is blocked because there is exactly 1 reader
				// then allow another read request, if write has not been waiting for more than 4 consecutive rounds
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
		// if the request is not in queue and read condition (reader < 2 and no writer) satisfied
		if request == nil && fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
			// no write has been waiting for more than 4 consecutive read
			if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
				return fm.executeRead(clientAddr)
			}
		} else if request == nil {
			// initial condition to execute the read is not satifised. add to queue
			request = &Request{
				Type:    "R",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
		} else if request != nil && !request.InQueue {
			// request has been poped from queue, execute read
			return fm.executeRead(clientAddr)
		}
	}
}

func (fm *FileMaster) executeRead(clientAddr string) error {
	fm.CurrentRead += 1
	// every request in the wait queue has been forced to wait another one round because of 
	// the read that is currently executing
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
		// requests just come in, and the condition for write is satisfied
		if request == nil && fm.CurrentWrite == 0 && fm.CurrentRead == 0 {
			// if there is no other write pending, simply execute
			if len(fm.WriteQueue) == 0 {
				return fm.executeWrite(clientAddr)
			} 
		} else if request == nil {
			// otherwise add to queue
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