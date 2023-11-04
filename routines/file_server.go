package routines

import (
	"net/http"
	"net/rpc"
	"net"
	"golang.org/x/crypto/ssh"
	"cs425-mp2/config"
	"cs425-mp2/util"
	"fmt"
	"log"
)

type FileService struct {
	Port int
	SshConfig *ssh.ClientConfig
	Filename2FileMaster map[string]*FileMaster
}

type CopyArgs struct {
	LocalFilePath string
	RemoteFilePath string
	RemoteAddr string
}

type RWArgs struct {
	Filename string
	ClientAddr string
}

type CreateFMArgs struct {
	Filename string
	Servants []string
}

type DeleteArgs struct {
	LocalFilePath string
}

func NewFileService(config *config.Config, port int) *FileService {
	this := new(FileService)
	this.Port = port
	this.SshConfig = &ssh.ClientConfig{
		User: config.SshUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.SshPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	this.Filename2FileMaster = make(map[string]*FileMaster)
	return this
}

func (this *FileService) Start(){
	// TODO: integrate this with the grep server
	rpc.Register(this)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", this.Port))
	if err != nil {
		log.Fatal("Failed to start file server", err)
	}

	go http.Serve(l, nil)
}

func (this *FileService) ReadFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.Filename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.ReadFile(args.ClientAddr)
	} else {
		log.Fatal("No corresponding filemaster for " + args.Filename)
	}
	return nil
}

func (this *FileService) CopyFileToRemote(args *CopyArgs, reply *string) error {
	return util.CopyFileToRemote(args.LocalFilePath, args.RemoteFilePath, args.RemoteAddr, this.SshConfig)
}

func (this *FileService) DeleteFile(args *DeleteArgs, reply *string) error {
	return util.DeleteFile(args.LocalFilePath)
}

func (this *FileService) CreateFileMaster(args *CreateFMArgs, reply *string) error{
	fm := NewFileMaster(args.Filename, args.Servants, this.SshConfig)
	this.Filename2FileMaster[args.Filename] = fm
	fmt.Println("here")
	return nil
}