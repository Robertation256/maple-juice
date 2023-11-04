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
	Port 					int
	SshConfig 				*ssh.ClientConfig
	Filename2FileMaster 	map[string]*FileMaster
	SdfsFolder 				string
}

type CopyArgs struct {
	LocalFilename 	string
	RemoteFilename 	string
	RemoteAddr 		string
}

type RWArgs struct {
	Filename 	string
	ClientAddr 	string
}

type CreateFMArgs struct {
	Filename 	string
	Servants 	[]string
}

type DeleteArgs struct {
	Filename string
}

func NewFileService(config *config.Config, port int, homedir string) *FileService {
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
	this.SdfsFolder = homedir + "/sdfs/"
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

// reroute to the corresponding file master
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

// reroute to the corresponding file master
func (this *FileService) WriteFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.Filename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.WriteFile(args.ClientAddr)
	} else {
		log.Fatal("No corresponding filemaster for " + args.Filename)
	}
	return nil
}

func (this *FileService) CopyFileToRemote(args *CopyArgs, reply *string) error {
	return util.CopyFileToRemote(args.LocalFilename, args.RemoteFilename, args.RemoteAddr, this.SshConfig, this.SdfsFolder)
}

// TODO: delete all replicas
func (this *FileService) DeleteFile(args *DeleteArgs, reply *string) error {
	return util.DeleteFile(args.Filename, this.SdfsFolder)
}

func (this *FileService) CreateFileMaster(args *CreateFMArgs, reply *string) error{
	fm := NewFileMaster(args.Filename, args.Servants, this.SshConfig, this.Port, this.SdfsFolder)
	this.Filename2FileMaster[args.Filename] = fm
	return nil
}