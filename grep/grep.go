package grep

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
)

type GrepService struct {
	logFileDir   string
	logFileNames []string
}

func NewGrepService(logFileDir string, localPort string) *GrepService {
	filePaths, err := os.ReadDir(logFileDir)
	if err != nil {
		log.Fatal("Error reading log file directory", err)
	}

	filesNames := make([]string, len(filePaths))

	for i := 0; i < len(filePaths); i++ {
		filesNames[i] = filePaths[i].Name()
	}

	this := new(GrepService)
	this.logFileDir = logFileDir
	hardCodedName := fmt.Sprintf("%s.txt", localPort)
	this.logFileNames = []string{hardCodedName}
	return this
}

func (this *GrepService) GrepLocal(args *Args, reply *string) error {
	pattern := args.Input
	*reply = ""

	for _, fileName := range this.logFileNames {
		// todo: remove cmd /K for linux
		cmd := exec.Command("grep", "-c", pattern, this.logFileDir+"/"+fileName)
		output, err := cmd.CombinedOutput()
		// exit code 1 means a match was not found
		if err != nil && cmd.ProcessState.ExitCode() != 1 {
			log.Println("Error while executing grep", err)
			return err
		}
		*reply += fmt.Sprintf("%s:%s", fileName, output)
	}
	return nil
}

type Args struct {
	Input string
}

func LoadIps() []string {
	var s []byte
	var err error
	s, err = os.ReadFile("../config.txt")
	if err != nil {
		s, err = os.ReadFile("./config.txt")
		if err != nil {
			log.Fatal("Error reading remote server config file", err)
		}
	}

	ips := strings.Split(string(s), ",")

	if len(ips) == 0 {
		log.Fatal("Remote server ip config is empty")
	}

	return ips
}

func CloseClients(clients []*rpc.Client) {
	for _, c := range clients {
		if c != nil {
			c.Close()
		}
	}
}
