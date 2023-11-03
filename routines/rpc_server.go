package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"time"
)


type RpcServer struct {
	// register any services using grpc
	grepService *GrepService
	fileMetadataService *FileMetadataService
}


func StartRpcServer(){

	server := RpcServer{
		grepService: NewGrepService(),
		fileMetadataService: NewFileMetadataService(),
	}

	rpc.Register(server)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", config.RpcServerPort))
	if err != nil {
		log.Fatal("Failed to start rpc server", err)
	}

	go http.Serve(l, nil)

	LOG_SERVER_STARTED.Done()
	SIGTERM.Wait()

}