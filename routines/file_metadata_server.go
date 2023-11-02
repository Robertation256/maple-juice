package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"log"
	"net"
	"net/rpc"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	REPORT_COLLECTION_TIMEOUT_SECONDS = 2
	REPORT_COLLECTION_PERIOD_MILLIS = 2000 
)



type FileMetadataService struct {
	metadataLock sync.Mutex
	nodeIdToMetadata map[string]*util.FileMetadataEntry
	fileNameToStatus map[string]*[]util.FileStatus
}

func NewFileMetadataService() *FileMetadataService {
	server := FileMetadataService{
		nodeIdToMetadata: make(map[string]*util.FileMetadataEntry),
		fileNameToStatus: make(map[string]*[]util.FileStatus),
	}
	return &server;
}


// file index & file master/servant info server, hosted by leader
func (rpcServer *FileMetadataService)StartMetadataServer(){

	rpc.Register(rpcServer)
	rpc.HandleHTTP()
	addr := ":"+strconv.Itoa(config.FileMetadataServerPort)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Failed to start file metadata server", err)
	}

	rpcServer.adjustCluster(collectMetadata())

	go func() {
		// todo: low-priority but needs graceful termination
		for {
			timer := time.After(time.Duration(REPORT_COLLECTION_PERIOD_MILLIS) * time.Millisecond)
			select {
			case <-timer:
				rpcServer.adjustCluster(collectMetadata())
			}
		}
	}()

	go http.Serve(l, nil)
}


func collectMetadata() *[]util.FileServerMetadataReport{
	
	ips := LocalMembershipList.AliveMembers()
	clients := make([]*rpc.Client, len(ips))
	reports := make([]util.FileServerMetadataReport, len(ips))

	collectionTimeout := time.After(time.Duration(REPORT_COLLECTION_TIMEOUT_SECONDS) * time.Second)

	calls := make([]*rpc.Call, len(ips))

	// todo: modularized batch rpc calls
	for index, ip := range ips {
		// start connection if it is not previously established
		if clients[index] == nil {
			clients[index] = dial(ip, config.FileServerPort)
		}
		
		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("FileServer.ReportMetadata", new(Args), &(reports[index]), nil)
			if call.Error != nil {		
				clients[index] = dial(ip, config.FileServerPort) // best effort re-dial
				if clients[index] != nil {
					call = clients[index].Go("FileServer.ReportMetadata", new(Args), &(reports[index]), nil)
				}
			}
			calls[index] = call
		}
	}

	// iterate and look for completed rpc calls
	for { 
		complete := true
		for i, call := range calls {
			select{
			case <-collectionTimeout:
				complete = true
				break
			default:
				if call != nil {
					select {
					case _, ok := <-call.Done:	// check if channel has output ready
						if !ok {
							log.Println("Channel closed for async rpc call")
						}
						calls[i] = nil
					default:
						complete = false
					}
				}
			}
		}
		if complete {
			break
		}
	}
	return &reports
}

// reallocate replicas as necessary
func (rpcServer *FileMetadataService)adjustCluster(reports *[]util.FileServerMetadataReport){

}



