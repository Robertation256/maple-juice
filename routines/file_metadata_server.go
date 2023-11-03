package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const (
	REPORT_COLLECTION_TIMEOUT_SECONDS = 2
	RECONCILIATION_PERIOD_MILLIS   = 2000		// period for a cycle of collect, repair and inform file metadata
)

var FileMetadataServerSigTerm chan int = make(chan int)

type FileMetadataService struct {
	metadataLock       sync.RWMutex
	// fileName -> replica distribution info
	metadata           map[string]*util.ClusterInfo
}

func NewFileMetadataService() *FileMetadataService {
	server := FileMetadataService{
	metadata: make(map[string]*util.ClusterInfo),
	}
	return &server
}

// file index & file master/servant info server, hosted by leader
func (rpcServer *FileMetadataService) StartMetadataServer() {

	rpc.Register(rpcServer)
	rpc.HandleHTTP()
	addr := ":" + strconv.Itoa(config.FileMetadataServerPort)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("Failed to start file metadata server", err)
	}

	// rpcServer.adjustCluster(collectMetadata())

	// go func() {
	// 	// todo: low-priority but needs graceful termination
	// 	for {
	// 		timer := time.After(time.Duration(RECONCILIATION_PERIOD_MILLIS) * time.Millisecond)
	// 		select {
	// 		case <-timer:
	// 			rpcServer.adjustCluster(collectMetadata())
	// 		}
	// 	}
	// }()

	go http.Serve(l, nil)
	log.Print("File metadata server started")

	FILE_METADATA_SERVER_SIGTERM.Wait()
}

// query replica distribution about a file, for DFS client
func (rpcServer *FileMetadataService) GetFileClusterInfo(fileName string) util.FileDistributionInfo{
	rpcServer.metadataLock.RLock()
	clusterInfo := rpcServer.metadata[fileName]
	rpcServer.metadataLock.RUnlock()

	if clusterInfo == nil {
		return util.FileDistributionInfo{
			Exists: false,
		}
	}
	
	servants := make([]util.FileInfo, len(clusterInfo.Servants))
	for _, servant := range clusterInfo.Servants{
		servants = append(servants, *servant)
	}
	
	return util.FileDistributionInfo{
		FileName: clusterInfo.FileName,
		Exists: true,
		Master: *clusterInfo.Master,
		Servants: servants,
	}
} 



// for each file, examine the hosting replicas and make necessary repairs
func checkAndRepair(nodeIdToFiles *map[string]map[string]*util.FileInfo, fileNameToReplicaInfo *map[string]*util.ClusterInfo){
	for _, clusterInfo := range *fileNameToReplicaInfo {
		if clusterInfo.Master == nil {
			// Master dead, try elect from servants
			err := clusterInfo.InstateNewMaster()
			if err != nil {
				// todo: consider removing dead file from metadata, all replicas are lost and nothing much can be done

				return 
			}
		}

		if clusterInfo.ClusterSize() < config.ReplicationFactor {
			clusterInfo.RecruitServants(nodeIdToFiles, config.ReplicationFactor)
		}
	}
}



func collectMetadata() *[]util.FileServerMetadataReport {

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
			select {
			case <-collectionTimeout:
				complete = true
				break
			default:
				if call != nil {
					select {
					case _, ok := <-call.Done: // check if channel has output ready
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
func (rpcServer *FileMetadataService) adjustCluster(reports *[]util.FileServerMetadataReport) {
	nodeIdToFiles, filenameToCluster :=  util.CompileReports(reports)
	checkAndRepair(nodeIdToFiles, filenameToCluster)

	rpcServer.metadataLock.Lock()
	rpcServer.metadata = *filenameToCluster
	rpcServer.metadataLock.Unlock()

	nodeIdToFiles = util.Convert(filenameToCluster)
	for nodeId, fileMap := range *nodeIdToFiles {
		go informMetadata(nodeId, fileMap)
	}
}



func informMetadata(nodeId string, fileMap map[string]*util.FileInfo){
	timeout := time.After(5*time.Second)
	ip := NodeIdToAddr(nodeId)
	client := dial(ip, config.FileServerPort)
	defer client.Close()

	data := make([]util.FileInfo, len(fileMap))
	for _, file := range fileMap {
		data = append(data, *file)
	}

	retFlag := ""

	call := client.Go("FileServer.UpdateMetadata", &data, &retFlag, nil)
	if call.Error != nil {
		log.Printf("Encountered error while informing node %s", nodeId)
		return
	}

	select{
	case <-timeout:
		log.Printf("Timeout informing node %s", nodeId)
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok {
			log.Println("File Metadata Server: Channel closed for async rpc call")
		} else {
			if retFlag == "ACK" {
				log.Printf("File Metadata Server: successfully informed node %s", nodeId)
			} else {
				log.Printf("File Metadata Server: node %s failed to process metadata update", nodeId)
			}
		}
		
	}
}
