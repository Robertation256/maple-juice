package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"errors"
	"log"
	"net/rpc"
	"sync"
	"time"
)

const (
	REPORT_COLLECTION_TIMEOUT_SECONDS = 2
	RECONCILIATION_PERIOD_MILLIS      = 2000 // period for a cycle of collect, repair and inform file metadata
)

var FileMetadataServerSigTerm chan int = make(chan int)

type FileMetadataService struct {
	metadataLock sync.RWMutex
	// fileName -> replica distribution info
	metadata map[string]*util.ClusterInfo
}

func NewFileMetadataService() *FileMetadataService {
	server := FileMetadataService{
		metadata: make(map[string]*util.ClusterInfo),
	}
	return &server
}

// file index & file master/servant info server, hosted by leader
func (this *FileMetadataService) Register() {
	rpc.Register(this)

	// todo: low-priority but needs graceful termination

	for {
		timer := time.After(time.Duration(RECONCILIATION_PERIOD_MILLIS) * time.Millisecond)
		select {
		case <-timer:
			if LeaderId == SelfNodeId {
				this.adjustCluster(collectMetadata())
			}
		}
	}

}

// --------------------------------------------
// Functions for handling DFS client requests
// --------------------------------------------

// query replica distribution about a file, for DFS client
func (this *FileMetadataService) HandleDfsClientRequest(request *DfsRequest, reply *DfsResponse) error {
	requestType := request.RequestType
	fileName := request.FileName

	// Might happen when a re-election happens right after client sending the request
	if LeaderId != SelfNodeId {
		return errors.New("Metadata service unavailable. This host is not a leader.")
	}

	if len(fileName) == 0 {
		return errors.New("File name is empty")
	}

	switch requestType {
	case FILE_GET:
		return this.handleGetRequest(fileName, reply)
	case FILE_PUT:
		return this.handlePutRequest(fileName, reply)
	case FILE_DELETE:
		return this.handleDeleteRequest(fileName) // no data replied, no error means success
	case FILE_LIST:
		return this.handleListRequest(fileName, reply)
	}

	return errors.New("Unsupported request type")
}

// clients tries to fetch distribution info of a file
func (this *FileMetadataService) handleGetRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	clusterInfo, exists := this.metadata[fileName]
	this.metadataLock.RUnlock()

	if !exists {
		return errors.New("File " + fileName + "does not exist")
	}
	reply = toResponse(clusterInfo)
	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handlePutRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.Lock()
	defer this.metadataLock.Unlock()

	clusterInfo, exists := this.metadata[fileName]
	var targetCluster *util.ClusterInfo

	if !exists {
		// new file, allocate a new cluster
		targetCluster = util.NewClusterInfo(fileName)

		targetCluster.RecruitFullCluster(&this.metadata, config.ReplicationFactor)

		// write back to metdata and notify invovlved nodes
		this.metadata[fileName] = targetCluster
		var err error
		for _, node := range *targetCluster.Flatten() {
			err = informMetadata(node.NodeId, &this.metadata)
		}
		return err
	}

	// return distribution info if found, client will contact file master if it is alive
	reply = toResponse(clusterInfo)
	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handleDeleteRequest(fileName string) error {
	this.metadataLock.Lock()
	defer this.metadataLock.Unlock()

	clusterInfo, exists := this.metadata[fileName]

	if !exists {
		// new file, allocate a new cluster
		return errors.New("File " + fileName + " does not exists")
	}

	delete(this.metadata, fileName)

	var err error
	for _, node := range *clusterInfo.Flatten() {
		err = informMetadata(node.NodeId, &this.metadata)
	}
	return err

}

// clients tries to write a file
func (this *FileMetadataService) handleListRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	defer this.metadataLock.RUnlock()

	clusterInfo, exists := this.metadata[fileName]

	if !exists {
		// new file, allocate a new cluster
		return errors.New("File " + fileName + " does not exists")
	}

	reply = toResponse(clusterInfo)
	return nil
}

// ----------------------------------------
// Functions for maintaining metadata
// ----------------------------------------

// for each file, examine the hosting replicas and make necessary repairs
func checkAndRepair(nodeIdToFiles *map[string]map[string]*util.FileInfo, fileNameToReplicaInfo *map[string]*util.ClusterInfo) {
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
			clients[index] = dial(ip, config.RpcServerPort)
		}

		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("FileServer.ReportMetadata", new(Args), &(reports[index]), nil)
			if call.Error != nil {
				clients[index] = dial(ip, config.RpcServerPort) // best effort re-dial
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
	nodeIdToFiles, filenameToCluster := util.CompileReports(reports)
	checkAndRepair(nodeIdToFiles, filenameToCluster)

	rpcServer.metadataLock.Lock()
	rpcServer.metadata = *filenameToCluster
	rpcServer.metadataLock.Unlock()

	nodeIdToFiles = util.Convert(filenameToCluster)
	for nodeId, _ := range *nodeIdToFiles {
		go informMetadata(nodeId, filenameToCluster)
	}
}

func informMetadata(nodeId string, metadata *util.Metadata) error {
	timeout := time.After(5 * time.Second)
	ip := NodeIdToIP(nodeId)
	client := dial(ip, config.RpcServerPort)
	defer client.Close()

	retFlag := ""

	call := client.Go("FileServer.UpdateMetadata", &metadata, &retFlag, nil)
	if call.Error != nil {
		log.Printf("Encountered error while informing node %s", nodeId)
		return call.Error
	}

	select {
	case <-timeout:
		return errors.New("Timeout informing node" + nodeId)
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok {
			log.Println("File Metadata Server: Channel closed for async rpc call")
			return errors.New("Node " + nodeId + " failed to respond to metadata update.")
		} else {
			if retFlag == "ACK" {
				log.Printf("File Metadata Server: successfully informed node %s", nodeId)
				return nil
			} else {
				log.Printf("File Metadata Server: node %s failed to process metadata update", nodeId)
				return errors.New("Node " + nodeId + " failed to process metadata update.")
			}
		}
	}
}

func toResponse(clusterInfo *util.ClusterInfo) *DfsResponse {

	servants := make([]util.FileInfo, len(clusterInfo.Servants))
	for _, servant := range clusterInfo.Servants {
		servants = append(servants, *servant)
	}

	ret := &DfsResponse{
		FileName: clusterInfo.FileName,
		Master:   *clusterInfo.Master,
		Servants: servants,
	}

	return ret
}