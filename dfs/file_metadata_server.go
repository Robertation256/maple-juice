package dfs

import (
	"errors"
	"fmt"
	"log"
	"maple-juice/config"
	"maple-juice/membership"
	"maple-juice/leaderelection"
	"maple-juice/util"
	"net/rpc"
	"regexp"
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
	// nodeId -> fileName -> file info
	metadata util.NodeToFiles
	tombstones map[string]bool 	// a set of filenames over which repair and surveliance are halted for the sake of deletion
	tombstoneLock sync.RWMutex 
}

func (this *FileMetadataService)ToString() string {
	this.metadataLock.RLock()
	val := this.metadata
	this.metadataLock.RUnlock()
	ret := ""

	for _, fmap := range val {
		for _, fileInfo := range fmap {
			ret += fileInfo.ToString()
		}
		
	}
	ret += "---------------"

	return ret
}

func NewFileMetadataService() *FileMetadataService {
	server := FileMetadataService{
		metadata: make(map[string]map[string]*util.FileInfo),
		tombstones: make(map[string]bool),
	}
	return &server
}

// file index & file master/servant info server, hosted by leader
func (this *FileMetadataService) Register() {
	rpc.Register(this)

	// todo: low-priority but needs graceful termination
	go func() {
		for {
			timer := time.After(time.Duration(RECONCILIATION_PERIOD_MILLIS) * time.Millisecond)
			select {
			case <-timer:
				if leaderelection.LeaderId == membership.SelfNodeId {
					this.adjustCluster(collectMetadata())
				}
			}
		}
	}()

}

// --------------------------------------------
// Functions for handling DFS client requests
// --------------------------------------------

// query replica distribution about a file, for DFS client
func (this *FileMetadataService) HandleDfsClientRequest(request *DfsRequest, reply *DfsResponse) error {
	requestType := request.RequestType
	fileName := request.FileName

	// Might happen when a re-election happens right after client sending the request
	if leaderelection.LeaderId != membership.SelfNodeId {
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
		return this.handleDeleteRequest(fileName, reply) // no data replied, no error means success
	case FILE_LIST:
		return this.handleListRequest(fileName, reply)
	}

	return errors.New("Unsupported request type")
}


func ingestReply(reply *DfsResponse, clusterInfo *util.ClusterInfo){
	responseVal := toResponse(clusterInfo)
	reply.FileName = responseVal.FileName
	reply.Master = responseVal.Master
	reply.Servants = responseVal.Servants
}

// clients tries to fetch distribution info of a file
func (this *FileMetadataService) handleGetRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	fileToClusterInfo := util.Convert2(&this.metadata)
	this.metadataLock.RUnlock()

	clusterInfo, exists := (*fileToClusterInfo)[fileName]

	if !exists {
		return errors.New("File " + fileName + "does not exist")
	}
	
	ingestReply(reply, clusterInfo)

	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handlePutRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.Lock()
	defer this.metadataLock.Unlock()

	fileToClusterInfo := util.Convert2(&this.metadata)

	targetCluster, exists := (*fileToClusterInfo)[fileName]

	if !exists {
		// new file, allocate a new cluster
		targetCluster = util.NewClusterInfo(fileName)

		targetCluster.RecruitFullCluster(&this.metadata, config.ReplicationFactor)

		// write back to metdata and notify invovlved nodes
		(*fileToClusterInfo)[fileName] = targetCluster
		converted := util.Convert(fileToClusterInfo)
		this.metadata = *converted
		var err error
		for _, node := range *targetCluster.Flatten() {
			err = informMetadata(node.NodeId, &this.metadata)
		}
		if err != nil {
			return err
		}
	}

	// return distribution info if found, client will contact file master if it is alive
	ingestReply(reply, targetCluster)
	// log.Printf("Sent put response: \n " + toResponse(targetCluster).toString())
	return nil
}

// clients tries to delete a file, check if it exists, if so return cluster info
func (this *FileMetadataService) handleDeleteRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	fileToClusterInfo := *util.Convert2(&this.metadata)
	this.metadataLock.RUnlock()


	clusterInfo, exists := fileToClusterInfo[fileName]

	if !exists {
		return errors.New("File " + fileName + " does not exist")
	}

	ingestReply(reply, clusterInfo)
	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handleListRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	defer this.metadataLock.RUnlock()

	fileToClusterInfo := *util.Convert2(&this.metadata)

	clusterInfo, exists := fileToClusterInfo[fileName]

	if !exists {
		// new file, allocate a new cluster
		return errors.New("File " + fileName + " does not exist")
	}

	ingestReply(reply, clusterInfo)
	return nil
}

// ----------------------------------------
// Functions for maintaining metadata
// ----------------------------------------

// for each file, examine the hosting replicas and make necessary repairs
func checkAndRepair(nodeIdToFiles *map[string]map[string]*util.FileInfo, fileNameToReplicaInfo *map[string]*util.ClusterInfo) {
	for _, clusterInfo := range *fileNameToReplicaInfo {
		// refrain from repairing a file cluster that is pending delete
		if clusterInfo.IsClusterPendingDelete(){
			continue
		}
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

	ips := membership.LocalMembershipList.AliveMembers()
	ips = append(ips, util.NodeIdToIP(membership.SelfNodeId))
	clients := make([]*rpc.Client, len(ips))
	reports := make([]util.FileServerMetadataReport, len(ips))

	collectionTimeout := time.After(time.Duration(REPORT_COLLECTION_TIMEOUT_SECONDS) * time.Second)

	calls := make([]*rpc.Call, len(ips))

	// todo: modularized batch rpc calls
	for index, ip := range ips {
		// start connection if it is not previously established
		if clients[index] == nil {
			clients[index] = util.Dial(ip, config.RpcServerPort)
		}

		arg := ""

		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("FileService.ReportMetadata", &arg, &(reports[index]), nil)
			if call.Error != nil {
				clients[index] = util.Dial(ip, config.RpcServerPort) // best effort re-dial
				if clients[index] != nil {
					call = clients[index].Go("FileService.ReportMetadata", &arg, &(reports[index]), nil)
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
				log.Print("Collection timeout !!!")
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
	rpcServer.tombstoneLock.RLock()
	tombstones := rpcServer.tombstones
	rpcServer.tombstoneLock.RUnlock()



	nodeIdToFiles, filenameToCluster := util.CompileReports(reports, &tombstones)

	checkAndRepair(nodeIdToFiles, filenameToCluster)

	rpcServer.metadataLock.Lock()
	rpcServer.metadata = *nodeIdToFiles
	rpcServer.metadataLock.Unlock()

	nodeIdToFiles = util.Convert(filenameToCluster)
	var remainingNodes sync.WaitGroup
	for nodeId := range *nodeIdToFiles {
		remainingNodes.Add(1)
		go func(nodeId string){
			informMetadata(nodeId, nodeIdToFiles)
			remainingNodes.Done()
		}(nodeId)
	}

	remainingNodes.Wait()
}

func informMetadata(nodeId string, metadata *util.NodeToFiles) error {
	timeout := time.After(60 * time.Second)
	ip := util.NodeIdToIP(nodeId)
	client := util.Dial(ip, config.RpcServerPort)
	if client == nil {
		log.Printf("Cannot connect to node %s while informing metadata", nodeId)
		return errors.New("Cannot connect to node")
	}
	defer client.Close()

	retFlag := ""

	call := client.Go("FileService.UpdateMetadata", metadata, &retFlag, nil)
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
				return nil
			} else {
				log.Printf("File Metadata Server: node %s failed to process metadata update", nodeId)
				return errors.New("Node " + nodeId + " failed to process metadata update.")
			}
		}
	}
}

func toResponse(clusterInfo *util.ClusterInfo) *DfsResponse {

	servants := make([]util.FileInfo, 0)
	for _, servant := range clusterInfo.Servants {
		if servant == nil {
			log.Printf("Warn: null servant ptr")
		} else {
			servants = append(servants, *servant)
		}
	}

	ret := &DfsResponse{
		FileName: clusterInfo.FileName,
		Servants: servants,
	}

	if clusterInfo.Master != nil {
		ret.Master = *clusterInfo.Master
	} else {
		log.Println("Warn: responded with null master")
	}

	return ret
}


// return SDFS file names matching a regex
func (this *FileMetadataService) HandleFileSearchRequest(regex *string, reply *[]string) error {
	r, err := regexp.Compile(*regex)
	if err != nil {
		return err
	}
	result := make([]string, 0)
	resultSet := make(map[string]bool)

	this.metadataLock.RLock()
	for _, fmap := range this.metadata {
		for fileName := range fmap {
			if r.MatchString(fileName) {
				resultSet[fileName] = true
			}
		}
	}
	this.metadataLock.RUnlock()

	for fileName := range resultSet{
		result = append(result, fileName)
	}

	*reply = result
	return nil
}

// remove a file from metadata service surveilance, most likely due to a pending deletion
func (this *FileMetadataService) RequestTombstone(fileName *string, reply *string) error {
	this.metadataLock.Lock()
	defer this.metadataLock.Unlock()

	fileMap := util.Convert2(&this.metadata) 
	

	_, exists := (*fileMap)[*fileName]
	if !exists {
		*reply = "FAILED"
		return errors.New(fmt.Sprintf("Requesting tombstone for non-existent file: %s", *fileName))
	}

	this.tombstoneLock.Lock()
	this.tombstones[*fileName] = true
	this.tombstoneLock.Unlock()

	delete(*fileMap, *fileName)
	prunedMetadata := util.Convert(fileMap)

	this.metadata = *prunedMetadata

	*reply = "ACK"
	return nil
}


func (this *FileMetadataService) ReleaseTombstone(fileName *string, reply *string) error {
	this.tombstoneLock.Lock()
	if this.tombstones[*fileName] {
		delete(this.tombstones, *fileName)
	}
	this.tombstoneLock.Unlock()
	*reply = "ACK"
	return nil
}
