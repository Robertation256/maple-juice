package util

import (
	"errors"
	"log"
)

const (
	MIN_REPLICA_NUM int = 4
)

// metadata reported by each file server and collected by the metadata server
type FileServerMetadataReport struct {
	NodeId      string
	FileEntries []FileInfo
}

type FileInfo struct {
	NodeId   string
	FileName string
	IsMaster bool
	InRepair bool // in the process of replicating from another replica
	Version  int
}

// replica cluster info for a file
type ClusterInfo struct {
	FileName string
	Master   *FileInfo
	Servants []*FileInfo
}

func NewReplicaInfo(fileName string) *ClusterInfo {
	ret := ClusterInfo{
		FileName: fileName,
		Servants: make([]*FileInfo, 0),
	}
	return &ret
}



// return type for DFS client metadata query
type FileDistributionInfo struct {
	FileName string
	Exists bool			// false if a file cannot be found
	Master   FileInfo
	Servants []FileInfo
}

// compile reports into map of nodeId -> fileName -> FileInfo and a map of fileName -> replicaInfo
func CompileReports(reports *[]FileServerMetadataReport) (*map[string]map[string]*FileInfo, *map[string]*ClusterInfo) {
	nodeIdToFiles := make(map[string]map[string]*FileInfo)
	fileNameToCluster := make(map[string]*ClusterInfo)

	for _, report := range *reports {
		nodeId := report.NodeId
		_, ok := nodeIdToFiles[nodeId]

		if !ok {
			nodeIdToFiles[nodeId] = make(map[string]*FileInfo)
		}

		for _, fileInfo := range report.FileEntries {
			fileName := fileInfo.FileName
			nodeIdToFiles[nodeId][fileName] = &fileInfo

			_, ok = fileNameToCluster[fileName]
			if !ok {
				fileNameToCluster[fileName] = NewReplicaInfo(fileName)
			}
			entry := fileNameToCluster[fileName]
			if fileInfo.IsMaster {
				if fileNameToCluster[fileName].Master != nil {
					log.Printf("Detected multiple masters for file %s", fileName)
				}
				entry.Master = &fileInfo
			} else {
				servants := entry.Servants
				servants = append(servants, &fileInfo)
				entry.Servants = servants
			}
			fileNameToCluster[fileName] = entry
		}
	}

	return &nodeIdToFiles, &fileNameToCluster
}

// try to choose servant with the largest version id as next master
// remove it from servant list
func (this *ClusterInfo) InstateNewMaster() error {
	var newMaster *FileInfo
	ti := 0
	for idx, servant := range this.Servants {
		if servant.InRepair {
			continue
		}

		if newMaster == nil || (*newMaster).Version < (*servant).Version {
			newMaster = servant
			ti = idx
		}
	}

	// found a new master, remove it from servants
	if newMaster != nil {
		servants := this.Servants
		this.Servants = append(servants[:ti], servants[ti+1:]...)
		return nil
	}

	// this only happens when master dies and no servants have completed replication
	return errors.New("Fatal: untable to establish new master for file " + this.FileName)
}


// pull in new servants to reach replication factor
func (cluster *ClusterInfo) RecruitServants(nodeIdToFiles *map[string]map[string]*FileInfo, replicationFactor int) {
	recruitNum := replicationFactor - cluster.ClusterSize()
	if recruitNum <= 0{
		return
	}

	for nodeId, fmap := range *nodeIdToFiles {
		_, exists := fmap[cluster.FileName]
		if !exists {
			newFileInfo := FileInfo{
				NodeId: nodeId,
				FileName: cluster.FileName,
				IsMaster: false,
				InRepair: true, 
				Version: 0,

			}
			cluster.Servants = append(cluster.Servants, &newFileInfo)
			if cluster.ClusterSize() >= replicationFactor{
				break
			}
		}
	}

	if cluster.ClusterSize() >= replicationFactor {
		log.Printf("[Warn] unable to fulfill replication factor for file %s, current cluster size is %d", cluster.FileName, cluster.ClusterSize())
	}
}


func (this *ClusterInfo) ClusterSize() int {
	size := 0
	if this.Master != nil {
		size += 1
	}
	return size + len(this.Servants)
}


func (this *ClusterInfo) Flatten() *[]*FileInfo {
	ret := make([]*FileInfo, 0)
	if this.Master != nil {
		ret = append(ret, this.Master)
	}
	ret = append(ret, this.Servants...)
	return &ret
}



func Convert(fileToCluster *map[string]*ClusterInfo) *map[string]map[string]*FileInfo{
	ret := make(map[string]map[string]*FileInfo)

	for fileName, cluster := range *fileToCluster{
		for _, fileInfo := range *cluster.Flatten(){
			nodeId := fileInfo.NodeId
			_, exists := ret[nodeId]
			if !exists {
				ret[nodeId] = make(map[string]*FileInfo)
			}
			ret[nodeId][fileName] = fileInfo
		}
	}

	return &ret
}
