package util

import (
	"log"
	"slices"
)

const (
	MIN_REPLICA_NUM int = 4
)


// entries maintained at the metadata server
type FileMetadataEntry struct {
	FileName string
	Master string		// nodeId
	Servants  []string  // nodeIds
}



func(this *FileMetadataEntry) AddServers(newNodes []string){
	if !this.NeedRepair(){
		log.Print("File cluster already satisfies legal size.")
	}

	if this.ReplicaNum() == 0{
		log.Fatal("Ghost file index entry")
	}

	if !this.HasMaster(){
		// pick the smallest nodeId as new master 
		this.Master = this.Servants[0]
		this.Servants = this.Servants[1:]
	}

	
	for _, newNode := range newNodes {
		// todo: issue replications to new nodes

		this.Servants = append(this.Servants, newNode)
		slices.Sort(this.Servants)
	}
	


}

// replica num is lower than legal size
func(this *FileMetadataEntry) NeedRepair() bool {
	return this.ReplicaNum() < MIN_REPLICA_NUM
}

func(this *FileMetadataEntry) IsStable() bool {
	return this.ReplicaNum() >= MIN_REPLICA_NUM
}

func(this *FileMetadataEntry) ReplicaNum() int {
	num := 0
	if len(this.Master) >  0 {
		num += 1
	}
	num += len(this.Servants)
	return num
}

func(this *FileMetadataEntry) HasMaster() bool {
	return len(this.Master) >  0
}