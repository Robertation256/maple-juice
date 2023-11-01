package util

import (
	"log"
	"sync"
	"slices"
)

const (
	MIN_REPLICA_NUM int = 4
)


// read write lock
var fileIndexLock sync.Mutex
var FileIndex = make(map[string]FileIndexEntry)

type FileIndexEntry struct {
	FileName string
	Master string		// nodeId
	Servants  []string  // nodeIds
}



func(this *FileIndexEntry) AddServers(newNodes []string){
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
func(this *FileIndexEntry) NeedRepair() bool {
	return this.ReplicaNum() < MIN_REPLICA_NUM
}

func(this *FileIndexEntry) IsStable() bool {
	return this.ReplicaNum() >= MIN_REPLICA_NUM
}

func(this *FileIndexEntry) ReplicaNum() int {
	num := 0
	if len(this.Master) >  0 {
		num += 1
	}
	num += len(this.Servants)
	return num
}

func(this *FileIndexEntry) HasMaster() bool {
	return len(this.Master) >  0
}