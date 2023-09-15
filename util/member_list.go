package util

import (
	"sync"
	// "time"
)

const (
	G	uint8 = 1	// gossip protocol
	GS	uint8 = 2	// gossip + suspicion protocol

	NORMAL uint8 = 0
	SUS uint8 = 1
	FAILED uint8 = 2
	LEFT uint8 = 3

	PERIOD_MILLI int64 = 500
)


type MemberListEntry struct {
	ip [4]int8
	port uint16 
	startUpTs int64  // we can use less bytes for this if we want tho

	seqNum uint32
	status uint8	

	expirationTs int64	// hearbeat must be received before this ts
}


// simple in-place merge. not thread-safe
func (this *MemberListEntry) Merge(remote MemberListEntry, protocol int){
	if remote.status > this.status {
		this.status = remote.status 	// LEFT dominates FAILED dominates SUSPICIOUS
		// todo: report status
	} else {
		
	}
}

// compare entry node id
func EntryCmp(e1 MemberListEntry, e2 MemberListEntry) int {
	for i:=0; i<4; i++ {
		ipCmp := e1.ip[i] - e2.ip[1]
		if ipCmp != 0 {
			return int(ipCmp) 
		}
	}
	portCmp := e1.port - e2.port  
	if portCmp != 0 {
		return int(portCmp)
	}
	tsCmp := e1.startUpTs - e2.startUpTs
	if tsCmp > 0 {
		return 1
	} else if tsCmp < 0{
		return -1
	} 
	return 0
}

// linked list node
type EntryNode struct {
	value MemberListEntry
	next *MemberListEntry
}


type MemberList struct {
	protocol   uint8
	protocolVersion uint		// used for syncing protocol used across machines
	entries *EntryNode
	lock sync.Mutex 			// write lock
}

// inplace merge
func (this *MemberList) Merge(other MemberList){
	this.lock.Lock()

	if other.protocolVersion > this.protocolVersion {
		this.protocol = other.protocol
		this.protocolVersion = other.protocolVersion
	}
	head := new(EntryNode)
	curr := head 
	localEntry := this.entries
	remoteEntry := other.entries 

	for localEntry != nil && remoteEntry != nil {
		cmp := EntryCmp(localEntry.value, remoteEntry.value)
		if cmp < 0 {
			curr.next = localEntry
		} else if cmp > 0 {
			curr.next = remoteEntry
			status := remoteEntry.value.status 
			if  status != SUS || this.protocol == GS {
				// log new join/failure/left/sus
			}
		} else {

		}
	}






	this.lock.Unlock()
}