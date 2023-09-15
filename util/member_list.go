package util

import (
	"sync"
	"time"
	"bytes"
	"binary"
)

const (
	G	uint8 = 1	// gossip protocol
	GS	uint8 = 2	// gossip + suspicion protocol

	NORMAL uint8 = 0
	SUS uint8 = 1
	FAILED uint8 = 2
	LEFT uint8 = 3

	PERIOD_MILLI int64 = 500	//todo: revisit these two values
	TIMEOUT_MILLI int64 = 1000 
)


type MemberListEntry struct {
	ip [4]uint8
	port uint16 
	startUpTs int64  // we can use less bytes for this if we want tho

	seqNum uint32
	status uint8	

	expirationTs int64	// hearbeat must be received before this ts
}

func (this *MemberListEntry) isFailed() bool {
	return this.status == FAILED || time.Now().UnixMilli() >= this.expirationTs
}

func (this *MemberListEntry) resetTimer(){
	this.expirationTs = time.Now().UnixMilli() + TIMEOUT_MILLI
}

// simple in-place merge. not thread-safe
func (this *MemberListEntry) Merge(remote MemberListEntry, protocol uint8) *MemberListEntry {
	if remote.status > this.status {	// LEFT dominates FAILED dominates SUS dominiates NORMAL
		// ignore if SUS received under G
		if protocol == G && remote.status == SUS {
			return this 
		}
		
		this.status = remote.status 	
		if remote.status == SUS {
			this.resetTimer()
		}
		reportStatusUpdate(*this)
	} else if (
		remote.seqNum > this.seqNum && 
		// normal seq num inc
		((remote.status == NORMAL && this.status == NORMAL ) || 
		// a node revives
		(protocol == GS && remote.status == NORMAL &&
			this.status == SUS))
	) {	
	   this.status = NORMAL 
	   this.seqNum = remote.seqNum
	   this.resetTimer
    }
	
	return this 
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

// serialiation
func (this *MemberList) WriteToBuffer(buf bytes.Buffer){
	this.lock.Lock() 
	buf.write(protocol)

	var uint16Arr [2]byte 
	var uint32Arr [4]byte
	var uint64Arr [8]byte

	binary.LittleEndian.PutUint32(uint32Arr, this.protocolVersion)
	buf.write(uint32Arr)

	ptr :=  this.entries
	for ptr != nil {
		entry := ptr.value 
		buf.write(entry.ip)

		binary.LittleEndian.PutUint16(uint16Arr, entry.port)
		buf.write(uint16Arr)

		binary.LittleEndian.PutUint64(uint64Arr, entry.startUpTs)
		buf.write(uint64Arr)



	}

}

// merge two membership lists sorted by node id
func (this *MemberList) Merge(other MemberList){
	this.lock.Lock()

	if other.protocolVersion > this.protocolVersion {
		if other.protocol == G && this.protocol == GS {
			this.cleanUpSusEntries()
		}
		this.protocol = other.protocol
		this.protocolVersion = other.protocolVersion
	}

	head := new(EntryNode) // dummy linked-list head
	curr := head 
	localEntry := this.entries
	remoteEntry := other.entries 

	for localEntry != nil && remoteEntry != nil {
		cmp := EntryCmp(localEntry.value, remoteEntry.value)
		if cmp < 0 {
			curr.next = localEntry
			localEntry = localEntry.next 
		} else if cmp > 0 { // new entry from remote
			curr.next = remoteEntry
			reportStatusUpdate(remoteEntry.value)
			remoteEntry = remoteEntry.next
		} else {
			curr.next = localEntry.value.Merge(remoteEntry.value, this.protocol)
			localEntry = localEntry.next 
			remoteEntry = remoteEntry.next
		}
		curr = curr.next 
	}

	if remoteEntry != nil {	// more new entries
		curr.next = remoteEntry
		for remoteEntry != nil {
			reportStatusUpdate(remoteEntry.value)
			remoteEntry = remoteEntry.next
		}
	}

	if localEntry != nil {
		curr.next = localEntry
	}
	this.lock.Unlock()
}

func (this *MemberList) cleanUpSusEntries(){
	// todo: expire all sus entries when switching from GS to G
}

func reportStatusUpdate(e MemberListEntry){
	// todo: if NORMAL report new join, 
	// report the status as is for the rest
}