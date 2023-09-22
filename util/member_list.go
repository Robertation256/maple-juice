package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	NA uint8 = 0 // happens for new joiner
	G  uint8 = 1 // gossip protocol
	GS uint8 = 2 // gossip + suspicion protocol

	NORMAL uint8 = 0
	SUS    uint8 = 1
	FAILED uint8 = 2
	LEFT   uint8 = 3

	PERIOD_MILLI  int64 = 500 //todo: revisit these two values
	TIMEOUT_MILLI int64 = 2500
	CLEANUP_MILLI int64 = 30000 // time to wait before removing failed/left entries

	MAX_ENTRY_NUM int = 100 // max amount of entries per UDP packet
	ENTRY_SIZE    int = 19
)

// read write lock
var memberListLock sync.Mutex

// linked list node
type EntryNode struct {
	Value *MemberListEntry
	Next  *EntryNode
}

type MemberList struct {
	Protocol        uint8
	ProtocolVersion uint32 // used for syncing protocol used across machines
	Entries         *EntryNode
	SelfEntry       *MemberListEntry
}

func NewMemberList(port uint16) *MemberList {
	selfEntry := &MemberListEntry{
		Ip:        getOutboundIp(),
		Port:      port,
		StartUpTs: time.Now().UnixMilli(),
		SeqNum:    0,
		Status:    NORMAL,
	}

	return &MemberList{
		Protocol:        0,
		ProtocolVersion: 0,
		Entries:         &EntryNode{Value: selfEntry},
		SelfEntry:       selfEntry,
	}
}

func (this *MemberList) IncSelfSeqNum() uint32 {
	memberListLock.Lock()
	this.SelfEntry.SeqNum += 1
	ret := this.SelfEntry.SeqNum
	memberListLock.Unlock()
	return ret
}

// insert a new entry, only used when introducer sees a new joiner
func (this *MemberList) AddNewEntry(entry *MemberListEntry) error {
	head := new(EntryNode)
	memberListLock.Lock()
	defer memberListLock.Unlock()

	head.Next = this.Entries
	curr := this.Entries
	prev := head
	for curr != nil {
		cmp := EntryCmp(entry, curr.Value)
		if cmp < 0 {
			break
		} else if cmp == 0 {
			return errors.New("Attempted to add a duplicate entry")
		}
		prev = curr
		curr = curr.Next
	}

	newNode := EntryNode{Value: entry, Next: curr}
	prev.Next = &newNode
	this.Entries = head.Next
	return nil
}

// distribute membership list across multiple UDP packets
// each with #entries <= MAX_ENTRY_NUM
// meantime check for failed entries and clean up obsolete failed/left entries
func (this *MemberList) ToPayloads() [][]byte {
	var uint16Arr []byte = make([]byte, 2)
	var uint32Arr []byte = make([]byte, 4)
	var uint64Arr []byte = make([]byte, 8)
	var count int = 0
	ret := make([][]byte, 0)
	head := new(EntryNode)
	prev := head

	memberListLock.Lock()

	ptr := this.Entries
	for ptr != nil {
		count = 0
		buf := bytes.NewBuffer(make([]byte, 0))
		//write member list header
		buf.WriteByte(this.Protocol)
		binary.LittleEndian.PutUint32(uint32Arr, this.ProtocolVersion)
		buf.Write(uint32Arr)

		// entries
		for ptr != nil && count < MAX_ENTRY_NUM {
			entry := ptr.Value

			if entry == this.SelfEntry || !entry.isObsolete() {
				buf.Write(entry.Ip[:])

				binary.LittleEndian.PutUint16(uint16Arr, entry.Port)
				buf.Write(uint16Arr)

				binary.LittleEndian.PutUint64(uint64Arr, uint64(entry.StartUpTs))
				buf.Write(uint64Arr)

				binary.LittleEndian.PutUint32(uint32Arr, entry.SeqNum)
				buf.Write(uint32Arr)

				status := entry.Status
				if entry == this.SelfEntry && entry.Status != LEFT {
					status = NORMAL
				} else if entry == this.SelfEntry {
					// status of self is set to left
					status = LEFT
				} else if entry.isFailed() && entry.Status != FAILED { // do a lazy flag check and write here
					if entry.Status == NORMAL {
						if this.Protocol == G {
							entry.Status = FAILED
							status = FAILED
							entry.setCleanupTimer()
						} else if this.Protocol == GS {
							entry.Status = SUS
							status = SUS
							entry.resetTimer()
						}
						reportStatusUpdate(entry)
					} else if entry.Status == SUS && this.Protocol == GS {
						entry.Status = FAILED
						status = FAILED
						entry.setCleanupTimer()
						reportStatusUpdate(entry)
					}
				}

				buf.WriteByte(status)
				count++
				prev.Next = ptr
				prev = ptr
			}
			ptr = ptr.Next
		}

		if count > 0 {
			ret = append(ret, buf.Bytes())
		}
	}

	this.Entries = head.Next
	memberListLock.Unlock()
	return ret
}

// deserialization
// todo: handle buffer underflow
func FromPayload(payload []byte, size int) *MemberList {
	buf := bytes.NewBuffer(payload)
	ret := new(MemberList)
	head := new(EntryNode)
	curr := head

	ret.Protocol = buf.Next(1)[0]
	ret.ProtocolVersion = binary.LittleEndian.Uint32(buf.Next(4))
	size -= 5

	var prevEntry *MemberListEntry = nil

	for size >= ENTRY_SIZE {
		var arr [4]byte
		copy(arr[:], buf.Next(4))
		var ip [4]uint8 = arr
		var port uint16 = binary.LittleEndian.Uint16(buf.Next(2))
		var startUpTs = int64(binary.LittleEndian.Uint64(buf.Next(8)))
		var seqNum uint32 = binary.LittleEndian.Uint32(buf.Next(4))
		var status uint8 = buf.Next(1)[0]

		listEntry := MemberListEntry{
			Ip:        ip,
			Port:      port,
			StartUpTs: startUpTs,
			SeqNum:    seqNum,
			Status:    status,
		}

		if prevEntry != nil && EntryCmp(prevEntry, &listEntry) >= 0 {
			log.Print("Corrupted remote member list: node id not sorted")
			return nil
		}
		prevEntry = &listEntry
		curr.Next = &EntryNode{Value: &listEntry, Next: nil}
		curr = curr.Next
		size -= ENTRY_SIZE
	}
	ret.Entries = head.Next
	return ret
}

func (this *MemberList) ToString() string {
	memberListLock.Lock()
	protocol := "Unknown"
	if this.Protocol == G {
		protocol = "Gossip"
	} else if this.Protocol == GS {
		protocol = "Gossip + Suspicion"
	}
	ret := fmt.Sprintf("Member list ------------\n"+
		"protocol: %s\n"+
		"protocolVersion: %d\n",
		protocol, this.ProtocolVersion)

	curr := this.Entries
	for curr != nil {
		if !curr.Value.isObsolete() {
			ret += "........................\n"
			if curr.Value == this.SelfEntry {
				ret += "[Local Machine]\n"
			}
			ret += curr.Value.ToString()
		}
		curr = curr.Next
	}
	memberListLock.Unlock()
	return ret
}

// merge two membership lists sorted by node id
func (this *MemberList) Merge(other *MemberList) {
	memberListLock.Lock()
	defer memberListLock.Unlock()

	this.mergeProtocol(other)

	head := new(EntryNode) // dummy linked-list head
	curr := head
	localEntry := this.Entries
	remoteEntry := other.Entries

	for localEntry != nil && remoteEntry != nil {
		cmp := EntryCmp(localEntry.Value, remoteEntry.Value)
		if cmp < 0 {
			curr.Next = localEntry
			localEntry = localEntry.Next
			curr = curr.Next
		} else if cmp > 0 { // new entry from remote
			if remoteEntry.Value.Status == SUS || remoteEntry.Value.Status == NORMAL {
				remoteEntry.Value.resetTimer()
				reportStatusUpdate(remoteEntry.Value)
				curr.Next = remoteEntry
				curr = curr.Next
			}
			remoteEntry = remoteEntry.Next
		} else {
			curr.Next = localEntry
			if localEntry.Value != this.SelfEntry {
				localEntry.Value.Merge(remoteEntry.Value, this.Protocol)
			}
			localEntry = localEntry.Next
			remoteEntry = remoteEntry.Next
			curr = curr.Next
		}	
	}
	
	if remoteEntry != nil { // more new entries
		for remoteEntry != nil {
			if remoteEntry.Value.Status == SUS || remoteEntry.Value.Status == NORMAL {
				remoteEntry.Value.resetTimer()
				reportStatusUpdate(remoteEntry.Value)
				curr.Next = remoteEntry
				curr = curr.Next
			} 
			remoteEntry = remoteEntry.Next
		}
	}

	if localEntry != nil {
		curr.Next = localEntry
	}
	this.Entries = head.Next
}

// handle potential protocol change
func (this *MemberList) mergeProtocol(other *MemberList) {
	memberListLock.Lock()
	defer memberListLock.Unlock()

	// resolve protocol incompatibility by pruning sus entries
	if this.ProtocolVersion > other.ProtocolVersion {
		if this.Protocol != other.Protocol && other.Protocol == GS {
			other.pruneSusEntries()
		}
	} else if this.ProtocolVersion < other.ProtocolVersion {
		if this.Protocol != other.Protocol && this.Protocol == GS {
			this.pruneSusEntries()
		}
	}

	if this.ProtocolVersion < other.ProtocolVersion {
		this.Protocol = other.Protocol
		this.ProtocolVersion = other.ProtocolVersion
	}

}

// get an array of host:port of alive members
func (this *MemberList) AliveMembers() []string {
	var ret []string
	memberListLock.Lock()
	ptr := this.Entries
	for ptr != nil {
		if ptr.Value != this.SelfEntry && ptr.Value.isAlive() {
			ret = append(ret, ptr.Value.Addr())
		}
		ptr = ptr.Next
	}
	memberListLock.Unlock()
	return ret
}

func (this *MemberList) UpdateProtocol(p uint8) {
	memberListLock.Lock()
	defer memberListLock.Unlock()
	if p != G && p != GS {
		log.Println("Failed to update protocol: unknown protocol")
		return
	}
	if this.Protocol == GS && p == G {
		this.pruneSusEntries()
	}
	this.Protocol = p
	this.ProtocolVersion++
}

// mark all sus entries as failed when we switch from Gossip + Suspicion to Gossip
func (this *MemberList) pruneSusEntries() {
	ptr := this.Entries
	for ptr != nil {
		if ptr.Value.Status == SUS {
			ptr.Value.Status = FAILED
		}
		ptr = ptr.Next
	}
}

func reportStatusUpdate(e *MemberListEntry) {
	currentTime := time.Now().UnixMilli()
	id := fmt.Sprintf("%s-%d", e.Addr(), e.StartUpTs)
	status := "JOINED"
	if e.Status == FAILED {
		status = "FAILED"
		ProcessLogger.LogFail(currentTime, id)
	} else if e.Status == LEFT {
		status = "LEFT"
		ProcessLogger.LogLeave(currentTime, id)
	} else if e.Status == SUS {
		status = "SUS"
		ProcessLogger.LogSUS(currentTime, id)
	} else {
		ProcessLogger.LogJoin(currentTime, id)
	}
	log.Printf("(%d) Entry update: %s - %s", time.Now().UnixMilli(), status, id)
}

// a simple test of serdes
// todo: move it to test cases
// func main() {
// 	e1 := MemberListEntry{
// 		Ip:        [4]uint8{6, 7, 8, 9},
// 		Port:      8000,
// 		StartUpTs: time.Now().UnixMilli(),
// 		Status:    NORMAL,
// 		SeqNum:    666,
// 	}

// 	e2 := MemberListEntry{
// 		Ip:        [4]uint8{125, 179, 210, 107},
// 		Port:      8001,
// 		StartUpTs: time.Now().UnixMilli() - 100,
// 		Status:    FAILED,
// 		SeqNum:    777,
// 	}
// 	n1 := EntryNode{Value: &e1}
// 	n2 := EntryNode{Value: &e2}
// 	n1.Next = &n2
// 	mbl := MemberList{
// 		Protocol:        G,
// 		ProtocolVersion: 321,
// 		Entries:         &n1,
// 	}

// 	payload := mbl.ToPayloads()

// 	deserialized := FromPayload(payload[0], len(payload[0]))

// 	fmt.Print(deserialized.ToString())

// }
