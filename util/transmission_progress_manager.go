package util

import (
	"fmt"
	"sync"
)

const (
	LOCAL_WRITE_COMPLETE int = 1
	GLOBAL_WRITE_COMPLETE   int = 2
)


// uitility for tracking file transmission completion
type TransmissionProgressManager struct {
	writeTaskCompleted map[string]int // a map of transmission id for tracing the progress of a file transmission
	lock               sync.RWMutex
}

func NewTransmissionProgressManager() *TransmissionProgressManager {
	return &TransmissionProgressManager{
		writeTaskCompleted: make(map[string]int),
	}
}

func (this *TransmissionProgressManager) IsLocalCompleted(transmissionId string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	value, exists := this.writeTaskCompleted[transmissionId]
	return exists && value == LOCAL_WRITE_COMPLETE
}


//check if a write has completed at all replicas
func (this *TransmissionProgressManager) IsGlobalCompleted(transmissionId string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	value, exists := this.writeTaskCompleted[transmissionId]
	return exists && value == GLOBAL_WRITE_COMPLETE
}

// remove a transmission from progress tracker 
func (this *TransmissionProgressManager) ReleaseTracking(transmissionId string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	_, exists := this.writeTaskCompleted[transmissionId]
	if exists {
		delete(this.writeTaskCompleted, transmissionId)
	}
}

func (this *TransmissionProgressManager) Complete(transmissionId string, completionType int) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.writeTaskCompleted[transmissionId] = completionType
}

func (this *TransmissionProgressManager) LocalComplete(transmissionId string) {
	this.Complete(transmissionId, LOCAL_WRITE_COMPLETE)
}

func (this *TransmissionProgressManager) GlobalComplete(transmissionId string) {
	this.Complete(transmissionId, GLOBAL_WRITE_COMPLETE)
}


type TransmissionIdGenerator struct {
	serviceName string 
	fileId int 
	lock sync.Mutex
}

func NewTransmissionIdGenerator(serviceName string) *TransmissionIdGenerator {
	return &TransmissionIdGenerator{
		serviceName: serviceName,
	}
}

func (this *TransmissionIdGenerator) NewTransmissionId(fileName string) string {
	this.lock.Lock()
	this.fileId += 1
	id := this.fileId
	this.lock.Unlock()
	return fmt.Sprintf("%s-%s-%d", this.serviceName, fileName, id)
}
