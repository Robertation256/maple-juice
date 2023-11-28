package util

import (
	"sync"
	"sync/atomic"
)

// RWLock with a fixed read capacity
type CapacityRWLock struct {
	readerCount atomic.Int32
	signals chan struct{}
	lock sync.RWMutex
}

func NewCapacityRWLock(readCapacity uint32) *CapacityRWLock {
	ret := &CapacityRWLock{
		signals: make(chan struct{}, readCapacity),
	}

	for i := readCapacity; i>0; i-- {
		ret.signals <- struct{}{}
	}

	return ret 
}

// merely tests if current reader count > 0
// should never be for acquiring read lock
func (this *CapacityRWLock) HasReaders() bool {
	return this.readerCount.Load() > 0
}

func (this *CapacityRWLock) RLock(){
	<- this.signals
	this.lock.RLock()
	this.readerCount.Add(1)
}

func (this *CapacityRWLock) RUnlock(){
	this.readerCount.Add(-1)
	this.lock.RUnlock()
	this.signals <- struct{}{}
}

func (this *CapacityRWLock) WLock(){
	this.lock.Lock()
}


func (this *CapacityRWLock) TryWLock() bool {
	return this.lock.TryLock()
}

func (this *CapacityRWLock) WUnlock(){
	this.lock.Unlock()
}






