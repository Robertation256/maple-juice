package util

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	FILE_OP_READ int = 11
	FILE_OP_WRITE int = 22

	MAX_TASK_QUEUE_SIZE int = 500
)

type Operation = func()


type FileOperation struct {
	OperationType int 
	Operation  Operation	// function handler for the file op
}

func NewFileOperation(opType int, opHandler Operation) *FileOperation {
	return &FileOperation{
		OperationType: opType,
		Operation: opHandler,
	}
}

func (this *FileOperation) IsRead() bool {
	return this.OperationType == FILE_OP_READ
}

// true if op is write or delete
func (this *FileOperation) IsWrite() bool {
	return this.OperationType == FILE_OP_WRITE
}

type opQueue struct {
	queue []FileOperation
	queueLock sync.RWMutex
}

func newOpQueue() *opQueue {
	return &opQueue{
		queue: make([]FileOperation, 0),
	}
}

func (this *opQueue) Push(op *FileOperation) error {
	this.queueLock.Lock()
	defer this.queueLock.Unlock()
	if len(this.queue) >= MAX_TASK_QUEUE_SIZE {
		return errors.New("Task queue is full")
	}
	this.queue = append(this.queue, *op)
	return nil
}

func (this *opQueue) Pop() *FileOperation{
	this.queueLock.Lock()
	defer this.queueLock.Unlock()
	if len(this.queue) == 0{
		return nil
	}
	ret := &this.queue[0]
	this.queue = this.queue[1:]
	return ret
}

func (this *opQueue) Peek() *FileOperation{
	this.queueLock.RLock()
	defer this.queueLock.RUnlock()
	if len(this.queue) == 0{
		return nil
	}
	ret := &this.queue[0]
	return ret
}

// scheduler for operations on a single file
type FileOperationScheduler struct {
	queue opQueue
	waitRound atomic.Int32  // number of rounds the head of the op queue gets preempted

	numReadInProgress atomic.Int32
	
	readCompletion sync.WaitGroup
	writeCompletion sync.WaitGroup

	maxPreemptedRounds int 

	wakeUpSignals chan byte		// wakes up the scheduler when not empty
}

func NewFileOperationScheduler(maxPreemptedRounds int) *FileOperationScheduler {
	return &FileOperationScheduler{
		queue: *newOpQueue(),
		maxPreemptedRounds: maxPreemptedRounds,
		wakeUpSignals: make(chan byte, MAX_TASK_QUEUE_SIZE),
	}
}


func (this *FileOperationScheduler) StartScheduling(){

	for {
		select {
		case <- this.wakeUpSignals:

		}
	}
}

// return true if scheduled
func (this *FileOperationScheduler) trySchedule() bool {
	task := this.queue.Peek()

	// this should not happen as len(wakeUpSignals) = len(queue), but in case happens, just discard this signal
	if task == nil {
		return true 
	}

	
}






func (this *FileOperationScheduler) AddTask(task *FileOperation) error {
	err := this.queue.Push(task)
	if err != nil {
		return err
	}
	this.wakeUpSignals <- byte(0)
	return nil
}



