package util

import (
	"errors"
	"log"
	"sync"
	"time"
)

const (
	FILE_OP_READ  int = 11
	FILE_OP_WRITE int = 22

	MAX_TASK_QUEUE_SIZE int    = 500
	MAX_CONCURRENT_READ uint32 = 2
)

type Operation = func() error

type FileOperation struct {
	OperationType    int
	Operation        Operation // function handler for the file op
	OperationTimeout *time.Duration
	ResponseChan     chan error
}

func NewFileOperation(opType int, opHandler Operation, timeout *time.Duration) *FileOperation {
	return &FileOperation{
		OperationType: opType,
		Operation:     opHandler,
		OperationTimeout: timeout,
		ResponseChan:  make(chan error, 1),
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
	queue     []FileOperation
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

func (this *opQueue) Pop() *FileOperation {
	this.queueLock.Lock()
	defer this.queueLock.Unlock()
	if len(this.queue) == 0 {
		return nil
	}
	ret := &this.queue[0]
	this.queue = this.queue[1:]
	return ret
}

// pop the first read task in the queue
func (this *opQueue) ExistsReadTask() bool {
	this.queueLock.RLock()
	defer this.queueLock.RUnlock()
	for _, task := range this.queue {
		if task.OperationType == FILE_OP_READ {
			return true
		}
	}
	return false
}

// pop the first read task in the queue
func (this *opQueue) PopFirstReadTask() *FileOperation {
	this.queueLock.Lock()
	defer this.queueLock.Unlock()
	for idx, task := range this.queue {
		if task.OperationType == FILE_OP_READ {
			this.queue = append(this.queue[:idx], this.queue[idx+1:]...)
			return &task
		}
	}
	return nil
}

func (this *opQueue) Peek() *FileOperation {
	this.queueLock.RLock()
	defer this.queueLock.RUnlock()
	if len(this.queue) == 0 {
		return nil
	}
	ret := &this.queue[0]
	return ret
}

// scheduler for operations on a single file
type FileOperationScheduler struct {
	queue          opQueue
	preemptedRound uint32 // number of rounds the head of the op queue gets preempted
	maxPreemptedRounds uint32
	fileLock	*CapacityRWLock
	wakeUpSignals chan struct{} // wakes up the scheduler when not task queue is not empty
}

func NewFileOperationScheduler(maxPreemptedRounds uint32) *FileOperationScheduler {
	return &FileOperationScheduler{
		queue:              *newOpQueue(),
		maxPreemptedRounds: maxPreemptedRounds,
		fileLock: NewCapacityRWLock(MAX_CONCURRENT_READ),
		wakeUpSignals:      make(chan struct{}, MAX_TASK_QUEUE_SIZE),
	}
}

func (this *FileOperationScheduler) StartScheduling() {
	for {
		select {
		case <-this.wakeUpSignals:
			this.schedule()
		}
	}
}

// schedule and execute next proper task
func (this *FileOperationScheduler) schedule() {
	task := this.queue.Peek()

	// this should not happen as len(wakeUpSignals) = len(queue), but in case this happens, do nothing
	if task == nil {
		return
	}

	// next task is read
	if task.OperationType == FILE_OP_READ {
		this.fileLock.RLock()
		this.preemptedRound = 0

		task = this.queue.Pop()

		// execute the actuall task
		go func() {
			defer this.fileLock.RUnlock()
			execute(task)
		}()
		return
	}

	// next task is write
	if task.OperationType == FILE_OP_WRITE {
		for {
			// if read in progress, try exploit read concurrency
			if this.preemptedRound < this.maxPreemptedRounds && this.queue.ExistsReadTask() && this.fileLock.HasReaders() {
				this.preemptedRound++
				task = this.queue.PopFirstReadTask()
				if task == nil {
					log.Printf("Error: unexpect task queue behavior")
					return
				}
				this.fileLock.RLock()
				go func() {
					defer this.fileLock.RUnlock()
					execute(task)
				}()
				return
			}

			//otherwise try schedule this write task
			if (this.fileLock.TryWLock()){
				task = this.queue.Pop()
				this.preemptedRound = 0
				go func() {
					defer this.fileLock.WUnlock()
					execute(task)
				}()
				return
			}
		}
	}

	log.Printf("Unknow file operation type: %d", task.OperationType)
}

func execute(task *FileOperation) {
	timeout := time.After(*task.OperationTimeout)
	c := make(chan error, 1)
	go func() { c <- task.Operation() }()
	select {
	case err := <-c:
		task.ResponseChan <- err
	case <-timeout:
		task.ResponseChan <- errors.New("File operation timeout")
	}
}

func (this *FileOperationScheduler) AddTask(task *FileOperation) error {
	err := this.queue.Push(task)
	if err != nil {
		return err
	}
	this.wakeUpSignals <- struct{}{}
	return nil
}
