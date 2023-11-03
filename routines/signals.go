package routines

import (
	"sync"
)
var NeedTermination bool
var SERVER_STARTED sync.WaitGroup
var SIGTERM sync.WaitGroup     // main termination
var HEARTBEAT_SENDER_TERM sync.WaitGroup // hearbeat sender termination
var FILE_METADATA_SERVER_SIGTERM sync.WaitGroup


func InitSignals() {
	NeedTermination = false
	SIGTERM.Add(1)
	HEARTBEAT_SENDER_TERM.Add(1)
}

func SignalTermination() {
	NeedTermination = true 
	SIGTERM.Done()
}

func AddServerToWait() {
	SERVER_STARTED.Add(1)
}