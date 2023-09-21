package routines

import (
	"sync"
)
var NeedTermination bool
var SIGTERM sync.WaitGroup     // main termination
var HEARTBEAT_SENDER_TERM sync.WaitGroup // graceful logger termination


func InitSignals(){
	NeedTermination = false
	SIGTERM.Add(1)
	HEARTBEAT_SENDER_TERM.Add(1)
}

func SignalTermination(){
	NeedTermination = true 
	SIGTERM.Done()
}