package routines

import (
	"sync"
	"cs425-mp2/config"
)


var NeedTermination bool
var SERVER_STARTED sync.WaitGroup
var SIGTERM sync.WaitGroup     // main termination
var HEARTBEAT_SENDER_TERM sync.WaitGroup // hearbeat sender termination
var FILE_METADATA_SERVER_SIGTERM sync.WaitGroup

var LOG_SERVER_STARTED sync.WaitGroup
var MEMBERSHIP_SERVER_STARTED sync.WaitGroup
var INTRODUCER_SERVER_STARTED sync.WaitGroup
var LEADER_ELECTION_SERVER_STARTED sync.WaitGroup
var FILE_SERVER_STARTED sync.WaitGroup


// todo: add file metadata and file server startup signals



func InitSignals() {
	NeedTermination = false
	SIGTERM.Add(1)
	HEARTBEAT_SENDER_TERM.Add(1)

	LOG_SERVER_STARTED.Add(1)

	MEMBERSHIP_SERVER_STARTED.Add(1)
	INTRODUCER_SERVER_STARTED.Add(1)

	LEADER_ELECTION_SERVER_STARTED.Add(1)
	FILE_SERVER_STARTED.Add(1)
}

func WaitAllServerStart(){
	LOG_SERVER_STARTED.Wait()
	MEMBERSHIP_SERVER_STARTED.Wait()
	LEADER_ELECTION_SERVER_STARTED.Wait()
	// FILE_SERVER_STARTED.Wait()

	if config.IsIntroducer {
		INTRODUCER_SERVER_STARTED.Wait()
	}
}

func SignalTermination() {
	NeedTermination = true 
	SIGTERM.Done()
}

func AddServerToWait() {
	SERVER_STARTED.Add(1)
}