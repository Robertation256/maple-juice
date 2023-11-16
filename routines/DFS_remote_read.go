package routines

import (
	"net/rpc"
)

// rpc for facilitating DFS mutliread command from DFS client


type DfsRemoteReader struct {}



func NewDfsRemoteReader() *DfsRemoteReader {
	return &DfsRemoteReader{}
}


func (this *DfsRemoteReader) Register(){
	rpc.Register(this)
}


func (this *DfsRemoteReader) Read(fileName *string, reply *string) error {
	localFileName := "remoted_initiated_" + *fileName 

	err := SDFSGetFile(*fileName, localFileName, RECEIVER_SDFS_CLIENT)

	if err == nil {
		*reply = "DONE"
	} else {
		*reply = "FAILED"
	}
	return err
}