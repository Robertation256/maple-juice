package util



// metadata reported by each file server and collected by the metadata server
type FileServerMetadataReport struct {
	NodeId string
	FileEntries []FileStatus
}


type FileStatus struct {
	FileName string
	IsMaster bool
	Version int 
}