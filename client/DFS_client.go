package client


import (
	"cs425-mp2/util"
)


const (
	FILE_PUT int = 1
	FILE_GET int = 2
	FILE_DELETE int = 3
	FILE_LIST int = 4 
)


type DfsRequest struct {
	RequestType int
	FileName string
}



// return type for DFS client metadata query
type DfsResponse struct {
	FileName string
	Master   util.FileInfo
	Servants []util.FileInfo
}




func GetFile(fileName string){

}


func PutFile(fileName string){}


func DeleteFile(fileName string){}



func ListFile(fileName string){}






