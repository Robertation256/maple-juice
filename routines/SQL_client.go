package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"log"
	"strconv"
	"fmt"
	"time"
)

var SQLClientInitialized bool = false

func InitializeSQLClient() {
	// join_juice and filter_juice are all static files. upload to sdfs on init
	SDFSPutFile("filter_juice.go", config.TemplateFileDir + "filter_juice.go")
	SDFSPutFile("join_juice.go", config.TemplateFileDir + "join_juice.go")
	SQLClientInitialized = true
}

// sql_fliter <input file> <regex> <num maples> <num juices> <sdfs_dest_filename> 
func ProcessFilterCmd(args []string) {

	if (!SQLClientInitialized) {
		InitializeSQLClient()
	}

	// TODO: add header parsing and allow user to specificy column
	if len(args) != 5 {
		log.Println("Invalid sql filter command")
		return
	}

	_, err := strconv.Atoi(args[2])
	_, err1 := strconv.Atoi(args[3])

	if err != nil || err1 != nil {
		log.Println("Invalid maple/juice task number")
		return
	}

	numMaples := args[2]
	numJuices := args[3]

	inputFile := args[0]
	// TODO: maybe check this? although it is captured in the executable
	regex := args[1]
	sdfsDestFileName := args[4]

	if len(inputFile) == 0 || len(regex) == 0 || len(sdfsDestFileName) == 0 {
		log.Println("file names and regex cannot be empty")
		return
	}

	timestamp := time.Now().UnixMilli()

	// generate executable with template
	executableName := fmt.Sprintf("filter_maple_%s_%d.go", inputFile, timestamp)
	err = util.GenerateFilterMapleExecutables(regex, executableName)

	if err != nil {
		log.Println("Error generating filter maple executable")
		return
	}
	
	// upload generated executable and input to sdfs
	SDFSPutFile(executableName, config.LocalFileDir + executableName)
	SDFSPutFile(inputFile, config.LocalFileDir + inputFile)

	// create maple task
	prefix := fmt.Sprintf("filter_%s_%d", inputFile, timestamp)
	ProcessMapleCmd([]string{executableName, numMaples, prefix, inputFile})

	// create juice task
	ProcessJuiceCmd([]string{"filter_juice.go", numJuices, prefix, sdfsDestFileName})

}

// sql_join <d1> <col_idx> <d2> <col_idx> <num maples> <num juices> <sdfs_dest_filename> 
func ProcessJoinCmd(args []string) {

	if (!SQLClientInitialized) {
		InitializeSQLClient()
	}

	// TODO: add header parsing and allow user to specificy column
	if len(args) != 7 {
		log.Println("Invalid sql filter command")
		return
	}

	_, err := strconv.Atoi(args[4])
	_, err1 := strconv.Atoi(args[5])

	if err != nil || err1 != nil {
		log.Println("Invalid maple/juice task number")
		return
	}

	numMaples := args[4]
	numJuices := args[5]

	d1 := args[0]
	col1 := args[1]
	d2 := args[2]
	col2 := args[2]
	sdfsDestFileName := args[6]

	if len(d1) == 0 || len(d2) == 0 || len(col1) == 0 || len(col2) == 0 || len(sdfsDestFileName) == 0 {
		log.Println("file names and column cannot be empty")
		return
	}

	timestamp := time.Now().UnixMilli()

	// generate executable with template for both d1 and d2
	executableNameD1 := fmt.Sprintf("join_maple_%s_%d.go", d1, timestamp)
	err = util.GenerateJoinMapleExecutables(col1, executableNameD1)

	if err != nil {
		log.Println("Error generating join maple executable")
		return
	}

	executableNameD2 := fmt.Sprintf("join_maple_%s_%d.go", d2, timestamp)
	err = util.GenerateJoinMapleExecutables(col1, executableNameD2)

	if err != nil {
		log.Println("Error generating join maple executable")
		return
	}

	// upload generated executable and input to sdfs
	SDFSPutFile(executableNameD1, config.LocalFileDir + executableNameD1)
	SDFSPutFile(d1, config.LocalFileDir + d1)

	SDFSPutFile(executableNameD2, config.LocalFileDir + executableNameD2)
	SDFSPutFile(d2, config.LocalFileDir + d2)

	// create maple task
	prefix := fmt.Sprintf("join_%s_%s_%d", d1, d2, timestamp)
	ProcessMapleCmd([]string{executableNameD1, numMaples, prefix, d1})
	ProcessMapleCmd([]string{executableNameD2, numMaples, prefix, d2})



	// create juice task
	ProcessJuiceCmd([]string{"join_juice.go", numJuices, prefix, sdfsDestFileName})

}