package routines

import (
	"cs425-mp4/config"
	"cs425-mp4/util"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// func InitializeSQLClient() {
// 	// join_juice and filter_juice are all static files. upload to sdfs on init
// 	SDFSPutFile("filter_juice.go", config.TemplateFileDir + "filter_juice.go")
// 	SDFSPutFile("join_juice.go", config.TemplateFileDir + "join_juice.go")
// 	SQLClientInitialized = true
// }

// SELECT ALL FROM <file_name> WHERE "<column_name>"="<regex>"
// SELECT ALL FROM <file1>, <file2> WHERE <file1>."<field_name1>"=<file2>."<field_name2>"
func ProcessSqlQuery(query string) {

	splitted := strings.Split(query, " ")

	if len(splitted) != 6  && len(splitted) != 7{
		log.Println("Invalid SQL query")
		log.Println("Filter usage: SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
		log.Println("Join usage: SELECT ALL FROM <file1>, <file2> WHERE <file1>.\"<field_name1>\"=<file2>.\"<field_name2>\"")
		return
	}

	// filter query
	if len(splitted) == 6 {
		fileName := splitted[3]
		condition := strings.Split(splitted[5], "=")
		if len(condition) != 2{
			log.Printf("Invalid condition for filter sql query")
			log.Println("Filter usage: SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
			return
		} 

		columnName := condition[0]
		regex := condition[1]
		// trim quotation marks
		columnName = columnName[1:len(columnName)-1]
		regex = regex[1:len(regex)-1]

		executeFilterQuery(fileName, columnName, regex)
	} else {
		// join query




	}




}



func executeFilterQuery(inputFile string, columnName string, regex string){

	if len(inputFile) == 0 || len(regex) == 0 || len(columnName) == 0{
		log.Println("Invalid filter query arguments")
		return
	}

	_, err := regexp.Compile(regex)
	if err != nil {
		log.Printf("Invalid regular expression in filter query: (%s)", regex)
		return
	}

	timestamp := time.Now().UnixMilli()
	sdfsDestFileName := fmt.Sprintf("filter_query_result_%s_%d", SelfNodeId, timestamp)

	// generate executable with template
	executableName := fmt.Sprintf("filter_maple_%s_%d.go", inputFile, timestamp)
	err = util.GenerateFilterMapleExecutables(columnName, regex, executableName)

	if err != nil {
		log.Println("Error generating filter maple executable")
		return
	}
	
	// upload generated executable and input to sdfs
	log.Printf("Uploading query input file")
	_, err = SDFSPutFile(inputFile, config.LocalFileDir + inputFile)
	if err != nil {
		log.Println("Error uploading input file", err)
	}

	log.Printf("Uploading executables")
	_, err = SDFSPutFile(executableName, config.LocalFileDir + executableName)
	if err != nil {
		log.Println("Error uploading maple executable", err)
	}
	_, err = SDFSPutFile("filter_juice.go", config.TemplateFileDir + "filter_juice.go")
	if err != nil {
		log.Println("Error uploading juice executable", err)
	}
	
	// submit maple job
	prefix := fmt.Sprintf("%s_%s_%d", inputFile, SelfNodeId, timestamp)
	ProcessMapleCmd([]string{executableName, strconv.Itoa(config.MapleTaskNum), prefix, inputFile})

	// submit juice job
	ProcessJuiceCmd([]string{"filter_juice.go", strconv.Itoa(config.JuiceTaskNum), prefix, sdfsDestFileName})

	SDFSFetchAndConcatWithPrefix(sdfsDestFileName, sdfsDestFileName, RECEIVER_SDFS_CLIENT)

	log.Printf("Query completed with result at %s", sdfsDestFileName+"")

}

// sql_join <d1> <col_idx> <d2> <col_idx> <num maples> <num juices> <sdfs_dest_filename> 
func ProcessJoinCmd(args []string) {

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