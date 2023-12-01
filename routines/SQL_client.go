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

		fileName1 := strings.Trim(splitted[3], ",")
		fileName2 := splitted[4]
		joinCondition := strings.Split(splitted[6], "=")
		if len(joinCondition) != 2 {
			log.Printf("Invalid join condition for join sql query")
			log.Println("Filter usage: SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
			return
		}

		left := strings.Split(joinCondition[0], ".")
		right := strings.Split(joinCondition[1], ".")

		if len(left) != 2 || len(right) != 2 {
			log.Printf("Invalid join condition for join sql query")
			log.Println("Filter usage: SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
			return
		}

		leftFile := left[0]
		rightFile := right[0]

		// trim quotation marks
		leftField := left[1][1:len(left[1])-1]
		rightField := right[1][1:len(right[1])-1]
		var field1, field2 string

		if (leftFile == fileName1 && rightFile == fileName2) {
			field1 = leftField
			field2 = rightField
		} else if (leftFile == fileName2 && rightFile == fileName1) {
			field1 = rightField
			field2 = leftField
		} else {
			log.Printf("Invalid join condition for join sql query: bad field qualifier")
			log.Println("Filter usage: SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
			return
		}

		executeJoinQuery(fileName1, fileName2, field1, field2)
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
	executableName := fmt.Sprintf("filter_maple_%s_%s_%d.go", inputFile, SelfNodeId, timestamp)
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
	err = ProcessMapleCmd([]string{executableName, strconv.Itoa(config.MapleTaskNum), prefix, inputFile})
	if err != nil {
		log.Println("Error executing Maple job for query", err)
		return 
	}
	// submit juice job
	err = ProcessJuiceCmd([]string{"filter_juice.go", strconv.Itoa(config.JuiceTaskNum), prefix, sdfsDestFileName})
	if err != nil {
		log.Println("Error executing Juice job for query", err)
		return
	}
	err = SDFSFetchAndConcatWithPrefix(sdfsDestFileName, sdfsDestFileName, RECEIVER_SDFS_CLIENT)
	if err != nil {
		log.Println("Error fetching query result to local folder", err)
		return
	}


	log.Printf("Query completed with result at %s in local folder", sdfsDestFileName)
}



func executeJoinQuery(fileName1, fileName2, fieldName1, fieldName2 string){
	if len(fileName1) == 0 || len(fileName2) == 0 || len(fieldName1) == 0 || len(fieldName2) == 0 {
		log.Println("Empty query argument found in join query")
		return
	}

	timestamp := time.Now().UnixMilli()

	// generate executable with template for both d1 and d2
	executableNameD1 := fmt.Sprintf("join_maple_%s_%s_%d.go", fileName1, SelfNodeId, timestamp)
	err := util.GenerateJoinMapleExecutables(fieldName1, executableNameD1)

	if err != nil {
		log.Println("Error generating maple executable for join query")
		return
	}

	executableNameD2 := fmt.Sprintf("join_maple_%s_%s_%d.go", fileName2, SelfNodeId, timestamp)
	err = util.GenerateJoinMapleExecutables(fieldName2, executableNameD2)

	if err != nil {
		log.Println("Error generating maple executable for join query")
		return
	}

	// upload generated executable and input to sdfs
	_, err = SDFSPutFile(executableNameD1, config.LocalFileDir + executableNameD1)
	if err != nil {
		log.Println("Error uploading executable for join query")
		return
	}
	_, err = SDFSPutFile(fieldName1, config.LocalFileDir + fieldName1)
	if err != nil {
		log.Println("Error uploading input file for join query")
		return
	}

	_, err = SDFSPutFile(executableNameD2, config.LocalFileDir + executableNameD2)
	if err != nil {
		log.Println("Error uploading executable for join query")
		return
	}
	_, err = SDFSPutFile(fileName2, config.LocalFileDir + fileName2)
	if err != nil {
		log.Println("Error uploading input file for join query")
		return
	}

	// create maple task
	prefix := fmt.Sprintf("join_%s_%s_%s_%d", fieldName1, fileName2, SelfNodeId, timestamp)
	err = ProcessMapleCmd([]string{executableNameD1, strconv.Itoa(config.MapleTaskNum), prefix, fileName1})
	if err != nil {
		log.Println("Error executing maple job for dataset1 in join query", err)
		return
	}
	err = ProcessMapleCmd([]string{executableNameD2, strconv.Itoa(config.MapleTaskNum), prefix, fileName2})
	if err != nil {
		log.Println("Error executing maple job for dataset2 in join query", err)
		return
	}

	sdfsDestFilePrefix := fmt.Sprintf("join_query_result_%s_%d", SelfNodeId, timestamp)
	// create juice task
	err = ProcessJuiceCmd([]string{"join_juice.go", strconv.Itoa(config.JuiceTaskNum), prefix, sdfsDestFilePrefix, "0", "0"})

	if err != nil {
		log.Println("Error executing juice job for join query", err)
		return
	}

	err = SDFSFetchAndConcatWithPrefix(sdfsDestFilePrefix, sdfsDestFilePrefix, RECEIVER_SDFS_CLIENT)

	if err != nil {
		log.Println("Error fetching join query result to local folder", err)
		return
	}

	log.Printf("Query completed with result at %s in local folder", sdfsDestFilePrefix)
}

