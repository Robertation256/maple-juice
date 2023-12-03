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

// Filter queries
// SELECT ALL FROM <file_name> WHERE "<column_name>"="<regex>"
// SELECT ALL FROM <file_name> WHERE "<regex>"	this one does does the whole line matching

// Join queries
// SELECT ALL FROM <file1>, <file2> WHERE <file1>."<field_name1>"=<file2>."<field_name2>"
func ProcessSqlQuery(query string) {

	splitted := strings.Split(query, " ")

	if len(splitted) != 6  && len(splitted) != 7{
		log.Println("Invalid SQL query")
		log.Println("Filter usage1 : SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
		log.Println("Filter usage2: SELECT ALL FROM <file_name> WHERE \"<regex>\"")
		log.Println("Join usage: SELECT ALL FROM <file1>, <file2> WHERE <file1>.\"<field_name1>\"=<file2>.\"<field_name2>\"")
		return
	}

	// filter query
	if len(splitted) == 6 {
		fileName := splitted[3]
		var regex, columnName string

		// queries specifies which column to filter
		if strings.Contains(splitted[5], "=") {
			condition := strings.Split(splitted[5], "=")
			if len(condition) != 2{
				log.Printf("Invalid condition for filter sql query")
				log.Println("Filter usage1: SELECT ALL FROM <file_name> WHERE \"<column_name>\"=\"<regex>\"")
				log.Println("Filter usage2: SELECT ALL FROM <file_name> WHERE \"<regex>\"")
				return
			} 
	
			columnName = condition[0]
			regex = condition[1]
			// trim quotation marks
			columnName = columnName[1:len(columnName)-1]
			regex = regex[1:len(regex)-1]

		// query does not specify which column to filter, use the whole condition as a regex
		} else {
			columnName = ""
			regex = splitted[5][1:len(splitted[5])-1]
		}


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

// Select percent composition, used for MP4 demo only
// SPC "<interconne_type>" FROM <input_file>
func ProcessSpcQuery(args []string) {
	if len(args) != 3 {
		log.Print("Invalid select percent composition query")
		log.Print("Usage: SPC \"<interconne_type>\" FROM <input_file>")
		return
	}
	interConnType := args[0][1:len(args[0])-1]
	inputFile := args[2]

	if len(interConnType) == 0 || len(inputFile) == 0{
		log.Print("Invalid select percent composition query arguments")
		log.Print("Usage: SPC \"<interconne_type>\" FROM <input_file>")
		return
	}

	

	timestamp := time.Now().UnixMilli()
	selfNodeId := SelfNodeId

	mapleOneExeName := fmt.Sprintf("demo_maple1_%s_%d.go", selfNodeId, timestamp)
	mapleTwoExeName := "demo_maple2.go"
	juiceOneExeName := "demo_juice1.go"
	juiceTwoExeName := "demo_juice2.go"

	err := util.GenerateDemoMapleOneExecutable(interConnType, mapleOneExeName)

	if err != nil {
		log.Println("Error generating maple executable for SPC phase 1")
		return
	}

	// todo: make these async uploads
	// upload input file and executable to sdfs
	log.Printf("Uploading query input file")
	_, err = SDFSPutFile(inputFile, config.LocalFileDir + inputFile)
	if err != nil {
		log.Println("Error uploading input file", err)
	}


	log.Printf("Uploading executables")
	_, err = SDFSPutFile(mapleOneExeName, config.LocalFileDir + mapleOneExeName)
	if err != nil {
		log.Println("Error uploading maple1", err)
	}

	_, err = SDFSPutFile(mapleTwoExeName, config.TemplateFileDir + mapleTwoExeName)
	if err != nil {
		log.Println("Error uploading maple2", err)
	}

	_, err = SDFSPutFile(juiceOneExeName, config.TemplateFileDir + juiceOneExeName)
	if err != nil {
		log.Println("Error uploading juice1", err)
	}

	_, err = SDFSPutFile(juiceTwoExeName, config.TemplateFileDir + juiceTwoExeName)
	if err != nil {
		log.Println("Error uploading juice2", err)
	}


	// start maple1
	phaseOnePrefix := fmt.Sprintf("maple_spc1_%s_%d", selfNodeId, timestamp)
	phaseTwoPrefix := fmt.Sprintf("maple_spc2_%s_%d", selfNodeId, timestamp)

	phaseOneOut := fmt.Sprintf("out_spc1_%s_%d", selfNodeId, timestamp)
	phaseTwoOut := fmt.Sprintf("out_spc2_%s_%d", selfNodeId, timestamp)

	phaseOnePrefix = strings.Replace(phaseOnePrefix, "-", "_", -1)
	phaseTwoPrefix = strings.Replace(phaseTwoPrefix, "-", "_", -1)

	phaseOneOut = strings.Replace(phaseOneOut, "-", "_", -1)
	phaseTwoOut = strings.Replace(phaseTwoOut, "-", "_", -1)


	log.Println("Starting SPC phase 1")
	err = ProcessMapleCmd([]string{mapleOneExeName, strconv.Itoa(config.MapleTaskNum), phaseOnePrefix, inputFile, "1"})
	if err != nil {
		log.Println("Error executing maple for SPC phase 1", err)
		return
	}

	err = ProcessJuiceCmd([]string{juiceOneExeName, strconv.Itoa(config.JuiceTaskNum), phaseOnePrefix, phaseOneOut, "0", "0"})
	if err != nil {
		log.Println("Error executing juice for SPC phase 1", err)
		return
	}

	err = SDFSFetchAndConcatWithPrefix(phaseOneOut, phaseOneOut, RECEIVER_SDFS_CLIENT)

	if err != nil {
		log.Println("Error fetching SPC phase 1 result to local folder", err)
		return
	}

	log.Println("Completed SPC phase 1")

	log.Printf("Uploading concatenated phase1 output file")
	_, err = SDFSPutFile(phaseOneOut, config.LocalFileDir + phaseOneOut)
	if err != nil {
		log.Println("Error uploading concatenated phase1 output file", err)
	}

	time.Sleep(1 * time.Second)

	log.Println("Starting SPC phase 2")
	err = ProcessMapleCmd([]string{mapleTwoExeName, strconv.Itoa(config.MapleTaskNum), phaseTwoPrefix, phaseOneOut, "0"})
	if err != nil {
		log.Println("Error executing maple for SPC phase 2", err)
		return
	}

	err = ProcessJuiceCmd([]string{juiceTwoExeName, strconv.Itoa(config.JuiceTaskNum), phaseTwoPrefix, phaseTwoOut, "0", "0"})
	if err != nil {
		log.Println("Error executing juice for SPC phase 2", err)
		return
	}

	err = SDFSFetchAndConcatWithPrefix(phaseTwoOut, phaseTwoOut, RECEIVER_SDFS_CLIENT)

	if err != nil {
		log.Println("Error fetching SPC query result to local folder", err)
		return
	}

	log.Printf("SPC Query completed with result at %s in local folder", phaseTwoOut)
}


func executeFilterQuery(inputFile string, columnName string, regex string){

	if len(inputFile) == 0 || len(regex) == 0 {
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
	err = ProcessMapleCmd([]string{executableName, strconv.Itoa(config.MapleTaskNum), prefix, inputFile, "1"})
	if err != nil {
		log.Println("Error executing Maple job for query", err)
		return 
	}
	// submit juice job
	err = ProcessJuiceCmd([]string{"filter_juice.go", strconv.Itoa(config.JuiceTaskNum), prefix, sdfsDestFileName, "0", "0"})
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
		log.Println("Error uploading executable for join query", err)
		return
	}
	_, err = SDFSPutFile(fileName1, config.LocalFileDir + fileName1)
	if err != nil {
		log.Println("Error uploading input file for join query", err)
		return
	}

	_, err = SDFSPutFile(executableNameD2, config.LocalFileDir + executableNameD2)
	if err != nil {
		log.Println("Error uploading executable for join query", err)
		return
	}
	_, err = SDFSPutFile(fileName2, config.LocalFileDir + fileName2)
	if err != nil {
		log.Println("Error uploading input file for join query", err)
		return
	}

	// create maple task
	prefix := fmt.Sprintf("join_%s_%s_%s_%d", fieldName1, fileName2, SelfNodeId, timestamp)
	err = ProcessMapleCmd([]string{executableNameD1, strconv.Itoa(config.MapleTaskNum), prefix, fileName1, "1"})
	if err != nil {
		log.Println("Error executing maple job for dataset1 in join query", err)
		return
	}
	err = ProcessMapleCmd([]string{executableNameD2, strconv.Itoa(config.MapleTaskNum), prefix, fileName2, "1"})
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

