package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"fmt"
	"regexp"
	"strings"
)


func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	filterColumn := "{{ .FilterColumn }}"
	regexFlag := "{{ .Regex }}"
	inputFileFlag := flag.String("in", "", "Input filename")
	prefixFlag := flag.String("prefix", "", "SDFS intermediate filename prefix")
	flag.Parse()

	filterColumn = strings.TrimSpace(filterColumn)
	filterByColumn := len(filterColumn) > 0	
	filterColumnIdx := -1

	// check if required flags are provided
	if *inputFileFlag == "" || *prefixFlag == "" {
		log.Fatal("Usage: go run filter_maple.go -E <regex> -in <inputfile> -prefix <sdfs_intermediate_filename_prefix>")
		return
	}

	// compile the regular expression
	regexpPattern, err := regexp.Compile(regexFlag)
	if err != nil {
		log.Fatal("Error compiling regular expression:", err)
		return
	}

	output := make(map[string]*os.File)

	file, err := os.Open(*inputFileFlag)
	if err != nil {
		log.Fatal("Error opening input file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan(){
		log.Fatal("Empty input to filter maple executable")
	}

	if filterByColumn {
		header := scanner.Text()
		filterColumnIdx = findColumnIndex(header, filterColumn)
		if filterColumnIdx < 0 {
			log.Fatalf("Unable to locate column (%s)for filter operation in input file header (%s)", filterColumn, header)
		}
	}

	outputFiles := []string{}
	key := "DummyFilterKey"


	for scanner.Scan() {
		line := scanner.Text()

		// check if the field matches the regular expression
		if  (!filterByColumn && regexpPattern.MatchString(line)) || (filterByColumn && regexpPattern.MatchString(getFieldByIndex(line, filterColumnIdx))) {

			// create or retrieve file descriptor for the key
			outputFile, exists := output[key]
			if !exists {
				outputFileName := fmt.Sprintf("%s-%s-%s", *prefixFlag, extractPartitionNumber(*inputFileFlag), key)
				var err error
				outputFile, err = os.Create(nodeManagerFileDir + outputFileName)
				if err != nil {
					log.Fatal("Error creating output file:", err)
				}
				output[key] = outputFile
				outputFiles = append(outputFiles, outputFileName)
			}

			// write directly to the file descriptor
			_, err := outputFile.WriteString(line + "\n")
			if err != nil {
				log.Fatal("Error writing to output file:", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("Error reading input file:", err)
		return
	}

	for _, file := range output {
		file.Close()
	}

	fmt.Println(strings.Join(outputFiles, ","))
	os.Exit(0)
}

func extractPartitionNumber(inputFileName string) string {
	splitted := strings.Split(inputFileName, "-")
	if (len(splitted)!=2){
		log.Fatalf("WARN: invalid maple input file name format for %s", inputFileName)
		return ""
	}
	return splitted[1]
}


func findColumnIndex(header string, columnName string) int {
	splitted := strings.Split(header, ",")
	idx := 0
	for _, field := range splitted {
		field = strings.Trim(field, " \n\r")
		if field == columnName {
			return idx
		}
		idx++
	}
	return -1
}

func getFieldByIndex(line string, index int) string {
	splitted := strings.Split(line, ",")
	if index < len(splitted) {
		return splitted[index]
	}
	log.Fatal("Invalid maple input file, mismatch between header and record field length")
	return ""
}
