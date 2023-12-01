package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strings"
	"fmt"
)


func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	joinColumn := "{{ .JoinColumn }}"
	inputFileFlag := flag.String("in", "", "Input filename")
	prefixFlag := flag.String("prefix", "", "SDFS intermediate filename prefix")
	flag.Parse()

	
	// check if required flags are provided
	if  *inputFileFlag == "" || *prefixFlag == ""{
		log.Fatal("Usage: go run yourprogram.go -col <column_index> -in <inputfile> -prefix <sdfs_intermediate_filename_prefix>")
	}

	output := make(map[string]*os.File)

	file, err := os.Open(*inputFileFlag)
	if err != nil {
		log.Fatal("Error opening input file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if !scanner.Scan(){
		log.Fatal("Empty input to join maple executable")
	}
	header := scanner.Text()

	joinColumnIdx := findColumnIndex(header, joinColumn)
	if joinColumnIdx < 0 {
		log.Fatal("Unable to locate column for filter operation in input file header")
	}


	outputFiles := []string{}

	// process the lines
	for scanner.Scan() {
		line := scanner.Text()
		values := strings.Split(line, ",")

		// check if the line has enough columns
		if joinColumnIdx < len(values) {
			key := values[joinColumnIdx]
			key = strings.TrimSpace(key)

			// create or retrieve file descriptor for the key
			outputFile, exists := output[key]
			if !exists {
				outputFileName := fmt.Sprintf("%s-%s-%s.txt", *prefixFlag, extractPartitionNumber(*inputFileFlag), key)
				outputFiles = append(outputFiles, outputFileName)
				var err error
				outputFile, err = os.Create(nodeManagerFileDir + outputFileName)
				if err != nil {
					log.Fatal("Error creating output file:", err)
				}
				output[key] = outputFile
			}

			formattedValue := fmt.Sprintf("%s @ %s", *inputFileFlag, line)
			_, err := outputFile.WriteString(formattedValue + "\n")
			if err != nil {
				log.Fatal("Error writing to output file:", err)
			}
		} else {
			log.Fatalf("Column index %d out of bounds in line: %s\n", joinColumnIdx, line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("Error reading input file:", err)
	}

	// close all file descriptors
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
