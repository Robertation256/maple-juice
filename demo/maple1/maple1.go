package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

/*

Maple (line):
	if Interconne = X:
		output(Detection_, 1)

*/

func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	filterColumn := "Interconne"
	interconneValue := "{{ .InterconneValue }}"
	inputFileFlag := flag.String("in", "", "Input filename")
	prefixFlag := flag.String("prefix", "", "SDFS intermediate filename prefix")
	flag.Parse()

	filterColumn = strings.TrimSpace(filterColumn)
	filterColumnIdx := -1

	// check if required flags are provided
	if *inputFileFlag == "" || *prefixFlag == "" {
		log.Fatal("Usage: go run maple1.go -in <inputfile> -prefix <sdfs_intermediate_filename_prefix>")
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
	if !scanner.Scan() {
		log.Fatal("Empty input to filter maple executable")
	}

	header := scanner.Text()
	filterColumnIdx = findColumnIndex(header, filterColumn)
	if filterColumnIdx < 0 {
		log.Fatalf("Unable to locate column (%s)for filter operation in input file header (%s)", filterColumn, header)
	}

	detectionColumnIdx := findColumnIndex(header, "Detection_")

	outputFiles := []string{}
	key := "DummyFilterKey"

	for scanner.Scan() {
		line := scanner.Text()

		// check if the field matches the regular expression
		if getFieldByIndex(line, filterColumnIdx) == interconneValue {

			key = strings.TrimSpace(getFieldByIndex(line, detectionColumnIdx))
			// slash is a path separatir in go and is not allowed in filename
			key = strings.ReplaceAll(key, "/", " OR ")

			if len(key) == 0 {
				key = "EmptyString"
			}

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
			_, err := outputFile.WriteString("1" + "\n")
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
	if len(splitted) != 2 {
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
