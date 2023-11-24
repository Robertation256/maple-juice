package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strings"
	"fmt"
	"strconv"
)

/*

assumpations about the input
1. user is aware of the schema, hence can specify the index of the column to perform join on
2. columns in the input file are separated by comma, i.e. each line in the data is a comma-separated string

example input: (if the schema is name, age, year)
someone, 18, freshman

if the join is d1.name = d2.id and the current maple is reading d1 

Maple (line):
	output(name, d1 @ line)

*/

func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	columnIdxFlag := flag.String("col", "", "Column index")
	inputFileFlag := flag.String("in", "", "Input filename")
	prefixFlag := flag.String("prefix", "", "SDFS intermediate filename prefix")
	flag.Parse()

	
	// check if required flags are provided
	if *columnIdxFlag == "" || *inputFileFlag == "" || *prefixFlag == ""{
		log.Fatal("Usage: go run yourprogram.go -col <column_index> -in <inputfile> -prefix <sdfs_intermediate_filename_prefix>")
	}

	output := make(map[string]*os.File)

	file, err := os.Open(*inputFileFlag)
	if err != nil {
		log.Fatal("Error opening input file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	columnIdx, conversionErr := strconv.Atoi(*columnIdxFlag)
	if conversionErr != nil {
		log.Fatal("Could not parse column index", conversionErr)
	}

	outputFiles := []string{}

	// process the lines
	for scanner.Scan() {
		line := scanner.Text()
		values := strings.Split(line, ",")

		// check if the line has enough columns
		if columnIdx < len(values) {
			key := values[columnIdx]
			key = strings.TrimSpace(key)

			// create or retrieve file descriptor for the key
			outputFile, exists := output[key]
			if !exists {
				outputFileName := fmt.Sprintf("%s-%s.txt", *prefixFlag, key)
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
			log.Fatalf("Column index %d out of bounds in line: %s\n", columnIdx, line)
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
}
