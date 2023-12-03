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
	output (1, line)

*/

func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	inputFileFlag := flag.String("in", "", "Input filename")
	prefixFlag := flag.String("prefix", "", "SDFS intermediate filename prefix")
	flag.Parse()

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

	outputFiles := []string{}
	key := "DummyFilterKey"

	for scanner.Scan() {
		line := scanner.Text()

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
