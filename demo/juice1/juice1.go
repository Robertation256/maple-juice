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

Juice (key, value):
	output(key, |value|)

*/

func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	inputFileFlag := flag.String("in", "", "Input filename")
	outputFileFlag := flag.String("dest", "", "Output filename")
	flag.Parse()

	// check if required flags are provided
	if *inputFileFlag == "" || *outputFileFlag == "" {
		log.Fatal("Usage: go run join_juice.go -in <inputfile> -dest <outputfile>")
	}

	// Read input file
	file, err := os.Open(*inputFileFlag)
	if err != nil {
		log.Fatal("Error opening input file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		log.Fatal("Error reading input file:", err)
	}

	splittedInputFilename := strings.Split(*inputFileFlag, "-")
	key := splittedInputFilename[len(splittedInputFilename)-1]

	// write output to file
	outputFile, err := os.Create(nodeManagerFileDir + *outputFileFlag)
	if err != nil {
		log.Fatal("Error creating output file:", err)
	}
	defer outputFile.Close()

	// process each line and get the count of records
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "1" {
			count += 1
		}
	}

	outputFile.WriteString(fmt.Sprintf("%s, %d\n", key, count))

	fmt.Println(*outputFileFlag)
	os.Exit(0)
}
