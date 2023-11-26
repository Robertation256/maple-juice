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

an example to help understand the juice task:

query is join on d1.name = d2.id

key for the current task is test
content of the input file: (values)
d1 @ test, 18, freshman
d2 @ test, Illinois, US
d2 @ test, Pairs, France

if the column to join on is unique, the input file should only have two lines, one from d1 and one
from d2. 
if it's not unique, multiple lines from each dataset will appear. the part before @ is the dataset
name and is used to help distinguish lines in this case

Juice (key, values):
	- separate values into two collections d1 and d2 based on the dataset name specified before @
	- for i in d1:
		for j in d2:
			output(i + "," +  j)

so for the example input above, the generated output will be
test, 18, freshman, test, Illinois, US
test, 18, freshman, test, Pairs, France

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


	// map each line to the dataset it's in
	datasetToLines := make(map[string][]string)

	// Read input file
	file, err := os.Open(*inputFileFlag)
	if err != nil {
		log.Fatal("Error opening input file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// process each line and populate datasetToLines
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "@", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid line format: %s", line)
		}
		dataset := strings.TrimSpace(parts[0])
		content := strings.TrimSpace(parts[1])
		datasetToLines[dataset] = append(datasetToLines[dataset], content)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("Error reading input file:", err)
	}

	// check if there are exactly two keys (datasets) in datasetToLines
	if len(datasetToLines) != 2 {
		log.Fatal("There should be exactly two datasets in the input file")
	}

	var keys []string
	for key := range datasetToLines {
		keys = append(keys, key)
	}
	d1, d2 := keys[0], keys[1]

	// write output to file
	outputFile, err := os.Create(nodeManagerFileDir + *outputFileFlag)
	if err != nil {
		log.Fatal("Error creating output file:", err)
	}
	defer outputFile.Close()

	// combine lines from d1 and d2
	for _, i1 := range datasetToLines[d1] {
		for _, i2 := range datasetToLines[d2] {
			_, err := outputFile.WriteString(i1 + ", " + i2 + "\n")
			if err != nil {
				log.Fatal("Error writing to output file:", err)
			}
		}
	}

	fmt.Println(*outputFileFlag)
	os.Exit(0)
}
