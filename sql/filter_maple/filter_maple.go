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


/*

Maple (line):
if line matches regex:
	output (1, line)

*/
func main() {
	log.SetOutput(os.Stderr)
	homedir, _ := os.UserHomeDir()
	nodeManagerFileDir := homedir + "/mr_node_manager/"

	// define flags
	regexFlag := flag.String("E", "", "Regular expression")
	inputFileFlag := flag.String("in", "", "Input filename")
	prefixFlag := flag.String("prefix", "", "SDFS intermediate filename prefix")
	flag.Parse()

	// check if required flags are provided
	if *regexFlag == "" || *inputFileFlag == "" || *prefixFlag == "" {
		log.Fatal("Usage: go run filter_maple.go -E <regex> -in <inputfile> -prefix <sdfs_intermediate_filename_prefix>")
		return
	}

	// compile the regular expression
	regexpPattern, err := regexp.Compile(*regexFlag)
	if err != nil {
		log.Fatal("Error compiling regular expression:", err)
		return
	}

	output := make(map[string][]string)

	file, err := os.Open(*inputFileFlag)
	if err != nil {
		log.Fatal("Error opening input file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		// check if the line matches the regular expression
		if regexpPattern.MatchString(line) {
			output["1"] = append(output["1"], line)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("Error reading input file:", err)
		return
	}

	outputFiles := []string{}
	// write the value for each key to a file
	for key, values := range output {
		outputFileName := fmt.Sprintf("%s-%s.txt", *prefixFlag, key)
		outputFile, err := os.Create(nodeManagerFileDir + outputFileName)
		if err != nil {
			log.Fatal("Error creating output file:", err)
			continue
		}
		defer outputFile.Close()

		for _, value := range values {
			_, err := outputFile.WriteString(value + "\n")
			if err != nil {
				log.Fatal("Error writing to output file:", err)
				continue
			}
		}

		outputFiles = append(outputFiles, outputFileName)
	}

	fmt.Println(strings.Join(outputFiles, ","))
}
