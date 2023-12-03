package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

/*

Juice (key, (detection, count))):
	compute the sum of count
	for each (detection, count)
		output(detection, count/sum)

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

	scanner := bufio.NewScanner(file)
	if err := scanner.Err(); err != nil {
		log.Fatal("Error reading input file:", err)
	}

	// write output to file
	outputFile, err := os.Create(nodeManagerFileDir + *outputFileFlag)
	if err != nil {
		log.Fatal("Error creating output file:", err)
	}
	defer outputFile.Close()

	// process each line and get the count of records
	sum := 0

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
		splitted := strings.Split(line, ",")
		fmt.Println(splitted)
		if len(splitted) != 2 {
			log.Fatal("Wrong input format")
		}
		countStr := strings.TrimSpace(splitted[1])
		count, err := strconv.Atoi(countStr)
		if err != nil {
			log.Fatal("Wrong input format", err)
		}
		sum += count
	}

	file.Close()

	file, _ = os.Open(*inputFileFlag)
	defer file.Close()
	scanner2 := bufio.NewScanner(file)

	for scanner2.Scan() {
		line := scanner2.Text()
		splitted := strings.Split(line, ",")
		countStr := strings.TrimSpace(splitted[1])
		count, _ := strconv.Atoi(countStr)
		percentage := float64(count) / float64(sum) * 100
		outputFile.WriteString(fmt.Sprintf("%s, %f%%\n", splitted[0], percentage))
	}

	fmt.Println(*outputFileFlag)
	os.Exit(0)
}
