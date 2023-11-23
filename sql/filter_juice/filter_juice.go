package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"fmt"
)

/*

Juice (key, value):
	identity

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
		log.Fatal("Usage: go run filter_juice.go -in <inputfile> -dest <sdfs_dest_filename>")
	}

	// for filter, the juice phase is simply an identity function
	// so copy the input file directly
	cmd := exec.Command("cp", *inputFileFlag, nodeManagerFileDir + *outputFileFlag)
	err := cmd.Run()
	if err != nil {
		log.Fatal("Error copying file:", err)
	}

	// print the output filename to stdout
	fmt.Println(*outputFileFlag)
}