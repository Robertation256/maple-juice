package util

import (
	"log"
	"strconv"
	"strings"
)

// extract line count from the output of a single server
func extractLineCount(str string) int32 {
	values := strings.Split(str, ":")
	if(len(values) < 2){
		return 0
	}
	countVal := strings.Trim(values[1], " \r\n")
	ret, err := strconv.Atoi(countVal)
	if(err != nil){
		log.Fatal(err)
	}
	return int32(ret);
}


// verify and parse command line input into a list of cmd args
func parseUserInput(input string) []string {
	containsRequiredFlag := false
	ret := make([]string,0)

	if(len(input) < 5 || input[:4] != "grep"){
		log.Fatal("Invalid command")
	}
	for  i := 4 ; i<len(input)-1; {
		if input[i]=='-' {	// an option
			if(input[i+1] == 'c'){
				containsRequiredFlag = true
			}
			ret = append(ret, "-"+ string(input[i+1]))
			i += 2
		} else if input[i]!=' ' {	// a pattern
			ret = append(ret, input[i:])
			break;
		} else {
			i++
		}
	}

	if(!containsRequiredFlag){
		log.Fatal("Grep command must carry -c option.")
	}

	
	for j := 0; j < len(ret); j++ {
		ret[j] = strings.Trim(ret[j], " \n\r")
	} 

	pattern := ret[len(ret)-1]
	if(len(pattern)>1 && pattern[0]=='"' && pattern[len(pattern)-1]=='"'){
		ret[len(ret)-1] = pattern[1:len(pattern)-1]	// strip enclosing quotes
	}
	
	return ret
}
