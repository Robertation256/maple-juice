package util

import (
	"log"
	"strconv"
	"strings"
)

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
	return ret;
}


func parseUserInput(input string) []string {	// parse out the options and the pattern
	containsRequiredFlag := false

	ret := make([]string,0)
	if(len(input) < 5 || input[:4] != "grep"){
		log.Fatal("Invalid option")
	}
	for  i := 4 ; i<len(input)-1; {
		if input[i]=='-' {
			if(input[i+1] == 'c'){
				containsRequiredFlag = true
			}
			ret = append(ret, "-"+ string(input[i+1]))
			i += 2
		} else if input[i]!=' ' {
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
