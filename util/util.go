package util

import (
	"log"
	"strconv"
	"strings"
)

func extractLineCount(str string) int {
	elems := strings.Split(str, ":")
	countVal := strings.Trim(elems[len(elems)-1], " \r\n")
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
	
	return ret
}