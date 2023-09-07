package util

import (
	"log"
	"strings"
)



func parseUserInput(input string) []string {	// parse out the options and the pattern
	ret := make([]string,0)
	if(len(input) < 5 || input[:4] != "grep"){
		log.Fatal("Invalid option")
	}
	for  i := 4 ; i<len(input)-1; {
		if input[i]=='-' {
			ret = append(ret, "-"+ string(input[i+1]))
			i += 2
		} else if input[i]!=' ' {
			ret = append(ret, input[i:])
			break;
		} else {
			i++
		}
	}

	for j := 0; j < len(ret); j++ {
		ret[j] = strings.Trim(ret[j], " \n\r")
	} 
	
	return ret
}