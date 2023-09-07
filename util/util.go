package util

import (
	"log"
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
			return ret
		} else {
			i++
		}
	}
	
	return ret
}