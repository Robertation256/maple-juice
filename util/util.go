package util

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

func Prompt(title string, ret *string, isValidInput func(string) bool) {
	var input string
	for {
		fmt.Println(title)
		fmt.Scanln(&input)
		if isValidInput(input) {
			*ret = input
			fmt.Println()
			return
		}
		fmt.Println("Invalid input, please try again.")
	}
}

func IsValidAddress(in string) bool {
	elems := strings.Split(in, ":")
	if len(elems) != 2 {
		return false
	}
	return IsValidIp(elems[0]) && IsValidPort(elems[1])
}

func IsValidPort(in string) bool {
	n, err := strconv.Atoi(in)
	return err == nil && n >= 0 && n <= 65535
}

func IsValidDropRate(in string) bool {
	n, err := strconv.ParseFloat(in, 64)
	return err == nil && n >= 0 && n <= 1
}

func IsValidIp(in string) bool {
	elems := strings.Split(in, ".")
	if len(elems) != 4 {
		return false
	}
	for _, e := range elems {
		n, err := strconv.Atoi(e)
		if err != nil || n < 0 || n > 255 {
			return false
		}
	}
	return true
}

func getOutboundIp() [4]uint8 {
	var ret [4]uint8
	conn, _ := net.Dial("udp4", "8.8.8.8:80")
	defer conn.Close()
	ip := strings.Split(conn.LocalAddr().String(), ":")[0]

	elems := strings.Split(ip, ".")
	for i := range ret {
		v, err := strconv.Atoi(elems[i])
		if err != nil {
			log.Println("Error parsing outbound ip address", err)
		}
		ret[i] = uint8(v)
	}
	return ret
}

func ExtractLineCount(str string) (int32, error) {
	str = strings.Trim(str, " \r\n")
	if len(str) == 0 {
		return 0, nil
	}
	values := strings.Split(str, ":")
	if len(values) < 2 {
		return -1, errors.New("Incorrect input format")
	}
	countVal := strings.Trim(values[1], " \r\n")
	ret, err := strconv.Atoi(countVal)
	if err != nil {
		return -1, err
	}
	return int32(ret), nil
}
