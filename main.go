package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

type GrepService struct{}

func (m *GrepService) PrintInput(args *Args, reply *string) error {
	*reply = "user entered " + args.Input
	return nil
}

type Args struct {
	Input string
}

func main() {
	grepService := new(GrepService)
	rpc.Register(grepService)
	rpc.HandleHTTP()
	fmt.Println("HTTP-RPC server is listening on :8080")

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("listen error:", err)
	}
	go http.Serve(l, nil)

	for {
		fmt.Println("enter something")
		var input string
		fmt.Scanln(&input)

		client, err := rpc.DialHTTP("tcp", "localhost:8080")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer client.Close()

		args := Args{Input: input}
		var result string

		err = client.Call("GrepService.PrintInput", args, &result)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("Result: %s\n", result)
	}
}
