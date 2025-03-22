package main

import (
	client "github.com/grove/clients/go"
)

func main() {
	cl, err := client.Connect("localhost")
	if err != nil {
		panic(err)
	}
	defer cl.Close()
}
