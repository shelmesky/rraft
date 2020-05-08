package main

import (
	"fmt"
	"github.com/shelmesky/rraft/raft"
)

func main() {
	fmt.Println("RRaft server starting")
	storeServer := raft.NewRaftServer("./node1", "0.0.0.0:11000", "0.0.0.0:11001")
	err := storeServer.Open("node1")
	if err != nil {
		fmt.Println("Can not start RRaft server:", err)
		return
	}
	err = storeServer.Run()
	if err != nil {
		panic(err)
	}
}
