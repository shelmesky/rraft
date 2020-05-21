package main

import (
	"flag"
	"fmt"
	"github.com/shelmesky/rraft/raft"
	"os"
	"os/signal"
)

const (
	DefaultHTTPAddr = "0.0.0.0:11000"
	DefaultRaftAddr = "0.0.0.0:11001"
)

var (
	httpAddr string
	raftAddr string
	nodeID   string
)

func init() {
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&nodeID, "id", "", "Node ID")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if nodeID == "" {
		fmt.Fprintf(os.Stderr, "No Node ID specified\n")
		os.Exit(1)
	}

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	raftDir := flag.Arg(0)
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	if err := os.MkdirAll(raftDir, 0700); err != nil {
		fmt.Fprintf(os.Stderr, "Make Raft storage directory faild: %s\n", err)
	}

	fmt.Println("RRaft server starting")
	storeServer := raft.NewRaftServer(raftDir, raftAddr, httpAddr)

	err := storeServer.Open(nodeID)
	if err != nil {
		fmt.Println("Can not start RRaft server:", err)
		return
	}

	storeServer.Run()

	fmt.Println("RRaft started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	fmt.Println("RRaft exiting")
}
