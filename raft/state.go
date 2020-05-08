package raft

const (
	Follower = iota
	Candidate
	Leader
	Shutdown
)
