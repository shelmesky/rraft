package raft

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type KVStore struct {
	HttpBind string
	RaftDir  string
	RaftBind string
	mu       sync.Mutex
	m        map[string]string
	raft     *Raft
}

func NewRaftServer(raftDir, raftBind, httpBind string) *KVStore {
	return &KVStore{
		HttpBind: httpBind,
		RaftDir:  raftDir,
		RaftBind: raftBind,
		m:        make(map[string]string),
	}
}

func (s *KVStore) Open(localID ServerID) error {
	config := DefaultConfig()

	transport, err := NewTCPTransport(localID, s.RaftBind)
	if err != nil {
		return err
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDb, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	logStore = boltDb
	stableStore = boltDb

	localAddr := ServerAddress(s.RaftBind)

	ra, err := NewRaft(config, (*fsm)(s), logStore, stableStore,
		localID, localAddr, transport)

	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	s.raft = ra

	http.HandleFunc("/get", s.HandleGet)
	http.HandleFunc("/set", s.HandleSet)

	return nil
}

func (s *KVStore) Run() error {
	return http.ListenAndServe(s.HttpBind, nil)
}

func (s *KVStore) HandleSet(w http.ResponseWriter, req *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(req.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for k, v := range m {
		if err := s.Set(k, v); err != nil {
			log.Println("Set key value to store failed:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

func (s *KVStore) HandleGet(w http.ResponseWriter, req *http.Request) {

}

// Get returns the value for the given key.
func (s *KVStore) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.
func (s *KVStore) Set(key, value string) error {
	if s.raft.GetState() != Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	raftTimeout := 10 * time.Second
	f := s.raft.Apply(b, raftTimeout)
	return f.err
}

type fsm KVStore

func (f *fsm) Apply(log *Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		f.mu.Lock()
		defer f.mu.Unlock()
		f.m[c.Key] = c.Value
		return nil
	case "delete":
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.m, c.Key)
		return nil
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}
