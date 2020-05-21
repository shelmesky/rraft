package raft

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"net/http"
	"os"
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

// 创建一个KV存储服务
func (s *KVStore) Open(localID string) error {
	config := DefaultConfig() // 默认Raft配置

	// TCP传输层
	transport, err := NewTCPTransport(ServerID(localID), s.RaftBind)
	if err != nil {
		return err
	}

	// 持久化的日志存储和Raft属性存储
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDb, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	logStore = boltDb
	stableStore = boltDb

	localAddr := ServerAddress(s.RaftBind)

	// 创建新的Raft实例
	ra, err := NewRaft(config, (*fsm)(s), logStore, stableStore,
		localID, localAddr, transport)

	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	s.raft = ra

	return nil
}

func (s *KVStore) Run() {
	go func() {
		r := mux.NewRouter()
		r.HandleFunc("/get/{key}", s.HandleGet)
		r.HandleFunc("/set", s.HandleSet)

		err := http.ListenAndServe(s.HttpBind, r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "HTTP server start failed: %s\n", err)
		}
	}()
}

// SET命令
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
	vars := mux.Vars(req)

	key := vars["key"]

	if key == "" {
		w.WriteHeader(http.StatusBadRequest)
	}

	v, err := s.Get(key)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(map[string]string{key: v})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	io.WriteString(w, string(b))
}

// Get函数根据key返回value
func (s *KVStore) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set函数根据key设置value
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
	fmt.Println("apply logs to leader, wait response...")
	return f.Error()
}

// FSM提供Apply函数
type fsm KVStore

func (f *fsm) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	f.raft.logger.Info("FSM receive command:", c)

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
