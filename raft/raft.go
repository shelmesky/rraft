package raft

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"log"
	"math"
	"math/big"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type LogType uint8

const (
	LogCommand LogType = iota
	LogNoop
	LogConfiguration
)

var (
	KeyCurrentTerm  = []byte("CurrentTerm")
	KeyLastVoteTerm = []byte("LastVoteTerm")
	KeyLastVoteCand = []byte("LastVoteCand")
)

func init() {
	// Ensure we use a high-entropy seed for the pseudo-random generator
	rand.Seed(newSeed())
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

type ServerID string
type ServerAddress string

type ServerSuffrage int

const (
	Voter ServerSuffrage = iota
	Nonvoter
	Staging
)

type Server struct {
	Suffrage ServerSuffrage
	ID       ServerID
	Address  ServerAddress
}

type Configuration struct {
	Server []Server
}

type Raft struct {
	currentTerm uint64 // 当前的Term，持久化存储
	commitIndex uint64 // 当前commit的index(leader的commit index总是领先于follower)
	lastApplied uint64 // 最后被应用到FSM的index

	configurations Configuration

	lastLock sync.Mutex
	/*
		1. 启动时从本地日恢复最后日志，设置下面两个字段
		2. follower从leader收到检查并处理后，也设置下面两个字段
	*/
	lastLogIndex uint64
	lastLogTerm  uint64

	localID   ServerID
	localAddr ServerAddress

	followerNextIndex  map[ServerAddress]uint64
	followerMatchIndex map[ServerAddress]uint64

	logger hclog.Logger

	/*
		1. logs: 存储和读取日志
		2. stable: 存储一些需要持久化的字段，如currentTerm
	*/
	logs   raft.LogStore
	stable raft.StableStore

	// 当前的Leader
	leader     ServerAddress
	leaderLock sync.RWMutex

	// 作为follower指示leader最后和自己联系的时间
	lastContact     time.Time
	lastContactLock sync.RWMutex

	trans Transport

	// 用于关闭Raft的channel和锁
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	rpcCh   <-chan RPC      // 从transport接收RPC请求并处理
	applyCh chan *LogFuture // 调用ApplyLog把日志发送给Leader处理

	fsm         FSM              // 状态机，日志commit之后调用状态机的Apply处理
	fsmMutateCh chan interface{} // 将日志发送到FSM处理

	config *Config // 配置参数

	routinesGroup sync.WaitGroup
	state         uint32
}

func (r *Raft) GetState() uint32 {
	stateAddr := (*uint32)(&r.state)
	return atomic.LoadUint32(stateAddr)
}

func (r *Raft) SetState(s uint32) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, s)
}

func (r *Raft) GetCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *Raft) SetCurrentTerm(term uint64) {
	err := r.stable.SetUint64(KeyCurrentTerm, term)
	if err != nil {
		panic(fmt.Errorf("failed to save current term: %v", err))
	}
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *Raft) GetLastLog() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

func (r *Raft) SetLastLog(index, term uint64) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
	return
}

func (r *Raft) GetLastIndex() uint64 {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	return r.lastLogIndex
}

func (r *Raft) GetCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *Raft) SetCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *Raft) GetLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *Raft) SetLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}

func (r *Raft) SetLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) *LogFuture {
	return r.ApplyLog(Log{Data: cmd}, timeout)
}

func (r *Raft) ApplyLog(log Log, timeout time.Duration) *LogFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	logFuture := &LogFuture{
		log: Log{
			Type:       LogCommand,
			Data:       log.Data,
			Extensions: log.Extensions,
		},
	}

	select {
	case <-timer:
		return &LogFuture{
			err: fmt.Errorf("ApplyLog timeout!"),
		}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

type Config struct {
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	CommitTimeout    time.Duration

	LogOutput io.Writer
	LogLevel  string
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 1000 * time.Millisecond,
		ElectionTimeout:  1000 * time.Millisecond,
		CommitTimeout:    50 * time.Millisecond,
		LogOutput:        os.Stderr,
		LogLevel:         "DEBUG",
	}
}

type Log struct {
	Index      uint64
	Term       uint64
	Type       LogType
	Data       []byte
	Extensions []byte
}

type LogFuture struct {
	log Log
	err error
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
type LogStore interface {
	// FirstIndex returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	GetLog(index uint64, log *Log) error

	// StoreLog stores a log entry.
	StoreLog(log *Log) error

	// StoreLogs stores multiple log entries.
	StoreLogs(logs []*Log) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
	DeleteRange(min, max uint64) error
}

type StableStore interface {
	Set(key []byte, val []byte) error

	// Get returns the value for key, or an empty byte slice if key was not found.
	Get(key []byte) ([]byte, error)

	SetUint64(key []byte, val uint64) error

	// GetUint64 returns the uint64 value for key, or 0 if key was not found.
	GetUint64(key []byte) (uint64, error)
}

type FSM interface {
	Apply(*raft.Log) interface{}
}

// AE请求
type AppendEntriesRPCRequest struct {
	term         uint64
	leaderId     string
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []*raft.Log
	leaderCommit uint64
}

// AE响应
type AppendEntriesRPCResponse struct {
	lastLog        uint64 // follower本地日志index
	term           uint64 // follower本地的term
	success        bool   // 是否成功写入到日志
	noRetryBackoff bool   // 是否需要尝试并匹配日志，不需要则leader直接从头发送
}

type RequestVoteRPCRequest struct {
	term         uint64
	candidateID  string
	lastLogIndex uint64
	lastLogTerm  uint64
}

type RequestVoteRPCResponse struct {
	term        uint64
	voteGranted bool
}

func (r *Raft) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func NewRaft(config *Config, fsm FSM, logs raft.LogStore,
	stable raft.StableStore, localID ServerID,
	localAddr ServerAddress, trans Transport) (*Raft, error) {

	currentTerm, err := stable.GetUint64(KeyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, fmt.Errorf("failed to load current term: %v", err)
	}

	lastIndex, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	var lastLog raft.Log
	if lastIndex > 0 {
		if err = logs.GetLog(lastIndex, &lastLog); err != nil {
			return nil, fmt.Errorf("failed to get last log at index %d: %v", lastIndex, err)
		}
	}

	var logger hclog.Logger
	logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.LevelFromString(config.LogLevel),
		Output: config.LogOutput,
	})

	r := &Raft{
		localID:   localID,
		localAddr: localAddr,
		logger:    logger,
		logs:      logs,
		stable:    stable,
		applyCh:   make(chan *LogFuture),
		config:    config,
		fsm:       fsm,
		trans:     trans,
	}

	r.rpcCh = trans.Consumer()

	trans.SetHeartbeatHandler(r.processHeartbeat)

	r.SetState(Follower)

	r.SetCurrentTerm(currentTerm)
	r.SetLastLog(lastLog.Index, lastLog.Term)

	r.goFunc(r.run)
	r.goFunc(r.runFSM)

	return r, nil
}

func (r *Raft) Leader() ServerAddress {
	r.leaderLock.RLock()
	leader := r.leader
	r.leaderLock.RUnlock()
	return leader
}

func (r *Raft) SetLeader(leader ServerAddress) {
	r.leaderLock.Lock()
	r.leader = leader
	r.leaderLock.Unlock()
}

func (r *Raft) run() {
	for {
		select {
		case <-r.shutdownCh:
			r.SetLeader("")
			return
		default:
		}

		switch r.GetState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}

func (r *Raft) runFSM() {
	for {
		select {
		case ptr := <-r.fsmMutateCh:
			switch req := ptr.(type) {
			case *raft.Log:
				r.fsm.Apply(req)
			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runFollower() {
	r.logger.Info("entering follower state", "follower", r, "leader", r.Leader())
	heartbeatTimer := randomTimeout(r.config.HeartbeatTimeout)
	for r.GetState() == Follower {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case <-heartbeatTimer:
			heartbeatTimer = randomTimeout(r.config.HeartbeatTimeout)

			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) < r.config.HeartbeatTimeout {
				continue
			}

			lastLeader := r.Leader()
			r.SetLeader("")

			r.logger.Warn("heartbeat timeout reached, starting election", "last-leader", lastLeader)
			r.SetState(Candidate)
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) processRPC(rpc RPC) {
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRPCRequest:
		r.appendEntries(rpc, cmd)
	case *RequestVoteRPCRequest:
		r.requestVote(rpc, cmd)
	default:
		r.logger.Error("got unexpected command",
			"command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRPCRequest) {
	resp := &AppendEntriesRPCResponse{
		term:           r.GetCurrentTerm(),
		lastLog:        r.GetLastIndex(),
		success:        false,
		noRetryBackoff: false,
	}

	var rpcErr error

	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// 如果AE请求中的term小于本地的，则直接返回
	if a.term < r.GetCurrentTerm() {
		return
	}

	/*
		因为不止Follower会处理AE请求，Candidate也会处理，所以如果是Candidate状态
		并收到了更大term的AE请求，则进入Follower状态并设置更大的Term.
	*/
	if a.term > r.GetCurrentTerm() || r.GetState() != Follower {
		r.SetState(Follower)
		r.SetCurrentTerm(a.term)
		resp.term = a.term
	}

	// 不论任何状态, 任何时候都设置leader为最新的leader
	r.SetLeader(ServerAddress(a.leaderId))

	if a.prevLogIndex > 0 {
		lastIdx, lastTerm := r.GetLastLog()

		var prevLogTerm uint64

		// leader发来的日志和本地index相等
		if a.prevLogIndex == lastIdx {
			prevLogTerm = lastTerm
		} else {
			// leader发来的日志和本地在本地查询不到
			var prevLog raft.Log
			err := r.logs.GetLog(a.prevLogIndex, &prevLog)
			if err != nil {
				// leader应该直接从头开始发送
				resp.noRetryBackoff = true
				return
			}

			// 在本地查到了leader发来的日志
			prevLogTerm = prevLog.Term
		}

		/*
			上面的代码使用以下两种方式获取term:
			1. 使用本地最后日志的term
			2. 使用之前本地日志的term
			获取到的term和AE请求中的term对比，
			不匹配则leader应该从头发送日志．
		*/
		if a.prevLogTerm != prevLogTerm {
			resp.noRetryBackoff = true
			return
		}

		// 循环处理每一条日志
		if len(a.entries) > 0 {
			lastLogIdx, _ := r.GetLastLog()

			var newEntries []*raft.Log

			for i, entry := range a.entries {
				if entry.Index > lastLogIdx {
					newEntries = a.entries[i:]
					break
				}

				var storeEntry raft.Log
				err := r.logs.GetLog(entry.Index, &storeEntry)
				if err != nil {
					return
				}

				if entry.Term != storeEntry.Term {
					err := r.logs.DeleteRange(entry.Index, lastLogIdx)
					if err != nil {
						return
					}

					newEntries = a.entries[i:]
					break
				}
			}

			if n := len(newEntries); n > 0 {
				// 保存所有应该保存的日志项
				err := r.logs.StoreLogs(newEntries)
				if err != nil {
					return
				}

				if len(newEntries) > 0 {
					last := newEntries[n-1]
					// 设置lastLogIndex和lastTerm
					r.SetLastLog(last.Index, last.Term)
				}
			}
		}

		// 如果leader的commit index大于本地的commit index
		if a.leaderCommit > 0 && a.leaderCommit > r.GetCommitIndex() {
			// leader的commit index可能大于本地last log index，也可能小于
			// 如果leader的commit index打，说明本地比leader差了很多日志，应该使用本地刚写入磁盘的日志作为commit index
			// 否则直接使用leader的commit index
			idx := min(a.leaderCommit, r.GetLastIndex())
			r.SetCommitIndex(idx)
			// 将直到commit index的日志项，都应用到FSM中
			r.processLogs(idx, nil)
		}

		resp.success = true
		r.SetLastContact()
	}
}

func (r *Raft) requestVote(rpc RPC, req *RequestVoteRPCRequest) {
	// 初始化一个投票响应
	resp := &RequestVoteRPCResponse{
		term:        r.GetCurrentTerm(),
		voteGranted: false,
	}

	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	candidateID := req.candidateID
	leader := r.Leader()
	// 如果当前存在leader并且leader不是本机
	if leader != "" && leader != ServerAddress(candidateID) {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from", candidateID,
			"leader", leader)
		return
	}

	// 如果投票请求的term小于本机，则拒绝投票
	if req.term < r.GetCurrentTerm() {
		return
	}

	// 如果请求的term大于本机，说明有新leader，则从candidate进入follower状态
	// 并设置term
	if req.term > r.GetCurrentTerm() {
		r.logger.Debug("lost leadership because received a requestVote with a newer term")
		r.SetState(Follower)
		r.SetCurrentTerm(req.term)
		resp.term = req.term
	}

	// 从磁盘获取最后投票的term和candidate信息
	lastVoteTerm, err := r.stable.GetUint64(KeyLastVoteTerm)
	if err != nil {
		r.logger.Error("failed to get last vote term", "error", err)
		return
	}
	lastVoteCandidateBytes, err := r.stable.Get(KeyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.logger.Error("failed to get last vote candidate", "error", err)
		return
	}

	// 如果保存在磁盘上的最近投票term和Candidate都不为空。
	// 如果candidate信息和请求中的一样，说明已经为这个candidate投了一票，但它没有收到投票结果
	// 所以就再投一次票。
	// 但如果只是term相同，就说明已经为其他candidate投票，不能再次投票
	if lastVoteTerm == req.term && lastVoteCandidateBytes != nil {
		r.logger.Info("duplicate requestVote for same term", "term", req.term)
		if bytes.Compare(lastVoteCandidateBytes, []byte(req.candidateID)) == 0 {
			r.logger.Warn("duplicate requestVote from", "candidate", req.candidateID)
			resp.voteGranted = true
		}
		return
	}

	// 如果请求的term小于本地term则拒绝投赞成票。
	// 如果请求的term等于本地term，但请求的index小于本地的也拒绝投赞成票。
	lastIdx, lastTerm := r.GetLastLog()
	if lastTerm > req.lastLogTerm {
		r.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidateID,
			"last-term", lastTerm,
			"last-candidate-term", req.lastLogTerm)
		return
	}
	if lastTerm == req.lastLogTerm && lastIdx > req.lastLogIndex {
		r.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidateID,
			"last-index", lastIdx,
			"last-candidate-index", req.lastLogIndex)
		return
	}

	// 投票之前将term和candidate信息持久化（不能重复投票）
	err = r.persistVote(req.term, req.candidateID)
	if err != nil {
		r.logger.Error("failed to persist vote", "error", err)
		return
	}

	// 投赞成票
	resp.voteGranted = true
	r.SetLastContact()
	return
}

// 将日志应用到FSM
func (r *Raft) processLogs(index uint64, futures map[uint64]*LogFuture) {
	lastApplied := r.GetLastApplied()

	if index <= lastApplied {
		r.logger.Warn("skipping application of old log", "index", index)
		return
	}

	for idx := lastApplied + 1; idx <= index; idx++ {
		l := new(raft.Log)
		err := r.logs.GetLog(idx, l)
		if err != nil {
			r.logger.Error("failed to get log", "index", idx, "error", err)
			panic(err)
		}

		r.fsmMutateCh <- l
	}

}

func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

func (r *Raft) processHeartbeat(rcp RPC) {

}

// 法定人数的数量是超过半数的节点
func (r *Raft) quorumSize() int {
	voters := len(r.configurations.Server)
	return voters/2 + 1
}

type voteResult struct {
	RequestVoteRPCResponse
	voterID ServerID
}

// candidate给自身投票，并向集群内的其他节点投票
// 返回一个channel，可以获取所有节点的投票结果
func (r *Raft) electSelf() <-chan *voteResult {
	respCh := make(chan *voteResult, len(r.configurations.Server))

	// 当前term+1，开始选举流程
	r.SetCurrentTerm(r.GetCurrentTerm() + 1)

	lastIdx, lastTerm := r.GetLastLog()
	req := &RequestVoteRPCRequest{
		term:         r.GetCurrentTerm(),
		candidateID:  string(r.localID),
		lastLogIndex: lastIdx,
		lastLogTerm:  lastTerm,
	}

	// askPeer将投票请求发送到其他节点，并将响应发送到channel中
	askPeer := func(peer Server) {
		r.goFunc(func() {
			resp := &voteResult{voterID: peer.ID}

			err := r.trans.RequestVote(string(peer.ID), peer.Address, req, &resp.RequestVoteRPCResponse)
			if err != nil {
				r.logger.Error("failed to make requestVote RPC",
					"target", peer,
					"error", err)
				resp.term = req.term
				resp.voteGranted = false
			}

			respCh <- resp
		})
	}

	// 循环处理所有节点并发送投票请求
	for _, server := range r.configurations.Server {
		if server.Suffrage == Voter {
			// 为自身投票并持久化
			if server.ID == r.localID {
				// 模拟收到投票请求并持久化
				err := r.persistVote(req.term, req.candidateID)
				if err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return nil
				}

				respCh <- &voteResult{
					RequestVoteRPCResponse: RequestVoteRPCResponse{
						term:        req.term,
						voteGranted: true,
					},
					voterID: r.localID,
				}

			} else {
				askPeer(server)
			}
		}
	}

	return respCh
}

func (r *Raft) persistVote(term uint64, candidate string) error {
	err := r.stable.SetUint64(KeyLastVoteTerm, term)
	if err != nil {
		return err
	}

	err = r.stable.Set(KeyLastVoteCand, []byte(candidate))
	if err != nil {
		return err
	}

	return nil
}

func (r *Raft) runCandidate() {
	r.logger.Info("entering candidate state", "node", r, "term", r.GetCurrentTerm()+1)

	voteCh := r.electSelf()

	electionTimer := randomTimeout(r.config.ElectionTimeout)

	grantedVote := 0
	votesNeeded := r.quorumSize()

	for r.GetState() == Candidate {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case vote := <-voteCh:
			if vote.term > r.GetCurrentTerm() {
				// 收到了比自己term高的回应，说明新leader已经选出
				// 应该进入Follower状态，并设置term为新leader的term
				r.logger.Debug("newer term discovered, fallback to follower")
				r.SetState(Follower)
				r.SetCurrentTerm(vote.term)
				return
			}

			// 统计赞成票的数量
			if vote.voteGranted {
				grantedVote++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.term, "tally", grantedVote)
			}

			// 收到了足够的赞成票，进入Leader状态
			if grantedVote >= votesNeeded {
				r.logger.Info("election won", "tally", grantedVote)
				r.SetState(Leader)
				r.SetLeader(r.localAddr)
				return
			}

		case <-electionTimer:
			// 选举超时，直接退出函数，外部的循环会继续执行本函数
			r.logger.Warn("Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runLeader() {
	log.Panicln("we are in leader...")
}
