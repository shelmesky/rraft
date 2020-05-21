package raft

import (
	"bytes"
	"container/list"
	crand "crypto/rand"
	"errors"
	"fmt"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"math"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type LogType uint8

const (
	LogCommand LogType = iota
	LogNoop
	LogConfiguration

	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	KeyCurrentTerm  = []byte("CurrentTerm")
	KeyLastVoteTerm = []byte("LastVoteTerm")
	KeyLastVoteCand = []byte("LastVoteCand")

	ErrLogNotFound    = errors.New("log not found")
	ErrNotLeader      = errors.New("node is not the leader")
	ErrRaftShutdown   = errors.New("raft is already shutdown")
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")
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

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

func DefaultServers() []Server {
	serverList := make([]Server, 3)
	serverList[0].Address = "127.0.0.1:11001"
	serverList[0].ID = "node1"
	serverList[0].Suffrage = Voter

	serverList[1].Address = "127.0.0.1:21001"
	serverList[1].ID = "node2"
	serverList[1].Suffrage = Voter

	serverList[2].Address = "127.0.0.1:31001"
	serverList[2].ID = "node3"
	serverList[2].Suffrage = Voter
	return serverList
}

func DefaultConfiguration() Configuration {
	var configuration Configuration
	configuration.Server = DefaultServers()
	return configuration
}

type followerReplication struct {
	currentTerm uint64

	nextIndex uint64

	peer Server

	commitment *commitment

	/*
		leader关闭某个follower的RSM时，将最后的log index写入到这个channel，
		RSM应该在退出之前尽力而为的将日志发送．
	*/
	stopCh chan uint64

	// triggerCh用于通知有新消息添加到leader的log中了．
	triggerCh chan struct{}

	lastContact     time.Time
	lastContactLock sync.RWMutex

	// failures统计RPC最近错误的次数，用于在回退时使用．
	failures uint64

	stepDown chan struct{}

	allowPipeline bool
}

// LastContact returns the time of last contact.
func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

// setLastContact sets the last contact to the current time.
func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	matchIndexes map[ServerID]uint64
	// a quorum stores up through this log entry. monotonically increases.
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	startIndex uint64
}

func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Server {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

func (c *commitment) match(server ServerID, matchIndex uint64) {
	fmt.Printf("ServerID: %s,  matchIndex: %d\n", server, matchIndex)
	c.Lock()
	defer c.Unlock()
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

func (c *commitment) recalculate() {
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for serverID, idx := range c.matchIndexes {
		fmt.Printf("-----\n%s -> %d\n-----\n", serverID, idx)
		matched = append(matched, idx)
	}
	sort.Sort(uint64Slice(matched))
	quorumMatchIndex := matched[(len(matched)-1)/2]
	fmt.Printf("matched: %v,  quorumMatchIndex: %d,  commitIndex: %d\n\n", matched, quorumMatchIndex, c.commitIndex)

	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		asyncNotifyCh(c.commitCh)
	}
}

// Called by leader after commitCh is notified
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// LeaderState是当我们成为领导者时使用的状态。
type leaderState struct {
	leadershipTransferInProgress int32 // indicates that a leadership transfer is in progress.
	commitCh                     chan struct{}
	commitment                   *commitment

	// leader发送log时，同时保存一份日志到inflight，
	// 等收到log被follower提交的channel通知，就从inflight中取出log，
	// 最后对每个log设置错误信息，等待接收结果的ApplyLog就能得到错误信息。
	inflight *list.List // list of logFuture in log index order

	replState map[ServerID]*followerReplication

	// stepDown初始化时赋值给每个RSM，它们在复制日志时，
	// 如果发生需要降级到follower的情况，就向这个channel发送消息，
	// 在leaderLoop中读取这个channel，做对应的降级动作。
	stepDown chan struct{}
}

type Raft struct {
	currentTerm uint64 // 当前的Term，持久化存储
	commitIndex uint64 // 当前commit的index(leader的commit index总是领先于follower)
	lastApplied uint64 // 最后被应用到FSM的index

	// 集群配置
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

	leaderState leaderState

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

func (r *Raft) String() string {
	return fmt.Sprintf("Node at %s [%v]", r.localAddr, stateAsString(r.GetState()))
}

func (r *Raft) GetState() uint32 {
	stateAddr := (*uint32)(&r.state)
	return atomic.LoadUint32(stateAddr)
}

func stateAsString(state uint32) string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "UnKnow State"
	}
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

func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	return r.ApplyLog(Log{Data: cmd}, timeout)
}

func (r *Raft) ApplyLog(log Log, timeout time.Duration) ApplyFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	logFuture := &LogFuture{
		log: raft.Log{
			Type:       raft.LogCommand,
			Data:       log.Data,
			Extensions: log.Extensions,
		},
	}

	logFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
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

	MaxAppendEntries int
}

func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 10000 * time.Millisecond,
		ElectionTimeout:  1000 * time.Millisecond,
		CommitTimeout:    50 * time.Millisecond,
		LogOutput:        os.Stderr,
		LogLevel:         "DEBUG",
		MaxAppendEntries: 64,
	}
}

type Log struct {
	Index      uint64
	Term       uint64
	Type       LogType
	Data       []byte
	Extensions []byte
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

// 使用新的goroutine启动一个函数
func (r *Raft) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

/*
config: 全局配置
fsm: 有限状态机
logs: 日志持久存储
stable: 存储Raft协议中需要持久话的字段
localID: 当前节点ID
localAddr: 当前节点Raft协议监听地址
trans: 传输层的抽象接口
*/
func NewRaft(config *Config, fsm FSM, logs raft.LogStore,
	stable raft.StableStore, localID string,
	localAddr ServerAddress, trans Transport) (*Raft, error) {

	// 从持久化存储中活动currentTerm字段
	currentTerm, err := stable.GetUint64(KeyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, fmt.Errorf("failed to load current term: %v", err)
	}

	// 从日志存储中获取最后的日志Index
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	// 尝试获取最后的日志项内容
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

	// 初始化Raft结构
	r := &Raft{
		localID:     ServerID(localID),
		localAddr:   localAddr,
		logger:      logger,
		logs:        logs,
		stable:      stable,
		applyCh:     make(chan *LogFuture),
		config:      config,
		fsm:         fsm,
		fsmMutateCh: make(chan interface{}, 128),
		trans:       trans,
	}

	// 默认节点配置
	r.configurations = DefaultConfiguration()

	// 使用rcpCh从传输层接收RPC请求
	r.rpcCh = trans.Consumer()

	// 立即设置心跳处理函数
	trans.SetHeartbeatHandler(r.processHeartbeat)

	// 初始化状态为Follower
	r.SetState(Follower)

	// 设置内存中的currentTerm
	r.SetCurrentTerm(currentTerm)
	// 设置内存中最后日志的Index和Term
	r.SetLastLog(lastLog.Index, lastLog.Term)

	r.goFunc(r.run)    // 启动Raft协议核心
	r.goFunc(r.runFSM) // 启动有限状态机

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

// 根据当前状态运行对应的处理函数
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

// 生成随机的时间：在minVal和2倍minVal之间
func randomTimeout(minVal time.Duration) (<-chan time.Time, time.Duration) {
	if minVal == 0 {
		return nil, 0
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra), minVal + extra
}

// 循环运行有限状态机
func (r *Raft) runFSM() {
	for {
		select {
		case ptr := <-r.fsmMutateCh: // 在leader和follower中当日志确认提交时，交给FSM执行
			switch req := ptr.(type) {
			case *raft.Log:
				r.fsm.Apply(req)
			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}
		case <-r.shutdownCh: // 退出FSM
			return
		}
	}
}

// 循环运行Follower
func (r *Raft) runFollower() {
	r.logger.Info("entering follower state", "follower", r, "leader", r.Leader())

	// 生成心跳超时
	heartbeatTimer, timeout := randomTimeout(r.config.HeartbeatTimeout)
	fmt.Println("心跳超时：", timeout)
	for r.GetState() == Follower {
		select {
		// 处理RPC请求
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		// 发生了心跳超时
		case <-heartbeatTimer:
			// 再生成一个新的心跳超时，在下一次循环中用到
			heartbeatTimer, timeout = randomTimeout(r.config.HeartbeatTimeout)
			fmt.Println("心跳超时：", timeout)

			// 查看心跳是否超时
			lastContact := r.LastContact()
			fmt.Printf("检查心跳是否超时：%v < %v = %v\n", time.Now().Sub(lastContact),
				r.config.HeartbeatTimeout, time.Now().Sub(lastContact) < r.config.HeartbeatTimeout)

			if time.Now().Sub(lastContact) < r.config.HeartbeatTimeout {
				continue
			}

			// 清空当前leader
			lastLeader := r.Leader()
			r.SetLeader("")

			// 进入到Candidate状态
			r.logger.Warn("heartbeat timeout reached, starting election", "last-leader", lastLeader)
			r.SetState(Candidate)
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// 根据RPC类型处理RCP请求，在Follower/Leader/Candidate状态下都会运行此函数
func (r *Raft) processRPC(rpc RPC) {
	fmt.Printf("节点 [%s], 状态: [%s] 收到RPC请求: %v\n", r.localID, r.String(), rpc)

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRPCRequest: // 日志追加请求：从Leader发向Follower
		r.appendEntries(rpc, cmd)
	case *RequestVoteRPCRequest: // 投票请求：从Candidate发向其他任意状态节点
		r.requestVote(rpc, cmd)
	default:
		r.logger.Error("got unexpected command",
			"command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// 处理日志追加请求：任意状态下都可能收到
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRPCRequest) {
	r.logger.Info("we are in appendEntries...")

	if len(a.Entries) == 0 {
		r.logger.Info("收到心跳信号...")
	}

	resp := &AppendEntriesRPCResponse{
		Term:           r.GetCurrentTerm(),
		LastLog:        r.GetLastIndex(),
		Success:        false,
		NoRetryBackoff: false,
	}

	var rpcErr error

	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// 如果AE请求中的term小于本地的，则直接返回
	if a.Term < r.GetCurrentTerm() {
		return
	}

	/*
		因为不止Follower会处理AE请求，Candidate和Leader状态也会收到，所以如果不是Follower状态
		并收到了更大term的AE请求，则进入Follower状态并设置更大的Term.
	*/
	if a.Term > r.GetCurrentTerm() || r.GetState() != Follower {
		r.SetState(Follower)
		r.SetCurrentTerm(a.Term)
		resp.Term = a.Term
	}

	// 不论任何状态, 任何时候都设置leader为最新的leader
	r.SetLeader(ServerAddress(a.LeaderId))

	if a.PrevLogIndex > 0 {
		lastIdx, lastTerm := r.GetLastLog()

		var prevLogTerm uint64

		// leader发来的日志和本地index相等
		if a.PrevLogIndex == lastIdx {
			prevLogTerm = lastTerm
		} else {
			// leader发来的日志和本地在本地查询不到
			var prevLog raft.Log
			err := r.logs.GetLog(a.PrevLogIndex, &prevLog)
			if err != nil {
				// leader应该直接从头开始发送
				resp.NoRetryBackoff = true
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
		if a.PrevLogTerm != prevLogTerm {
			resp.NoRetryBackoff = true
			return
		}

		// 循环处理每一条日志
		if len(a.Entries) > 0 {
			lastLogIdx, _ := r.GetLastLog()

			var newEntries []*raft.Log

			for i, entry := range a.Entries {
				if entry.Index > lastLogIdx {
					newEntries = a.Entries[i:]
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

					newEntries = a.Entries[i:]
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
		if a.LeaderCommit > 0 && a.LeaderCommit > r.GetCommitIndex() {
			// leader的commit index可能大于本地last log index，也可能小于
			// 如果leader的commit index打，说明本地比leader差了很多日志，应该使用本地刚写入磁盘的日志作为commit index
			// 否则直接使用leader的commit index
			idx := min(a.LeaderCommit, r.GetLastIndex())
			r.SetCommitIndex(idx)
			// 将直到commit index的日志项，都应用到FSM中
			r.processLogs(idx, nil)
		}
	}

	fmt.Printf("更新心跳时间...\n")
	resp.Success = true
	r.SetLastContact()
}

func (r *Raft) requestVote(rpc RPC, req *RequestVoteRPCRequest) {
	// 初始化一个投票响应
	resp := &RequestVoteRPCResponse{
		Term:        r.GetCurrentTerm(),
		VoteGranted: false,
	}

	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	candidateID := req.CandidateID
	leader := r.Leader()
	// 如果当前存在leader并且leader不是本机
	if leader != "" && leader != ServerAddress(candidateID) {
		r.logger.Warn("rejecting vote request since we have a leader",
			"from", candidateID,
			"leader", leader)
		return
	}

	// 如果投票请求的term小于本机，则拒绝投票
	if req.Term < r.GetCurrentTerm() {
		return
	}

	// 如果请求的term大于本机，说明有新leader，则从candidate进入follower状态
	// 并设置term
	if req.Term > r.GetCurrentTerm() {
		r.logger.Debug("enter follower state because we received a requestVote with a newer term")
		r.SetState(Follower)
		r.SetCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// 从磁盘获取最后投票的term和candidate信息
	lastVoteTerm, err := r.stable.GetUint64(KeyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
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
	if lastVoteTerm == req.Term && lastVoteCandidateBytes != nil {
		r.logger.Info("duplicate requestVote for same term", "term", req.Term)
		if bytes.Compare(lastVoteCandidateBytes, []byte(req.CandidateID)) == 0 {
			r.logger.Warn("duplicate requestVote from", "candidate", req.CandidateID)
			resp.VoteGranted = true
		}
		return
	}

	// 如果请求的term小于本地term则拒绝投赞成票。
	// 如果请求的term等于本地term，但请求的index小于本地的也拒绝投赞成票。
	lastIdx, lastTerm := r.GetLastLog()
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidateID,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}
	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidateID,
			"last-index", lastIdx,
			"last-candidate-index", req.LastLogIndex)
		return
	}

	// 投票之前将term和candidate信息持久化（不能重复投票）
	err = r.persistVote(req.Term, req.CandidateID)
	if err != nil {
		r.logger.Error("failed to persist vote", "error", err)
		return
	}

	// 投赞成票
	resp.VoteGranted = true
	r.SetLastContact()
	return
}

// 将日志应用到FSM
func (r *Raft) processLogs(index uint64, futures map[uint64]*LogFuture) {
	fmt.Printf("processLogs: %d, futures: %v\n", index, futures)

	/*
		每次重启之后，发送Noop日志会引发最少一次提交，由于lastApplied没有持久化，
		所以每次重启之后lastApplied都是0，就导致从0开始读取本地的日志并发送到FSM.
		创建快照后，重启Raft尝试从快照恢复restoreSnapshot()可以防止每次都从0开始重复读取日志.
	*/
	lastApplied := r.GetLastApplied() // 最后应用到状态机的日志Index

	// 小于lastApplied认为是旧日志
	if index <= lastApplied {
		r.logger.Warn("skipping application of old log", "index", index)
		return
	}

	// 从lastApplied循环到index，依次处理日志
	// futures是在Leader的inflight数组中保存了ApplyLog函数生成的日志副本
	for idx := lastApplied + 1; idx <= index; idx++ {
		future, futureOk := futures[idx]
		var log *raft.Log
		if futureOk {
			// 如果在inflight中存在日志Index，则使用inflight暂存的日志副本发送给FSM
			log = &future.log
		} else {
			// 否则就根据日志Index从本地日志库中查询日志，然后发送给FSM
			l := new(raft.Log)
			err := r.logs.GetLog(idx, l)
			if err != nil {
				r.logger.Error("failed to get log", "index", idx, "error", err)
				panic(err)
			}

			log = l
		}

		// 不向FSM发送Noop命令
		if log.Type == raft.LogNoop {
			continue
		}

		r.fsmMutateCh <- log

		// 特别注意：如果在futures存在日志副本，需要给这个日志响应nil
		// 目的是让Leader回复nil给还在ApplyLog函数中等待的client
		if futureOk {
			future.respond(nil)
		}
	}

	// 每次处理一个，就设置FSM的last applied
	r.SetLastApplied(index)

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
		Term:         r.GetCurrentTerm(),
		CandidateID:  string(r.localID),
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
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
				resp.Term = req.Term
				resp.VoteGranted = false
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
				err := r.persistVote(req.Term, req.CandidateID)
				if err != nil {
					r.logger.Error("failed to persist vote", "error", err)
					return nil
				}

				respCh <- &voteResult{
					RequestVoteRPCResponse: RequestVoteRPCResponse{
						Term:        req.Term,
						VoteGranted: true,
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

// 循环运行Candidate
func (r *Raft) runCandidate() {
	r.logger.Info("entering candidate state", "node", r, "term", r.GetCurrentTerm()+1)

	// 从voteCh中可以异步读取到各个节点(包括当前节点自身)的投票响应
	voteCh := r.electSelf()

	// 生成一个随机的投票超时时间
	electionTimer, timeout := randomTimeout(r.config.ElectionTimeout)
	fmt.Println("选举超时：", timeout)

	grantedVote := 0              // 赞成票的数量
	votesNeeded := r.quorumSize() // 实际需要达到的票数(n/2-1)

	for r.GetState() == Candidate {
		select {
		// Candidate状态下可以接受其他节点的投票请求
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		// 从投票响应的channel中读取投票响应
		case vote := <-voteCh:
			if vote.Term > r.GetCurrentTerm() {
				// 收到了比自己term高的回应，说明新leader已经选出
				// 应该进入Follower状态，并设置term为新leader的term
				r.logger.Debug("newer term discovered, fallback to follower")
				r.SetState(Follower)
				r.SetCurrentTerm(vote.Term)
				return
			}

			// 统计赞成票的数量
			if vote.VoteGranted {
				grantedVote++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.Term, "tally", grantedVote)
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

// 进入Leader状态，立即初始化Leader的各种状态信息数据
func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)                  // 用于RSM发送给leader循环的已提交日志Index
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh, // RSM统计每个Follower的已提交日志Index
		r.configurations, r.GetLastIndex()+1)
	r.leaderState.inflight = list.New()                               // Leader发送日志给Follower后，临时保存日志在inflight一份副本，用于日志被提交后回复client
	r.leaderState.replState = make(map[ServerID]*followerReplication) // 管理每个Follower的复制状态机
	r.leaderState.stepDown = make(chan struct{}, 1)                   // RSM在运行过程中，发现需要降级为Follower，就向这个channel发送数据
}

// 循环运行Leader
func (r *Raft) runLeader() {
	r.logger.Info("entering leader state", "leader", r)

	// leaderState保存了在leader状态时的一些关键信息：
	// 1. follower的日志提交
	// 2. 每个follower的复制状态机
	// 3. inflight数组：如果一个日志已经提交，leader需要从inflight中取出它并回复给client.
	// 4. stepDown的channel: 在日志复制过程中，如果探测出了更新的Leader，则通过这个channel通知leader
	r.setupLeaderState()

	// 退出leader状态时的清理
	defer func() {
		r.SetLastContact()

		// 直接关闭每个follower的RSM
		// 和startStopReplication关闭RSM不一样
		// 这里是直接关闭，并没有在退出前发送全部日志
		// 因为startStopReplication是正常关闭，这里的关闭是因为我们已经不是leader
		// 需要立刻进入到follower状态
		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}

		// 清空leaderState状态
		r.leaderState.commitCh = nil
		r.leaderState.commitment = nil
		r.leaderState.replState = nil
		r.leaderState.stepDown = nil

		r.leaderLock.Lock()
		if r.leader == r.localAddr {
			r.leader = ""
		}
		r.leaderLock.Unlock()
	}()

	// 根据follower的数量，启动或停止相应数量的复制状态机
	r.startStopReplication()

	// 在当选为Leader后，立刻发送一个空日志给所有Follower，这样顺带就把之前Term的日志一起提交了.
	noop := &LogFuture{
		log: raft.Log{
			Type: raft.LogNoop,
		},
	}
	r.dispatchLogs([]*LogFuture{noop})

	// 开始Leader循环
	r.leaderLoop()
}

// asyncNotifyCh is used to do an async channel send
// to a single channel without blocking.
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

/*
启动或停止follower的复制状态机
*/
func (r *Raft) startStopReplication() {
	inConfig := make(map[ServerID]bool, len(r.configurations.Server))
	lastIdx := r.GetLastIndex()

	for _, server := range r.configurations.Server {
		if server.ID == r.localID {
			continue
		}
		// 统计有多少个follower
		inConfig[server.ID] = true

		// 如果follower没启动过RSM，就启动它
		if _, ok := r.leaderState.replState[server.ID]; !ok {
			r.logger.Info("added peer, starting replication", "peer", server.ID)
			s := &followerReplication{
				peer:        server,
				commitment:  r.leaderState.commitment,
				stopCh:      make(chan uint64, 1),
				triggerCh:   make(chan struct{}, 1),
				currentTerm: r.GetCurrentTerm(),
				nextIndex:   lastIdx + 1,
				lastContact: time.Now(),
				stepDown:    r.leaderState.stepDown,
			}
			// 保存每个follower的RSM
			r.leaderState.replState[server.ID] = s
			// 启动复制函数
			r.goFunc(func() { r.replicate(s) })
			// 立刻通知RSM开始复制
			asyncNotifyCh(s.triggerCh)
		}
	}

	// 如果follower减少了就停止对应的RSM
	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}

		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIdx)
		// 关闭RSM之前将当前last index发送，让RSM在退出前把所有日志发出去
		repl.stopCh <- lastIdx
		// 通知RSM关闭
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
	}
}

// 循环运行leader
func (r *Raft) leaderLoop() {
	r.logger.Info("enter leaderLoop() function")
	defer func() {
		r.logger.Info("leave leaderLoop() function")
	}()
	stepDown := false

	for r.GetState() == Leader {

		select {
		// 处理RPC请求
		case rpc := <-r.rpcCh:
			r.logger.Info("leaderLoop rpcCh...")
			r.processRPC(rpc)

		// 在leader运行中检查是否需要降级为follower
		case <-r.leaderState.stepDown:
			r.logger.Info("leaderLoop stepDown...")
			r.SetState(Follower)

		// 发送给follower的日志被提交，就需要处理提交后的事情
		case <-r.leaderState.commitCh:
			r.logger.Info("leaderLoop commitCh...")
			// 获取当前最大的已提交日志
			commitIndex := r.leaderState.commitment.getCommitIndex()
			// 设置当前最大的已提交日志
			r.SetCommitIndex(commitIndex)

			var groupReady []*list.Element
			var groupFutures = make(map[uint64]*LogFuture)
			var lastIdxInGroup uint64

			// inflight数组中临时保存了之前RSM发送日志时的另一份日志
			// 也就是说发送给follower之前，leader写日志到自己本地，也同时保存了一份到inflight
			// 现在从中取出已提交的日志，需要利用这些日志，来回复还在等待的client
			for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
				commitLog := e.Value.(*LogFuture)
				idx := commitLog.log.Index
				if idx > commitIndex {
					break
				}

				groupReady = append(groupReady, e)
				groupFutures[idx] = commitLog
				lastIdxInGroup = idx
			}

			// 写入到FSM并移除inflight中保存的临时日志
			if len(groupReady) > 0 {
				// lastIdxGroup是这些已提交日志中最大的index
				// groupFutures是从inflight取出的已提交日志
				// processLogs把它们发送给FSM处理，同时使用日志中携带的Future功能回复client
				r.processLogs(lastIdxInGroup, groupFutures)

				for _, e := range groupReady {
					r.leaderState.inflight.Remove(e)
				}
			}

		// 从ApplyLog函数收到的新日志
		case newLog := <-r.applyCh:
			r.logger.Info("leaderLoop applyCh...")
			ready := []*LogFuture{newLog}

			// 一次性尽量收集更多的日志
		GROUP_COMMIT_LOOP:
			for i := 0; i < r.config.MaxAppendEntries; i++ {
				select {
				case newLog := <-r.applyCh:
					ready = append(ready, newLog)
				default:
					break GROUP_COMMIT_LOOP
				}
			}

			if stepDown {
				for i := range ready {
					ready[i].err = ErrNotLeader
				}
			} else {
				// leader写入日志到本地磁盘，并通知RSM复制日志
				r.dispatchLogs(ready)
			}

		case <-r.shutdownCh:
			return
		}
	}
}

/*
Leader分发日志：现写一份日志到Leader本地磁盘，再通知RSM复制日志给Follower
*/
func (r *Raft) dispatchLogs(applyLogs []*LogFuture) {
	fmt.Printf("dispatchLogs: %v\n", applyLogs[0])

	// 获取当前leader的最后日志Index和Term
	term := r.GetCurrentTerm()
	lastIndex := r.GetLastIndex()

	n := len(applyLogs)
	logs := make([]*raft.Log, n)

	// 循环给每个日志设置index和term，并同时把日志保存到inflight中一份.
	for idx, applyLog := range applyLogs {
		lastIndex++
		applyLog.log.Term = term
		applyLog.log.Index = lastIndex
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)
	}

	// leader保存日志到自己的日志库中
	err := r.logs.StoreLogs(logs)
	if err != nil {
		// 如果保存出错就回退到Follower状态
		r.logger.Error("failed to commit logs", "error", err)
		for _, applyLog := range applyLogs {
			applyLog.err = err
		}
		r.SetState(Follower)
		return
	}

	// 将刚才保存的日志的index发送给统计日志是否提交的函数
	r.leaderState.commitment.match(r.localID, lastIndex)

	// 设置leader中最后日志的Index和Term
	r.SetLastLog(lastIndex, term)

	// 通知每个Follower的RSM
	for _, f := range r.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}

}

func backoff(base time.Duration, round, limit uint64) time.Duration {
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

// Leader用于不停发送心跳给Follower的函数
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	// 空的日志追加请求，不含日志
	req := AppendEntriesRPCRequest{
		Term:     r.currentTerm,
		LeaderId: string(r.leader),
	}

	var resp AppendEntriesRPCResponse
	for {
		// 等待下一次心跳间隔
		timeoutChan, timeout := randomTimeout(r.config.HeartbeatTimeout / 10)
		fmt.Println("发送心跳间隔：", timeout)
		select {
		case <-timeoutChan:
		case <-stopCh:
			return
		}

		// 发送心跳给Follower
		r.logger.Info("发送心跳给: ", s.peer.ID)
		err := r.trans.AppendEntries(string(s.peer.ID), s.peer.Address, &req, &resp)
		if err != nil {
			// 传输心跳失败则增加失败计数
			r.logger.Error("failed to heartbeat to", "peer", s.peer.Address, "error", err)
			failures++
			select {
			// 指数回退
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			failures = 0
		}
	}
}

// RSM核心函数，循环运行
func (r *Raft) replicate(s *followerReplication) {
	// 启动heartbeat处理函数
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat) // 函数退出时关闭heartbeats处理函数
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

	shouldStop := false // 调用发送AE的函数，返回是否需要停止RSM
	for !shouldStop {
		//commitTimeoutChan, timeout := randomTimeout(r.config.CommitTimeout)
		//fmt.Println("提交超时：", timeout)
		select {
		// startStopReplication关闭RSM将最大的lastLogIndex发送到stopCh
		// 这里接收到之后会把最后的日志发送完成后再退出
		case maxIndex := <-s.stopCh:
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		case <-s.triggerCh:
			// 获取本地的所有日志发送给follower
			lastLogIdx, _ := r.GetLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
			/*
				case <-commitTimeoutChan:
					// 由于heartbeat不发送leaderCommit值，而且follower由于网络和磁盘
					// 阻塞可能不能及时知道leaderCommit，所以在超时之前尽快发送.
					lastLogIdx, _ := r.GetLastLog()
					shouldStop = r.replicateTo(s, lastLogIdx)

			*/
		}

		// 如果上面发送AE给follower一切正常，且允许流水线方式发送.
		if !shouldStop && s.allowPipeline {
			// goto pipeline
		}
	}
	return
}

// 实际发送日志追加请求给Follower的函数
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	var req AppendEntriesRPCRequest
	var resp AppendEntriesRPCResponse
START:
	// 如果和follower通信的失败次数大于0，则以10毫秒为基础使用指数回退
	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	// 从follower的nextIndex开始，直到leader本地的lastIndex开始，在req内保存日志
	err := r.setupAppendEntries(s, &req, atomic.LoadUint64(&s.nextIndex), lastIndex)
	if err == ErrLogNotFound {
		// goto SEND_SNAP
	} else if err != nil {
		return
	}

	// 将日志发送给follower
	err = r.trans.AppendEntries(string(s.peer.ID), s.peer.Address, &req, &resp)
	if err != nil {
		r.logger.Error("failed to appendEntries to", "peer", s.peer, "error", err)
		s.failures++
		return
	}

	// 如果收到了follower的term比自己大，说明集群内已经有新leader
	if resp.Term > req.Term {
		asyncNotifyCh(s.stepDown)
		return true
	}

	// 设置当前follower的最后联系时间
	s.setLastContact()

	if resp.Success {
		// follower响应成功，更新follower的nextIndex
		// 并统计index是否被大多数follower写盘
		updateLastAppended(s, &req)

		s.failures = 0
		s.allowPipeline = true
	} else {
		// 如果失败，设置下一个要发送的index为follower响应的index
		atomic.StoreUint64(&s.nextIndex, max(min(s.nextIndex-1, resp.LastLog+1), 1))

		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
	}

	// 如果收到stopCh的停止信号，就返回shouldStop为true
	// 需要关闭的场景有：
	// 1. 整个leader退出，在leaderLoop函数退出时会关闭所有follower的RSM
	// 2. startStopReplication函数中单独的关闭某个follower的RSM
	select {
	case <-s.stopCh:
		return true
	default:
	}

	// 如果经过一次发送follower回应的nextIndex还是小于本地最后的index
	// 就再次发送
	if atomic.LoadUint64(&s.nextIndex) <= lastIndex {
		goto START
	}

	return
}

func updateLastAppended(s *followerReplication, req *AppendEntriesRPCRequest) {
	// Mark any inflight logs as committed
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&s.nextIndex, last.Index+1)
		s.commitment.match(s.peer.ID, last.Index)
	}
}

// 为每个日志追加请求设置前一个Index和Term，以及应该发送的日志.
func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRPCRequest, nextIndex, lastIndex uint64) error {
	//  设置当前的Index和Term，以及Leader当前的已提交Index
	req.Term = s.currentTerm
	req.LeaderId = string(r.leader)
	req.LeaderCommit = r.GetCommitIndex()

	// 设置前一个日志的Index和Term
	err := r.setPreviousLog(req, nextIndex)
	if err != nil {
		return err
	}

	// 设置所有应该发送的日志
	err = r.setNewLogs(req, nextIndex, lastIndex)
	if err != nil {
		return err
	}

	return nil
}

// Leader根据某个Follower的nextIndex设置应该发送给它的PrevIndex和PrevTerm
func (r *Raft) setPreviousLog(req *AppendEntriesRPCRequest, nextIndex uint64) error {
	if nextIndex == 1 {
		req.PrevLogIndex = 0
		req.PrevLogTerm = 0
	} else {
		var l raft.Log
		err := r.logs.GetLog(nextIndex-1, &l)
		if err != nil {
			r.logger.Error("failed to get log", "index", nextIndex-1, "error", err)
			return err
		}

		req.PrevLogIndex = l.Index
		req.PrevLogTerm = l.Term
	}

	return nil
}

// Leader从某个Follower的nextIndex开始直到lastIndex，查询应该发送给Follower的日志．
func (r *Raft) setNewLogs(req *AppendEntriesRPCRequest, nextIndex, lastIndex uint64) error {
	req.Entries = make([]*raft.Log, 0, r.config.MaxAppendEntries)
	maxIndex := min(nextIndex+uint64(r.config.MaxAppendEntries)-1, lastIndex)

	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(raft.Log)
		err := r.logs.GetLog(i, oldLog)

		if err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			return err
		}

		req.Entries = append(req.Entries, oldLog)
	}

	return nil
}
