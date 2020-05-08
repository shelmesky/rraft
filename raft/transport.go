package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	rpcAppendEntries uint8 = iota
	rpcRequestVote
	rpcInstallSnapshot
	rpcTimeoutNow

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// rpcMaxPipeline controls the maximum number of outstanding
	// AppendEntries RPC calls.
	rpcMaxPipeline = 128
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

type RPCRequest struct {
	ReqType uint8
	Req     interface{}
}

// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}
	Reader   io.Reader // Set only for InstallSnapshot
	RespChan chan RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	Consumer() chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	LocalAddr() ServerAddress

	// AppendEntries sends the appropriate RPC to the target node.
	AppendEntries(id string, target ServerAddress, args *AppendEntriesRPCRequest, resp *AppendEntriesRPCResponse) error

	// RequestVote sends the appropriate RPC to the target node.
	RequestVote(id string, target ServerAddress, args *RequestVoteRPCRequest, resp *RequestVoteRPCResponse) error

	// SetHeartbeatHandler is used to setup a heartbeat handler
	// as a fast-pass. This is to avoid head-of-line blocking from
	// disk IO. If a Transport does not support this, it can simply
	// ignore the call, and push the heartbeat onto the Consumer channel.
	SetHeartbeatHandler(cb func(rpc RPC))
}

type ConnCoding struct {
	conn    net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
}

type TCPTransport struct {
	localID         ServerID
	localAddr       ServerAddress
	clientConns     map[ServerAddress]*ConnCoding
	connsLock       *sync.Mutex
	rpcChan         chan RPC
	heartbeatFn     func(RPC)
	heartbeatFnLock sync.Mutex
}

func NewTCPTransport(id ServerID, raftBind string) (Transport, error) {
	transport := new(TCPTransport)
	transport.clientConns = make(map[ServerAddress]*ConnCoding, 10)
	transport.connsLock = new(sync.Mutex)
	transport.localID = id

	addr, err := net.ResolveTCPAddr("tcp", raftBind)
	if err != nil {
		return nil, err
	}

	transport.localAddr = ServerAddress(raftBind)

	err = transport.serverLoop(id, addr)
	if err != nil {
		return transport, err
	}

	return transport, nil
}

func (trans *TCPTransport) serverLoop(id ServerID, addr *net.TCPAddr) error {
	listen, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	acceptLoop := func() {
		for {
			conn, err := listen.Accept()
			if err != nil {
				log.Println("Accept() failed:", err)
				continue
			}

			go trans.handleConn(conn)
		}
	}

	go acceptLoop()

	return nil
}

func (trans *TCPTransport) handleConn(conn net.Conn) {
	if conn == nil {
		return
	}

	defer conn.Close()

	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	for {
		// 解码请求
		var rpcRequest RPCRequest
		err := decoder.Decode(&rpcRequest)
		if err != nil {
			log.Println("Decode rpcData failed:", err)
			return
		}

		var rpc RPC
		rpc.Command = rpcRequest.Req
		rpc.RespChan = make(chan RPCResponse, 1)

		trans.rpcChan <- rpc

		// 等待处理的结果
		select {
		case resp := <-rpc.RespChan:
			if resp.Error != nil {
				log.Println("RPC Response failed:", resp.Error)
				respErr := ""
				if resp.Error != nil {
					respErr = resp.Error.Error()
				}
				err := encoder.Encode(respErr)
				err = encoder.Encode(resp.Response)
				if err != nil {
					log.Println("Send RPC Response faield:", err)
					return
				}
			}
		}
	}
}

func (trans *TCPTransport) getConnCoding(target ServerAddress) (*ConnCoding, error) {
	trans.connsLock.Lock()
	defer trans.connsLock.Unlock()

	if conn, ok := trans.clientConns[target]; ok {
		return conn, nil
	} else {
		return trans.dialFollower(target)
	}
}

func (trans *TCPTransport) dialFollower(target ServerAddress) (*ConnCoding, error) {
	timeout := time.Duration(time.Second * 5)
	conn, err := net.DialTimeout("tcp", string(target), timeout)
	if err != nil {
		return nil, err
	}

	trans.connsLock.Lock()
	defer trans.connsLock.Unlock()

	connCoding := &ConnCoding{
		conn:    conn,
		encoder: gob.NewEncoder(conn),
		decoder: gob.NewDecoder(conn),
	}

	trans.clientConns[target] = connCoding

	return connCoding, nil
}

func (trans *TCPTransport) Consumer() chan RPC {
	return trans.rpcChan
}

func (trans *TCPTransport) LocalAddr() ServerAddress {
	return trans.localAddr
}

func (trans *TCPTransport) AppendEntries(id string, target ServerAddress, args *AppendEntriesRPCRequest,
	resp *AppendEntriesRPCResponse) error {

	connCoding, err := trans.getConnCoding(target)
	if err != nil {
		return err
	}

	req := RPCRequest{
		ReqType: rpcAppendEntries,
		Req:     args,
	}

	err = connCoding.encoder.Encode(req)
	if err != nil {
		return err
	}

	respErr := ""
	err = connCoding.decoder.Decode(&respErr)
	if err != nil {
		return err
	}

	if respErr != "" {
		return fmt.Errorf(respErr)
	}

	err = connCoding.decoder.Decode(resp)
	if err != nil {
		return err
	}

	return nil
}

func (trans *TCPTransport) RequestVote(id string, target ServerAddress, args *RequestVoteRPCRequest,
	resp *RequestVoteRPCResponse) error {

	connCoding, err := trans.getConnCoding(target)

	if err != nil {
		return err
	}

	req := RPCRequest{
		ReqType: rpcRequestVote,
		Req:     args,
	}

	err = connCoding.encoder.Encode(req)
	if err != nil {
		return err
	}

	respErr := ""
	err = connCoding.decoder.Decode(&respErr)
	if err != nil {
		return err
	}

	if respErr != "" {
		return fmt.Errorf(respErr)
	}

	err = connCoding.decoder.Decode(resp)
	if err != nil {
		return err
	}

	return nil
}

func (trans *TCPTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	trans.heartbeatFnLock.Lock()
	defer trans.heartbeatFnLock.Unlock()
	trans.heartbeatFn = cb
}
