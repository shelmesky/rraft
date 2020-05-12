package raft

import (
	"github.com/hashicorp/raft"
	"time"
)

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

type deferError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

type LogFuture struct {
	deferError
	log      raft.Log
	response interface{}
	dispatch time.Time
}

func (l *LogFuture) Response() interface{} {
	return l.response
}

func (l *LogFuture) Index() uint64 {
	return l.log.Index
}

// Future is used to represent an action that may occur in the future.
type Future interface {
	// Error blocks until the future arrives and then
	// returns the error status of the future.
	// This may be called any number of times - all
	// calls will return the same value.
	// Note that it is not OK to call this method
	// twice concurrently on the same Future instance.
	Error() error
}

// IndexFuture is used for future actions that can result in a raft log entry
// being created.
type IndexFuture interface {
	Future

	// Index holds the index of the newly applied log entry.
	// This must not be called until after the Error method has returned.
	Index() uint64
}

// ApplyFuture is used for Apply and can return the FSM response.
type ApplyFuture interface {
	IndexFuture

	// Response returns the FSM response as returned
	// by the FSM.Apply method. This must not be called
	// until after the Error method has returned.
	Response() interface{}
}
