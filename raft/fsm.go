package raft

import (
	"io"
	"time"
)

type FSM interface {
	Apply(log *Log) any
	Snapshot() (FSMSnapshot, error)
	Restore(snapshot io.Reader) error
}

type Log struct {
	Data  []byte
	Index int
	Term  int
}

type FSMSnapshot interface {
	Persist(io.Writer) error
	Release()
}

type Future interface {
	Error() error
}

type ApplyFuture interface {
	Future
	Response() any
	Index() int
}

type ErrorFuture struct {
	err error
}

func (ef *ErrorFuture) Error() error {
	return ef.err
}

func (ef *ErrorFuture) Response() any {
	return nil
}

func (ef *ErrorFuture) Index() int {
	return -1
}

type shutdownFuture struct {
	rf *Raft
}

func (f shutdownFuture) Error() error {
	if f.rf == nil {
		return nil
	}
	f.rf.waitAndDone()

	return nil
}

var _ Future = (*shutdownFuture)(nil)

type ApplyMsgFuture struct {
	done     chan struct{}
	raftDone <-chan struct{}
	err      error
	index    int
	response any
	term     int // term when submitted
	timeout  time.Duration
	waited   bool // whether we've already waited for completion
}

// wait blocks until the future completes, can be called multiple times
func (f *ApplyMsgFuture) wait() {
	if f.waited {
		return
	}
	select {
	case <-time.After(f.timeout):
		f.err = ErrTimeout
	case <-f.raftDone:
		f.err = ErrRaftKilled
	case <-f.done:
	}
	f.waited = true
}

func (f *ApplyMsgFuture) Error() error {
	f.wait()
	return f.err
}

func (f *ApplyMsgFuture) Response() any {
	f.wait()
	if f.err != nil {
		return nil
	}
	return f.response
}

func (f *ApplyMsgFuture) Index() int {
	return f.index
}
