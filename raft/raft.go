package raft

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adamors/raft/persister"
)

var (
	ErrTimeout           error
	ErrRaftKilled        error
	ErrNotLeader         error
	ErrLeadershipChanged error
)

func init() {
	ErrTimeout = errors.New("apply timed out")
	ErrRaftKilled = errors.New("raft killed")
	ErrNotLeader = errors.New("node is no longer leader")
	ErrLeadershipChanged = errors.New("leader changed")
}

type Raft struct {
	mu        sync.Mutex
	transport Transport
	persister persister.Persister
	me        int
	dead      int32
	shutDown  bool

	rng *rand.Rand // per-server RNG so nodes don't share identical election timeouts

	isLeader        bool
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	doneCh chan struct{}

	fsm FSM

	futures   map[int]*ApplyMsgFuture
	futuresMu sync.Mutex

	wg sync.WaitGroup

	maxraftstate      int // snapshot if log grows this big
	heartbeatInterval time.Duration
	leaderId          int // id of leader from peers

	// Persistent
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Snapshot
	snapshot          []byte
	lastIncludedTerm  int
	lastIncludedIndex int

	lastAckTime []time.Time
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// GetState returns the current term and whether this node is leader or not
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader
}

// Done returns the done channel
func (rf *Raft) Done() <-chan struct{} {
	return rf.doneCh
}

// Leader tries to return the address of the leader. If this not is the leader
// it will return its own address, otherwise it will try to look up the leader's
// address if the leader's id is known. Empty string otherwise.
func (rf *Raft) Leader() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader {
		return rf.transport.Address(rf.me)
	}
	if rf.leaderId < 0 {
		return ""
	}
	return rf.transport.Address(rf.leaderId)
}

// WaitForLeader blocks until it can return the address of the leader node or
// until timeout runs out. In this case, an error is returned.
func (rf *Raft) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutc:
			return ErrTimeout
		case <-ticker.C:
			if rf.Leader() != "" {
				return nil
			}
		}
	}
}

// Apply is responsible for replicating a command to every node. If the current
// node is not the leader it will return early with an error, otherwise it will
// send out append entry calls to each node to replicate the command.
func (rf *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	rf.mu.Lock()

	if !rf.isLeader {
		rf.mu.Unlock()
		return &ErrorFuture{ErrNotLeader}
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: cmd})
	rf.heartbeat()
	rf.persist()
	index := rf.lastLogIndex()
	term := rf.currentTerm

	rf.mu.Unlock()

	future := &ApplyMsgFuture{
		done:     make(chan struct{}),
		raftDone: rf.doneCh,
		timeout:  timeout,
		index:    index,
		term:     term,
	}

	rf.futuresMu.Lock()
	rf.futures[index] = future
	rf.futuresMu.Unlock()
	return future
}

// Shutdown will shut down raft, returning a future. Calling Error() on the future
// will block since it will call wait on all running goroutines. These will stop
// gradually.
func (rf *Raft) Shutdown() Future {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.shutDown {
		rf.shutDown = true
		atomic.StoreInt32(&rf.dead, 1)

		return shutdownFuture{rf: rf}
	}

	return shutdownFuture{nil}
}

func (rf *Raft) waitAndDone() {
	rf.wg.Wait()
	close(rf.doneCh)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) stepDownAsLeader(newTerm int) {
	// caller holds lock
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.isLeader = false
	rf.persist()
}

// NewRaft returns a new Raft instance
func NewRaft(transport Transport, me int, p persister.Persister, fsm FSM, config *Config) *Raft {
	if config == nil {
		config = DefaultConfig()
	}
	rf := &Raft{
		transport:         transport,
		persister:         p,
		me:                me,
		electionTimeout:   config.ElectionTimeout,
		heartbeatInterval: config.HeartbeatInterval,
		votedFor:          -1,
		leaderId:          -1,
		maxraftstate:      config.MaxRaftState,
		rng:               rand.New(rand.NewSource(int64(me + 1))),
		log:               []LogEntry{{Term: 0}},
		nextIndex:         make([]int, transport.NumPeers()),
		matchIndex:        make([]int, transport.NumPeers()),
		lastAckTime:       make([]time.Time, transport.NumPeers()),
		fsm:               fsm,
		doneCh:            make(chan struct{}),
		futures:           make(map[int]*ApplyMsgFuture),
	}

	rf.readPersist(p.ReadRaftState())
	rf.snapshot = p.ReadSnapshot()

	if len(rf.snapshot) > 0 {
		rf.fsm.Restore(bytes.NewReader(rf.snapshot))
	}

	// Restarted nodes should give the existing leader time to send a heartbeat
	// before triggering an election.
	if rf.currentTerm > 0 {
		rf.lastHeartbeat = time.Now()
	}

	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex

	rf.wg.Add(3)
	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applyTicker()

	return rf
}
