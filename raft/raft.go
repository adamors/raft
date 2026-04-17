package raft

import (
	"bytes"
	"errors"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adamors/raft/persister"
)

var (
	ErrTimeout             error
	ErrRaftKilled          error
	ErrNotLeader           error
	ErrLeadershipChanged   error
	ErrConfigChangePending error
	ErrAddrInvalid         error
	ErrInternalError       error
	ErrCannotRemoveLeader  error
)

func init() {
	ErrTimeout = errors.New("apply timed out")
	ErrRaftKilled = errors.New("raft killed")
	ErrNotLeader = errors.New("node is no longer leader")
	ErrLeadershipChanged = errors.New("leader changed")
	ErrConfigChangePending = errors.New("configuration change in progress, try again")
	ErrAddrInvalid = errors.New("could not dial provider server address or address is not known")
	ErrInternalError = errors.New("internal error occured, check logs")
	ErrCannotRemoveLeader = errors.New("cannot remove the leader, transfer leadership first")
}

type Raft struct {
	mu        sync.Mutex
	transport Transport
	persister persister.Persister
	me        string
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
	leaderId          string // address of leader, empty if unknown

	configChangePending bool

	// Persistent
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Snapshot
	snapshot          []byte
	lastIncludedTerm  int
	lastIncludedIndex int

	lastAckTime map[string]time.Time
	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int
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

// Leader tries to return the address of the leader. Returns own address if
// this node is the leader, the known leader's address if available, empty string otherwise.
func (rf *Raft) Leader() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isLeader {
		return rf.me
	}
	return rf.leaderId
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

	return rf.respondWithFuture(index, term, timeout)
}

// AddServer allows ammending Raft's list of nodes. At once, only one server can be
// added, and each server addition goes through the Raft concensus. There's no
// voting/non-voting distinction at this time.
func (rf *Raft) AddServer(address string, timeout time.Duration) Future {
	index, term, err := rf.makeConfigChange(address, addServerChange)
	if err != nil {
		return &ErrorFuture{err}
	}

	return rf.respondWithFuture(index, term, timeout)
}

// RemoveServer removes a node from the list of nodes managed by Raft. At once, only
// one server can be removed and each server removal goes through the Raft consensus.
// Attempting to remove the current leader will result in an error for now.
func (rf *Raft) RemoveServer(address string, timeout time.Duration) ApplyFuture {
	index, term, err := rf.makeConfigChange(address, removeServerChange)
	if err != nil {
		return &ErrorFuture{err}
	}

	return rf.respondWithFuture(index, term, timeout)
}

// GetConfiguration returns the nodes that Raft currently knows about
func (rf *Raft) GetConfiguration() *ConfigurationFuture {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	peers := rf.transport.Peers()
	servers := make([]Server, len(peers))
	for i, addr := range peers {
		servers[i] = Server{Address: addr}
	}

	return &ConfigurationFuture{configuration: Configuration{Servers: servers}}
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

func (rf *Raft) respondWithFuture(index, term int, timeout time.Duration) ApplyFuture {
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

func (rf *Raft) waitAndDone() {
	rf.wg.Wait()
	close(rf.doneCh)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func rngSeed(id string) int64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return int64(h.Sum64())
}

func (rf *Raft) stepDownAsLeader(newTerm int) {
	// caller holds lock
	rf.currentTerm = newTerm
	rf.votedFor = ""
	rf.isLeader = false
	rf.persist()
}

// NewRaft returns a new Raft instance
func NewRaft(transport Transport, me string, p persister.Persister, fsm FSM, config *Config) *Raft {
	if config == nil {
		config = DefaultConfig()
	}
	rf := &Raft{
		transport:         transport,
		persister:         p,
		me:                me,
		electionTimeout:   config.ElectionTimeout,
		heartbeatInterval: config.HeartbeatInterval,
		maxraftstate:      config.MaxRaftState,
		rng:               rand.New(rand.NewSource(rngSeed(me))),
		log:               []LogEntry{{Term: 0}},
		nextIndex:         make(map[string]int),
		matchIndex:        make(map[string]int),
		lastAckTime:       make(map[string]time.Time),
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
