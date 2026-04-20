//go:build !sim

package server

import (
	"bytes"
	"encoding/gob"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/adamors/raft/persister"
	"github.com/adamors/raft/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// testFSM is a minimal FSM for testing
type testFSM struct {
	mu   sync.Mutex
	logs map[int][]byte
}

var _ raft.FSM = (*testFSM)(nil)

func newTestFSM() *testFSM {
	return &testFSM{logs: make(map[int][]byte)}
}

func (f *testFSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs[l.Index] = l.Data
	return nil
}

func (f *testFSM) get(index int) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	v, ok := f.logs[index]
	return v, ok
}

type testFSMSnapshot struct {
	reader io.Reader
}

func (s *testFSMSnapshot) Persist(w io.Writer) error {
	_, err := io.Copy(w, s.reader)
	return err
}

func (s *testFSMSnapshot) Release() {}

func (f *testFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(f.logs); err != nil {
		return nil, err
	}
	return &testFSMSnapshot{reader: &buf}, nil
}

func (f *testFSM) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var logs map[int][]byte
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&logs); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logs = logs
	return nil
}

type testNode struct {
	raft    *raft.Raft
	fsm     *testFSM
	grpcSrv *grpc.Server
}

func startCluster(t *testing.T, n int) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}

	nodes := make([]*testNode, n)
	for i := range n {
		transport, err := raft.NewGrpcTransport(addrs)
		require.NoError(t, err)

		fsm := newTestFSM()
		r := raft.NewRaft(transport, addrs[i], persister.NewInMemoryPersister(), fsm, raft.DefaultConfig())

		gsrv := grpc.NewServer()
		NewRaftServiceHandler(r).Register(gsrv)
		go gsrv.Serve(listeners[i])

		nodes[i] = &testNode{raft: r, fsm: fsm, grpcSrv: gsrv}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.grpcSrv.Stop()
		}
	})

	return nodes
}

func nCommitted(t *testing.T, index int, nodes []*testNode) (int, []byte) {
	t.Helper()
	count := 0
	var cmd []byte
	for _, node := range nodes {
		v, ok := node.fsm.get(index)
		if !ok {
			continue
		}
		if count > 0 && !slices.Equal(cmd, v) {
			t.Fatalf("committed values at index %v do not match", index)
		}
		count++
		cmd = v
	}
	return count, cmd
}

func applyOnLeader(t *testing.T, nodes []*testNode, cmd []byte, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			f := node.raft.Apply(cmd, 2*time.Second)
			if f.Error() == nil {
				return f.Index()
			}
		}
	}
	t.Fatal("timed out waiting to apply command")
	return -1
}

func TestServer(t *testing.T) {
	nodes := startCluster(t, 3)

	t.Run("leader election succeeds", func(t *testing.T) {
		require.NoError(t, nodes[0].raft.WaitForLeader(3*time.Second))

		leaderCount := 0
		var leaderAddr string
		for _, node := range nodes {
			_, isLeader := node.raft.GetState()
			if isLeader {
				leaderCount++
				leaderAddr = node.raft.Leader()
			}
		}
		require.Equal(t, 1, leaderCount, "expected exactly one leader")
		for _, node := range nodes {
			require.Equal(t, leaderAddr, node.raft.Leader(), "all nodes should agree on leader")
		}
	})

	t.Run("replication succeeds", func(t *testing.T) {
		require.NoError(t, nodes[0].raft.WaitForLeader(3*time.Second))
		cmd := []byte("hello")
		index := applyOnLeader(t, nodes, cmd, 10*time.Second)
		require.Eventually(t, func() bool {
			n, got := nCommitted(t, index, nodes)
			return n == len(nodes) && slices.Equal(got, cmd)
		}, 2*time.Second, 20*time.Millisecond)
	})

	t.Run("apply on follower returns error", func(t *testing.T) {
		require.NoError(t, nodes[0].raft.WaitForLeader(3*time.Second))
		for _, node := range nodes {
			_, isLeader := node.raft.GetState()
			if !isLeader {
				f := node.raft.Apply([]byte("from follower"), 1*time.Second)
				require.Error(t, f.Error())
				return
			}
		}
		t.Fatal("no follower found")
	})
}

func TestWaitForLeaderTimeout(t *testing.T) {
	// Two-node cluster where the second peer is unreachable. Leader should never get elected
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()

	transport, err := raft.NewGrpcTransport([]string{addr, "127.0.0.1:1"})
	require.NoError(t, err)

	r := raft.NewRaft(transport, addr, persister.NewInMemoryPersister(), newTestFSM(), raft.DefaultConfig())
	gsrv := grpc.NewServer()
	NewRaftServiceHandler(r).Register(gsrv)
	go gsrv.Serve(l)
	t.Cleanup(func() { gsrv.Stop() })

	err = r.WaitForLeader(300 * time.Millisecond)
	require.Error(t, err)
}
