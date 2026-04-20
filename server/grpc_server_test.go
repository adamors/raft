//go:build !sim

package server

import (
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/adamors/raft/persister"
	"github.com/adamors/raft/raft"
	"github.com/adamors/raft/server"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	listeners := make([]net.Listener, 3)
	addresses := make([]string, 3)
	servers := make([]*server.GrpcServer, 3)

	for i := range 3 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		addresses[i] = l.Addr().String()
	}

	transport, err := raft.NewGrpcTransport(addresses)
	require.NoError(t, err)

	for i := range 3 {
		persister := persister.NewInMemoryPersister()
		servers[i] = server.NewGRPCServer(addresses[i], transport, persister, raft.DefaultConfig())
		go func() {
			servers[i].Serve(listeners[i])
		}()
	}

	t.Run("leader election succeeds", func(t *testing.T) {
		testLeaderElection(t, servers)
	})
	t.Run("replication succeeds", func(t *testing.T) {
		testReplication(t, servers)
	})

	for i := range 3 {
		servers[i].Stop()
	}
}

func testLeaderElection(t *testing.T, servers []*server.GrpcServer) {
	err := servers[0].Raft().WaitForLeader(3 * time.Second)
	require.NoError(t, err)
	leaderCount := 0
	var leaderAddr string
	for _, server := range servers {
		_, isLeader := server.GetState()
		if isLeader {
			leaderCount++
			leaderAddr = server.Address()
		}
	}
	require.Equal(t, 1, leaderCount, "expected exactly one leader")

	for _, server := range servers {
		addr := server.Raft().Leader()
		require.Equal(t, leaderAddr, addr, "all nodes should agree on leader")
	}
}

func testReplication(t *testing.T, servers []*server.GrpcServer) {
	err := servers[0].Raft().WaitForLeader(3 * time.Second)
	require.NoError(t, err)
	cmd := []byte("100")

	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		index := -1
		for _, server := range servers {
			future := server.Raft().Apply(cmd, 2*time.Second)
			if future.Error() == nil {
				index = future.Index()
				break
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := nCommitted(t, index, servers)
				if nd > 0 && nd >= len(servers) {
					// committed
					if slices.Equal(cmd1, cmd) {
						// and it was the command we submitted.
						fmt.Printf("agreement of %s reached\n", cmd)
						return
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatal("replication timed out")
}

func nCommitted(t *testing.T, index int, servers []*server.GrpcServer) (int, []byte) {
	t.Helper()

	count := 0
	var cmd []byte
	for _, server := range servers {
		if server.ApplyErr() != "" {
			t.Fatal(server.ApplyErr())
		}
		cmd1, ok := server.Logs(index)
		if ok {
			b, _ := cmd1.([]byte)
			if count > 0 && !slices.Equal(cmd, b) {
				t.Fatalf("committed values at index %v do not match (%v != %v)", index, cmd, b)
			}
			count += 1
			cmd = b
		}
	}

	return count, cmd
}
