package raft_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim/gosimruntime"
)

func TestSingleNode(t *testing.T) {
	c := newCluster(1)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected leader, got %v", err)
	}

	cmd := []byte("hello")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("single node commit failed: %v", err)
	}
}

// TestInitialElection:  Start 3 machines, each running a `GrpcServer` bound to
// a gosim-simulated TCP listener. Call `WaitForLeader`.
// Assert exactly one server returns `isLeader=true` and all three agree on who the leader is.
// No network manipulation - validates the happy path.
func TestInitialElection(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeader(c, 3*time.Second)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	leaderCount := 0
	for _, addr := range c.addrs {
		state, err := getState(addr)
		if err != nil {
			t.Fatalf("Expected no error from getting state, got %v", err)
		}
		if state.isLeader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("Expected only one leader, got %d", leaderCount)
	}

	for _, addr := range c.addrs {
		state, err := getState(addr)
		if err != nil {
			t.Fatalf("Expected no error from getting state, got %v", err)
		}
		if state.leader != leaderAddr {
			t.Fatalf("All nodes should agree on leader")
		}
	}
}

// TestReElection: Find the leader, isolate that machine from the network entirely.
// Wait for re-election on the remaining two. Reconnect the old leader - it should discover a higher term and
// demote to follower without disturbing the new leader. Then disconnect two of the three so quorum is lost.
// Advance simulated time past two election timeouts.
// Assert the one remaining connected server does not declare itself leader.
// Reconnect one - quorum restored - assert a leader is elected.
func TestReElection(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderGrpcAddr, err := waitForLeader(c, 3*time.Second)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}
	leaderAddr, err := getHttpFromGrpc(c, leaderGrpcAddr)
	if err != nil {
		t.Fatalf("Couldn't find http address of leader at %s", leaderGrpcAddr)
	}

	disconnect(c, leaderAddr)

	leaderGrpcAddr, err = waitForLeader(c, 3*time.Second)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	connect(c, leaderAddr)
	newLeaderGrpcAddr, err := waitForLeader(c, 3*time.Second)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	if leaderGrpcAddr != newLeaderGrpcAddr {
		t.Fatalf("Leader address changed, wanted %s got %s", leaderGrpcAddr, newLeaderGrpcAddr)
	}

	// Disconnect two nodes, with quorum lost leader will step down
	disconnect(c, c.addrs[0])
	disconnect(c, c.addrs[1])

	time.Sleep(2 * time.Second)

	_, err = waitForLeader(c, 3*time.Second)
	if err == nil {
		t.Fatal("expected no leader without quorum")
	}

	// Reconnect one node so we have quorum
	connect(c, c.addrs[0])

	_, err = waitForLeader(c, 3*time.Second)
	if err != nil {
		t.Fatalf("expected leader after reconnect, got %v", err)
	}
}

// TestManyElections: 7 machines, 10 rounds.
// Each round: randomly isolate 3 machines,
// assert the remaining 4 still have or elect a leader, then reconnect all 3.
// The random seed is fixed, so every failure is reproducible without extra tooling.
func TestManyElections(t *testing.T) {
	c := newCluster(7)
	time.Sleep(time.Second)

	rng := rand.New(rand.NewSource(gosimruntime.Seed()))
	for range 10 {
		perm := rng.Perm(len(c.addrs))
		isolated := perm[:3]
		for _, i := range isolated {
			disconnect(c, c.addrs[i])
		}
		_, err := waitForLeader(c, 3*time.Second)
		if err != nil {
			t.Fatalf("lost leader after isolation: %v", err)
		}

		for _, i := range isolated {
			connect(c, c.addrs[i])
		}
	}
}
