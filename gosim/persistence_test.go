package raft_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
)

type entry struct {
	index int
	data  []byte
}

// TestPersist1: 3 nodes. Submit a handful of commands, verify they commit.
// Crash all 3 machines. Restart all 3. Verify the cluster re-elects a leader
// (persisted `term`/`votedFor` means they don't start from term 0) and all
// previously committed entries are still in the log.
// Submit a new command and verify it commits.
func TestPersist1(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	entries := make([]entry, 3)

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		index, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit)
		if err != nil {
			t.Fatalf("submit failed %v", err)
		}
		entries[i] = entry{index: index, data: cmd}
	}

	for _, machine := range c.machines {
		machine.Crash()
	}

	for _, machine := range c.machines {
		machine.Restart()
	}

	leaderAddr, err = waitForLeaderAddr(c)

	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	// TODO move this to the bottom once we have no-op command
	cmd := []byte("500")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("submit failed %v", err)
	}

	for _, entry := range entries {
		val, err := readEntry(leaderAddr.httpAddr, entry.index)
		if err != nil {
			t.Fatalf("could not read committed index %d after recovery, got err %v", entry.index, err)
		}
		if !bytes.Equal(val, entry.data) {
			t.Fatal("Value at index didn't match the expected data")
		}
	}
}

// TestPersist2: 3 nodes. Iterative crash-and-restart: submit some commands,
// crash one server, submit more, restart it, submit more, crash the leader,
// restart it, and so on. At each step verify that commands submitted
// before any crash are still committed and consistent across all nodes.
// Tests that `currentTerm`, `votedFor`, and the log are all persisted and
// restored correctly under repeated partial failures.
func TestPersist2(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	machine, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Got error while trying to crash a machine %v", err)
	}

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	machine.Restart()

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	leaderMachine, err := crashLeader(c, leaderAddr)
	if err != nil {
		t.Fatalf("Could not crash leader machine %v", leaderMachine)
	}
	newLeaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, newLeaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	leaderMachine.Restart()

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, newLeaderAddr, cmd, strictCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}
}

// TestPersist3: 3 nodes. Disconnect a follower. Submit enough commands to the
// remaining two that the log grows large. Reconnect the follower - it's missing
// many entries. Verify it catches up entirely from log entries (no snapshot).
// Then repeat, but take a snapshot before reconnection so the follower must
// also handle receiving an `InstallSnapshot`.
// Tests the interaction between crash recovery, log replay, and snapshot
// installation.
func TestPersist3(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	machine, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Got error while trying to crash a machine %v", err)
	}

	// these should not trigger snapshot since we have maxraftstate=1000
	for i := range 20 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	machine.Restart()

	cmd := []byte("999")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("follower did not catch up via log replay: %v", err)
	}

	machine, err = crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Got error while trying to crash a machine %v", err)
	}
	// this should trigger snapshot
	for i := range 50 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	machine.Restart()

	cmd = []byte("777")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("follower did not catch up via snapshot installation: %v", err)
	}
}

// TestFigure8: Repeatedly create the Figure 8 safety condition from the Raft
// paper under controlled churn.
// 5 nodes. In each round, whichever server is currently leader tries to append
// a command, then is likely to fail shortly afterward, often before that entry
// is committed. If too few servers remain alive to form a majority, restart one.
// This creates many terms containing partially replicated, often uncommitted
// entries, and exercises the rule that leaders must not consider old-term
// entries committed just by counting replicas.
// After the churn ends, restore the full cluster and verify it can still commit
// a new command safely on all servers.
func TestFigure8(t *testing.T) {
	// by default gosim times out after 10mins
	gosim.SetSimulationTimeout(30 * time.Minute)

	servers := 5
	c := newCluster(servers)
	time.Sleep(time.Second)

	restartOneIfNeeded := func() {
		if servers-len(c.crash) >= servers/2+1 {
			return
		}
		for _, m := range c.machines {
			if c.crash[m.Label()] {
				m.Restart()
				delete(c.crash, m.Label())
				return
			}
		}
	}

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("initial leader election failed: %v", err)
	}
	var committed []entry
	initial := entry{data: []byte("init")}
	index, err := submitAndVerifyCommand(c, leaderAddr, initial.data, strictCommit)
	if err != nil {
		t.Fatalf("initial command failed: %v", err)
	}
	initial.index = index
	committed = append(committed, initial)

	for iter := range 1000 {
		if iter > 0 && iter%200 == 0 {
			leaderAddr, err := waitForLeaderAddr(c)
			if err != nil { // we could be mid-election here
				continue
			}

			cmd := fmt.Appendf(nil, "committed-%d", iter)
			index, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit)
			if err != nil {
				continue
			}
			committed = append(committed, entry{index: index, data: cmd})
		}

		leaderGrpc, err := waitForLeader(c, 500*time.Millisecond)
		if err != nil {
			restartOneIfNeeded()
			continue
		}

		addr, err := getHttpFromGrpc(c, leaderGrpc)
		if err != nil {
			t.Fatalf("could not resolve leader grpc address %q: %v", leaderGrpc, err)
		}

		if _, err := submitCommand(addr, []byte(strconv.Itoa(iter))); err != nil {
			continue
		}

		if rand.Int()%1000 < 100 {
			time.Sleep(time.Duration(rand.Int63()%500) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Int63()%13) * time.Millisecond)
		}

		if _, err := crashLeader(c, addr); err != nil {
			continue
		}

		restartOneIfNeeded()
	}

	for _, m := range c.machines {
		if c.crash[m.Label()] {
			m.Restart()
			delete(c.crash, m.Label())
		}
	}

	leaderAddr, err = waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("final leader election failed: %v", err)
	}

	if _, err := submitAndVerifyCommand(c, leaderAddr, []byte("final"), strictCommit); err != nil {
		t.Fatalf("final command failed: %v", err)
	}

	for _, entry := range committed {
		val, err := readEntry(leaderAddr.httpAddr, entry.index)
		if err != nil {
			t.Fatalf("could not read committed index %d after recovery, got err %v", entry.index, err)
		}
		if !bytes.Equal(val, entry.data) {
			t.Fatalf("committed index %d was overwritten: expected %q got %q", entry.index, entry.data, val)
		}
	}
}
