package raft_test

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
)

// TestSnapshotInstall: 3 nodes. Disconnect one follower. Submit many commands
// to the other two — enough to trigger multiple snapshots, discarding log
// entries before the snapshot index. Reconnect the follower.
// It's so far behind that the leader no longer has the log entries it needs
// and must send `InstallSnapshot`. Verify the follower's FSM state matches
// the others after installation, and that it can commit new entries
// after catching up.
func TestSnapshotInstall(t *testing.T) {
	// by default gosim times out after 10mins
	gosim.SetSimulationTimeout(30 * time.Minute)

	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	machine, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash machine %v", err)
	}

	// maxraftstate is 1000 bytes, this should trigger log compaction
	for i := range 500 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	machine.Restart()
	time.Sleep(time.Second)

	cmd := []byte("000")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("submit failed %v", err)
	}
}

// TestSnapshotAllCrash: 3 nodes. Submit many commands, cross several snapshot
// boundaries. Crash all 3 machines simultaneously. Restart all 3. They must
// restore from their snapshots, re-elect a leader, and agree on FSM state
// without replaying log entries before the snapshot index.
// Submit new commands and verify they commit normally. Tests cold-start from snapshot.
func TestSnapshotAllCrash(t *testing.T) {
	// by default gosim times out after 10mins
	gosim.SetSimulationTimeout(30 * time.Minute)

	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	// maxraftstate is 1000 bytes, this should trigger log compaction
	for i := range 500 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	time.Sleep(time.Second)

	follower1, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash follower1 %v", err)
	}
	follower2, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash follower2 %v", err)
	}
	leader, err := crashLeader(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash leader %v", err)
	}

	follower1.Restart()
	follower2.Restart()
	leader.Restart()

	leaderAddr, err = waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	entries := []entry{
		{index: 1, data: []byte("100")},
		{index: 100, data: []byte("10000")},
		{index: 200, data: []byte("20000")},
	}
	for _, entry := range entries {
		val, err := readEntry(leaderAddr.httpAddr, entry.index)
		if err != nil {
			t.Fatalf("could not read committed index %d after recovery, got err %v", entry.index, err)
		}
		if !bytes.Equal(val, entry.data) {
			t.Fatalf("Value at index %d didn't match the expected data", entry.index)
		}
	}

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 900) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}
}
