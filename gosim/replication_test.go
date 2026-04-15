package raft_test

import (
	"strconv"
	"testing"
	"time"
)

// TestBasicAgree: 3 nodes. Submit 3 commands sequentially via `Start()`.
// For each command, poll all machines until all 3 have committed it.
// Verifies that in a healthy cluster, commands commit to all peers.
func TestBasicAgree(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}
}

// TestFollowerFailure: 5 nodes. Submit a batch of commands, verify they commit.
// Crash follower 0, submit more commands, verify they still commit on the
// remaining quorum. Crash another follower, same thing.
// Then crash a third - now only 2 remain, no quorum - submit a command and
// assert it never commits. Revive all crashed servers, verify they catch up.
func TestFollowerFailure(t *testing.T) {
	c := newCluster(5)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	for i := range 5 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	crashedMachine, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash machine %v", err)
	}

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	crashedMachine2, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash machine %v", err)
	}

	for i := range 3 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	crashedMachine3, err := crashFollower(c, leaderAddr)
	if err != nil {
		t.Fatalf("Failed to crash machine %v", err)
	}
	cmd := []byte("500")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err == nil {
		t.Fatal("submit should have failed")
	}

	crashedMachine.Restart()
	crashedMachine2.Restart()
	crashedMachine3.Restart()

	time.Sleep(time.Second)

	leaderAddr, err = waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("no leader after restart: %v", err)
	}
	cmd = []byte("999")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("expected all nodes to catch up after restart: %v", err)
	}

}

// TestLeaderFailure: 5 nodes. Find the leader, crash it. Wait for re-election
// on the remaining 4. Submit a command, verify it commits.
// Find the new leader, crash it. Repeat.
// Tests that the cluster survives repeated leader turnover without losing
// committed entries.
func TestLeaderFailure(t *testing.T) {
	c := newCluster(5)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	for range 2 {
		if _, err := crashLeader(c, leaderAddr); err != nil {
			t.Fatalf("Could not crash leader %v", err)
		}

		leaderAddr, err = waitForLeaderAddr(c)
		if err != nil {
			t.Fatalf("Leader election failed %v", err)
		}

		cmd := []byte("999")
		if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("expected commit to submit: %v", err)
		}
	}
}

// TestRejoin: 3 nodes. Crash the leader. Submit a couple of commands to the
// remaining two - they elect a new leader and commit. Restart the old leader.
// It wakes up with a stale log and stale term.
// Verify it receives `AppendEntries` to catch up, doesn't disrupt the cluster,
// and eventually has all committed entries. Tests that a returning ex-leader
// correctly defers to the new term.
func TestRejoin(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}
	oldLeaderMachine, err := crashLeader(c, leaderAddr)
	if err != nil {
		t.Fatalf("Could not crash leader %v", err)
	}

	newLeaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	for i := range 5 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(c, newLeaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed %v", err)
		}
	}

	oldLeaderMachine.Restart()
	time.Sleep(time.Second)

	leaderAddr, err = waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Leader election failed %v", err)
	}

	if leaderAddr != newLeaderAddr {
		t.Fatalf("Old leader distrupted the cluster")
	}

	cmd := []byte("123")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("all nodes didn't agree on commit %v", err)
	}
}

// TestBackup: 5 nodes. Isolate the leader + 1 follower into a minority partition.
// The other 3 elect a new leader. Submit many commands to the minority side -
// they build up uncommitted log entries. Submit many commands to the majority
// side  - they commit. Heal the partition. The old leader will have many
// conflicting uncommitted entries that need to be rolled back.
// Verify the log is corrected and all 5 eventually agree.
func TestBackup(t *testing.T) {
	majority := newCluster(5)
	time.Sleep(time.Second)

	minority, err := createMinorityCluster(majority)
	if err != nil {
		t.Fatalf("Error creating minority cluster %v", err)
	}

	majorityLeaderAddr, err := waitForLeaderAddr(majority)
	if err != nil {
		t.Fatalf("Leader election failed for majority cluster %v", err)
	}

	minorityLeaderAddr, err := waitForLeaderAddr(minority)
	if err != nil {
		t.Fatalf("Leader election failed for minority cluster %v", err)
	}

	for i := range 10 {
		cmd := []byte(strconv.Itoa((i + 1) * 100))
		if _, err := submitAndVerifyCommand(majority, majorityLeaderAddr, cmd, quorumCommit); err != nil {
			t.Fatalf("submit failed for majority cluster %v", err)
		}

		// fire into the minority to build up conflicting uncommitted entries
		cmd = []byte(strconv.Itoa((i + 1) * 200))
		submitCommand(minorityLeaderAddr, cmd)
	}

	healCluster(majority, minority)
	time.Sleep(time.Second)

	majorityLeaderAddr, err = waitForLeaderAddr(majority)
	if err != nil {
		t.Fatalf("Leader election failed for majority cluster %v", err)
	}

	cmd := []byte("555")
	if _, err := submitAndVerifyCommand(majority, majorityLeaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("all nodes didn't agree on commit %v", err)
	}
}
