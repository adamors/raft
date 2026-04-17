package raft_test

import (
	"bytes"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/jellevandenhooff/gosim"
)

// TestAddServer simply adds a new server and tries to force strict commit after it's added
func TestAddServer(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	machineAddr := setupAddress(len(c.addrs) + 1)
	machine := setupMachine(machineAddr, []string{})
	resp, err := http.Post("http://"+leaderAddr.httpAddr+"/add-server/"+machineAddr.grpcAddr, "application/octet-stream", &bytes.Reader{})

	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("Error adding new server %v %d", err, resp.StatusCode)
	}
	resp.Body.Close()

	c.addrs = append(c.addrs, machineAddr)
	c.machines = append(c.machines, machine)

	cmd := []byte("555")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("all nodes didn't agree on commit %v", err)
	}
}

// TestRemoveServer removes a follower and makes sure consensus still works
func TestRemoveServer(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	var followerAddr address
	var followerMachine gosim.Machine
	for i, addr := range c.addrs {
		if addr.ip != leaderAddr.ip {
			followerAddr = addr
			followerMachine = c.machines[i]
			break
		}
	}

	if followerAddr == (address{}) {
		t.Fatal("Could not find a follower to remove")
	}

	resp, err := http.Post("http://"+leaderAddr.httpAddr+"/remove-server/"+followerAddr.grpcAddr, "application/octet-stream", &bytes.Reader{})

	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("Error removing server %v %d", err, resp.StatusCode)
	}

	resp.Body.Close()

	c.addrs = slices.DeleteFunc(c.addrs, func(addr address) bool {
		return addr.ip == followerAddr.ip
	})

	c.machines = slices.DeleteFunc(c.machines, func(machine gosim.Machine) bool {
		return machine.Label() == followerMachine.Label()
	})

	cmd := []byte("555")
	if _, err := submitAndVerifyCommand(c, leaderAddr, cmd, strictCommit); err != nil {
		t.Fatalf("all nodes didn't agree on commit %v", err)
	}
}

// TestConfigChangePending submits two add server requests at the same time, expecting the second one to fail
func TestConfigChangePending(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	addr1 := setupAddress(len(c.addrs) + 1)
	addr2 := setupAddress(len(c.addrs) + 2)
	setupMachine(addr1, []string{})
	setupMachine(addr2, []string{})

	// First config change, backgrounded so it doesn't block test
	go http.Post("http://"+leaderAddr.httpAddr+"/add-server/"+addr1.grpcAddr, "application/octet-stream", &bytes.Reader{})

	// Give the first request just enough time to reach the server and set
	// configChangePending, but not enough for applyTicker (10ms) to clear it.
	time.Sleep(1 * time.Millisecond)

	// This should now fail
	resp, err := http.Post("http://"+leaderAddr.httpAddr+"/add-server/"+addr2.grpcAddr, "application/octet-stream", &bytes.Reader{})
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("Expected config change pending error, got %v %d", err, resp.StatusCode)
	}
	resp.Body.Close()
}

// TestRemoveLeader attempt to remove the leader and it expects to fail
func TestRemoveLeader(t *testing.T) {
	c := newCluster(3)
	time.Sleep(time.Second)

	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		t.Fatalf("Expected no error from waitForLeader, got %v", err)
	}

	resp, err := http.Post("http://"+leaderAddr.httpAddr+"/remove-server/"+leaderAddr.grpcAddr, "application/octet-stream", &bytes.Reader{})
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("Expected error when removing leader server %v %d", err, resp.StatusCode)
	}

	resp.Body.Close()
}
