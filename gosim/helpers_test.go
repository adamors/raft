package raft_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"

	"github.com/adamors/raft/persister"
	"github.com/adamors/raft/raft"
	"github.com/adamors/raft/server"
	"github.com/jellevandenhooff/gosim"
)

const (
	GRPC_PORT = "7000"
	HTTP_PORT = "7001"
)

type address struct {
	ip       string
	grpcAddr string
	httpAddr string
}

type raftState struct {
	term     int
	isLeader bool
	leader   string
}

type cluster struct {
	addrs    []address
	machines []gosim.Machine
	crash    map[string]bool
}

func newCluster(count int) *cluster {
	addrs := setupAddresses(count)
	machines := setupMachines(addrs)
	return &cluster{
		addrs:    addrs,
		machines: machines,
		crash:    map[string]bool{},
	}
}

func setupAddresses(count int) []address {
	addressses := make([]address, count)
	for i := range count {
		ip := fmt.Sprintf("10.0.0.%d", i+1)
		addressses[i] = address{
			ip:       ip,
			grpcAddr: ip + ":" + GRPC_PORT,
			httpAddr: ip + ":" + HTTP_PORT,
		}
	}
	return addressses
}

func setupMachines(addrs []address) []gosim.Machine {
	machines := make([]gosim.Machine, len(addrs))
	ips := make([]string, len(addrs))
	for i, addr := range addrs {
		ips[i] = addr.grpcAddr
	}

	for i, addr := range addrs {
		p := persister.NewInMemoryPersister()
		machines[i] = gosim.NewMachine(gosim.MachineConfig{
			Addr: netip.MustParseAddr(addr.ip),
			MainFunc: func() {
				transport, _ := raft.NewGrpcTransport(ips)
				persister := p
				cfg := raft.DefaultConfig()
				cfg.MaxRaftState = 1000
				server := server.NewGRPCServer(i, transport, persister, cfg)
				go server.ServeStatus(addr.httpAddr)
				l, err := net.Listen("tcp", addr.grpcAddr)
				if err != nil {
					fmt.Printf("Error setting listener, %v", err)
				}
				server.Serve(l)
			},
		})
	}
	return machines
}

func disconnect(c *cluster, isolatedAddr address) {
	for _, addr := range c.addrs {
		if addr.ip != isolatedAddr.ip {
			gosim.SetConnected(isolatedAddr.ip, addr.ip, false)
		}
	}
}

func connect(c *cluster, isolatedAddr address) {
	for _, addr := range c.addrs {
		if addr.ip != isolatedAddr.ip {
			gosim.SetConnected(isolatedAddr.ip, addr.ip, true)
		}
	}
}

func getState(addr address) (raftState, error) {
	var state raftState
	resp, err := http.Get("http://" + addr.httpAddr + "/state")
	if err != nil {
		return state, err
	}
	defer resp.Body.Close()

	fmt.Fscanf(resp.Body, "%d %v %s", &state.term, &state.isLeader, &state.leader)
	return state, nil
}

func waitForLeader(c *cluster, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, addr := range c.addrs {
			state, err := getState(addr)
			if err != nil {
				// we poll all machines, crashed ones will not respond.
				continue
			}
			if state.isLeader {
				return state.leader, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", errors.New("no leader elected")
}

func waitForLeaderAddr(c *cluster) (address, error) {
	leaderGrpcAddr, err := waitForLeader(c, 3*time.Second)
	if err != nil {
		return address{}, fmt.Errorf("Got error waiting for leader %w", err)
	}

	leaderAddr, err := getHttpFromGrpc(c, leaderGrpcAddr)
	if err != nil {
		return address{}, fmt.Errorf("Could not find leader's address %w", err)
	}

	return leaderAddr, nil
}

func submitAndVerifyCommand(c *cluster, leaderAddr address, cmd []byte, mode commitMode) (int, error) {
	index, err := submitCommand(leaderAddr, cmd)
	if err != nil {
		return -1, fmt.Errorf("Expected no error from submitting command, got %w", err)
	}
	commit, err := waitForCommit(c, index, 3*time.Second, mode)
	if err != nil {
		return -1, fmt.Errorf("Expected no error from waitForCommit %w", err)
	}
	if !bytes.Equal(cmd, commit) {
		return -1, fmt.Errorf("Expected %v, got %v", cmd, commit)
	}
	return index, nil
}

func getHttpFromGrpc(c *cluster, grpcAddr string) (address, error) {
	for _, addr := range c.addrs {
		if addr.grpcAddr == grpcAddr {
			return addr, nil
		}
	}
	return address{}, fmt.Errorf("Couldn't find grpcAddr %s", grpcAddr)
}

func crashFollower(c *cluster, leaderAddr address) (gosim.Machine, error) {
	leaderMachine, err := getMachineByAddr(c, leaderAddr.httpAddr)
	if err != nil {
		return gosim.Machine{}, err
	}

	for _, machine := range c.machines {
		if machine.Label() != leaderMachine.Label() && !c.crash[machine.Label()] {
			machine.Crash()
			c.crash[machine.Label()] = true
			return machine, nil
		}
	}

	return gosim.Machine{}, fmt.Errorf("no follower available to crash")
}

func crashLeader(c *cluster, leaderAddr address) (gosim.Machine, error) {
	machine, err := getMachineByAddr(c, leaderAddr.httpAddr)
	if err != nil {
		return gosim.Machine{}, err
	}
	if c.crash[machine.Label()] {
		return gosim.Machine{}, fmt.Errorf("Leader already crashed")
	}

	machine.Crash()
	c.crash[machine.Label()] = true
	return machine, nil
}

func getMachineByAddr(c *cluster, httpAddr string) (gosim.Machine, error) {
	for i, addr := range c.addrs {
		if addr.httpAddr == httpAddr {
			return c.machines[i], nil
		}
	}
	return gosim.Machine{}, fmt.Errorf("Could not find address %s", httpAddr)
}

func submitCommand(leaderAddr address, cmd []byte) (int, error) {
	resp, err := http.Post("http://"+leaderAddr.httpAddr+"/start", "appliction/octet-stream", bytes.NewReader(cmd))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var index int
	fmt.Fscanf(resp.Body, "%d", &index)
	return index, nil
}

type commitMode int

const (
	strictCommit commitMode = iota
	quorumCommit
)

func waitForCommit(c *cluster, index int, timeout time.Duration, mode commitMode) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	required := len(c.addrs)
	if mode == quorumCommit {
		required = len(c.addrs)/2 + 1
	}

	for time.Now().Before(deadline) {
		committed := 0
		var val, firstVal []byte
		hasFirst := false
		for _, addr := range c.addrs {
			v, err := readEntry(addr.httpAddr, index)
			if err != nil {
				continue
			}
			val = v
			if !hasFirst {
				firstVal = bytes.Clone(val)
				hasFirst = true
			} else if !bytes.Equal(firstVal, val) {
				return val, errors.New("mismatch between commits")
			}
			committed++
		}
		if committed >= required {
			return val, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, errors.New("commit timed out")
}

func readEntry(httpAddr string, index int) ([]byte, error) {
	var val []byte
	resp, err := http.Get(fmt.Sprintf("http://%s/logs/%d", httpAddr, index))
	if err != nil {
		return val, err
	}
	if resp.StatusCode != 200 {
		return val, fmt.Errorf("Got %d status code", resp.StatusCode)
	}
	val, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	return val, nil
}

// createMinorityCluster removes the leader and the follower from an existing cluster
// and creates a minority cluster with them.
func createMinorityCluster(c *cluster) (*cluster, error) {
	leaderAddr, err := waitForLeaderAddr(c)
	if err != nil {
		return nil, fmt.Errorf("Leader election failed %w", err)
	}

	var followerAddr address
	for _, addr := range c.addrs {
		if leaderAddr.ip != addr.ip {
			followerAddr = addr
			break
		}
	}

	disconnect(c, leaderAddr)
	disconnect(c, followerAddr)

	machine1, err := getMachineByAddr(c, leaderAddr.httpAddr)
	if err != nil {
		return nil, err
	}
	machine2, err := getMachineByAddr(c, followerAddr.httpAddr)
	if err != nil {
		return nil, err
	}

	trimmedAddrs := make([]address, 0, len(c.addrs)-2)
	for _, addr := range c.addrs {
		if addr.ip == leaderAddr.ip || addr.ip == followerAddr.ip {
			continue
		}
		trimmedAddrs = append(trimmedAddrs, addr)
	}
	c.addrs = trimmedAddrs

	trimmedMachines := make([]gosim.Machine, 0, len(c.machines)-2)
	for _, machine := range c.machines {
		if machine.Label() == machine1.Label() || machine.Label() == machine2.Label() {
			continue
		}
		trimmedMachines = append(trimmedMachines, machine)
	}
	c.machines = trimmedMachines

	//connect the two isolated machines
	gosim.SetConnected(leaderAddr.ip, followerAddr.ip, true)

	return &cluster{
		addrs:    []address{leaderAddr, followerAddr},
		machines: []gosim.Machine{machine1, machine2},
		crash:    map[string]bool{},
	}, nil
}

// healCluster adds back the machines/addresses of minority cluster to majority
// cluster, then connects the machines as well
func healCluster(majority *cluster, minority *cluster) {
	for _, machine := range minority.machines {
		majority.machines = append(majority.machines, machine)
	}
	for _, addr := range minority.addrs {
		majority.addrs = append(majority.addrs, addr)
		connect(majority, addr)
	}
}
