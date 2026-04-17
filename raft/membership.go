package raft

import (
	"bytes"
	"encoding/gob"
)

const (
	addServerChange uint8 = iota
	removeServerChange
)

type Server struct {
	Address string
}

type Configuration struct {
	Servers []Server
}

type ConfigurationFuture struct {
	configuration Configuration
}

func (f *ConfigurationFuture) Error() error {
	return nil
}

func (f *ConfigurationFuture) Configuration() Configuration {
	return f.configuration
}

func (rf *Raft) makeConfigChange(address string, changeType uint8) (int, int, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader {
		return -1, -1, ErrNotLeader
	}
	// We can have a single config change at once
	if rf.configChangePending {
		return -1, -1, ErrConfigChangePending
	}

	var entry LogEntry
	var err error
	switch changeType {
	case addServerChange:
		if ok := rf.transport.Dial(address); !ok {
			return -1, -1, ErrAddrInvalid
		}
		entry = rf.addServerLogEntry(address)
	case removeServerChange:
		if address == rf.me {
			return -1, -1, ErrCannotRemoveLeader
		}
		entry, err = rf.removeServerLogEntry(address)
		if err != nil {
			return -1, -1, err
		}
	}

	rf.log = append(rf.log, entry)

	rf.heartbeat()
	rf.persist()

	index := rf.lastLogIndex()
	term := rf.currentTerm

	return index, term, nil
}

func encodeAddrs(addrs []string) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(addrs)
	return buf.Bytes()
}

func (rf *Raft) addServerLogEntry(address string) LogEntry {
	rf.configChangePending = true

	addrs := append(rf.transport.Peers(), address)

	return LogEntry{
		Term:    rf.currentTerm,
		Command: encodeAddrs(addrs),
		Type:    ConfigurationType,
	}
}

func (rf *Raft) removeServerLogEntry(address string) (LogEntry, error) {
	found := false
	addrs := make([]string, 0, rf.transport.NumPeers()-1)
	for _, peer := range rf.transport.Peers() {
		if address == peer {
			found = true
			continue
		}
		addrs = append(addrs, peer)
	}

	if !found {
		return LogEntry{}, ErrAddrInvalid
	}

	rf.configChangePending = true

	return LogEntry{
		Term:    rf.currentTerm,
		Command: encodeAddrs(addrs),
		Type:    ConfigurationType,
	}, nil
}
