package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

type persistedState struct {
	CurrentTerm       int
	VotedFor          string
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (rf *Raft) persist() {
	var w bytes.Buffer
	if err := gob.NewEncoder(&w).Encode(persistedState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}); err != nil {
		log.Printf("failed to encode raft state: %v", err)
		return
	}
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	var state persistedState
	if err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&state); err != nil {
		log.Printf("failed to decode raft state: %v", err)
		return
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.lastIncludedIndex = state.LastIncludedIndex
	rf.lastIncludedTerm = state.LastIncludedTerm
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
