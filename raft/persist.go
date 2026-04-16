package raft

import (
	"bytes"
	"encoding/gob"
	"log"
)

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil || d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("Error reading from persistence")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logEntries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
