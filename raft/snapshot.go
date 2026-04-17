package raft

import (
	"bytes"
	"log"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(peer string, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.transport.Call(peer, "Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) Snapshot(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return // already snapshotted past this point
	}

	newLastIncludedTerm := rf.log[rf.logIndex(index)].Term

	rf.log = append([]LogEntry{{Term: newLastIncludedTerm}}, rf.log[rf.logIndex(index)+1:]...)

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = newLastIncludedTerm

	snapshot, err := rf.fsm.Snapshot()
	if err != nil {
		log.Fatal("Error snapshotting fsm")
	}
	var buffer bytes.Buffer
	if err := snapshot.Persist(&buffer); err != nil {
		log.Fatal("Error snapshotting fsm")
	}
	rf.snapshot = buffer.Bytes()

	rf.persist()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownAsLeader(args.Term)
	}

	rf.lastHeartbeat = time.Now()
	rf.isLeader = false

	if args.LastIncludedIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	rf.snapshot = args.Data
	sliceIdx := args.LastIncludedIndex - rf.lastIncludedIndex

	if sliceIdx < len(rf.log) && rf.log[sliceIdx].Term == args.LastIncludedTerm {
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.log[sliceIdx+1:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = args.LastIncludedIndex

	rf.persist()

	rf.fsm.Restore(bytes.NewReader(rf.snapshot))

	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshotToFollower(peer string, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(peer, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term || !rf.isLeader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownAsLeader(reply.Term)
		return
	}

	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
}
