package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int // term of conflicting entry
	XIndex int // index of first entry w/ XTerm
	XLen   int // length of followers log
}

func (rf *Raft) sendAppendEntries(peer string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.transport.Call(peer, "Raft.AppendEntries", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDownAsLeader(args.Term)
	}

	reply.Term = rf.currentTerm

	// 1. Reject stale requests. Don't update lastHeartbeat or step down from
	// leadership for a stale AppendEntries — that would wrongly demote a
	// newly-elected leader receiving in-flight messages from an old leader.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// args.Term == rf.currentTerm: legitimate current leader.
	rf.lastHeartbeat = time.Now()
	rf.isLeader = false

	rf.leaderId = args.LeaderId

	// 2. part 1 - log too short
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.XLen = rf.lastLogIndex() + 1
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}

	// 2. part 1.5 - PrevLogIndex is in our snapshot (already have it)
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		return
	}

	// 2. part 2 - term mismatch
	reply.XIndex = 0
	if rf.log[rf.logIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[rf.logIndex(args.PrevLogIndex)].Term

		for i := args.PrevLogIndex; i >= rf.lastIncludedIndex; i-- {
			if rf.log[rf.logIndex(i)].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
			if i == rf.lastIncludedIndex {
				reply.XIndex = rf.lastIncludedIndex
			}
		}
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// 3.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= rf.lastLogIndex() {
			if rf.log[rf.logIndex(index)].Term != entry.Term {
				rf.log = rf.log[:rf.logIndex(index)]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else { // 4.
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	// 5.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	reply.Success = true
}

func (rf *Raft) updateCommitIndex() {
	// caller holds lock

	// Find highest N where majority have matchIndex[i] >= N
	// and log[N].term == currentTerm
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[rf.logIndex(n)].Term != rf.currentTerm {
			continue // Can only commit entries from current term
		}

		count := 1 // Count self
		for _, peer := range rf.transport.Peers() {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}

		if count > rf.transport.NumPeers()/2 {
			rf.commitIndex = n
			break
		}
	}
}

func (rf *Raft) sendEntriesForFollower(peer string, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}

	if ok := rf.sendAppendEntries(peer, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastAckTime[peer] = time.Now()

	if rf.currentTerm != args.Term || !rf.isLeader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownAsLeader(reply.Term)
		return
	}

	if reply.Success {
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.updateCommitIndex()
	} else { // follower is behind
		if reply.XTerm == -1 {
			rf.nextIndex[peer] = reply.XLen
		} else {
			rf.nextIndex[peer] = reply.XIndex
			rf.nextIndex[peer] = max(rf.lastIncludedIndex+1, rf.nextIndex[peer])
		}
	}
}

func (rf *Raft) heartbeat() {
	// caller holds lock
	rf.updateCommitIndex()

	term := rf.currentTerm
	commitIndex := rf.commitIndex

	for _, peer := range rf.transport.Peers() {
		if peer == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[peer] - 1

		if prevLogIndex < rf.lastIncludedIndex {
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.snapshot,
				Done:              true,
			}
			go rf.sendInstallSnapshotToFollower(peer, args)
		} else {
			prevLogTerm := rf.log[rf.logIndex(prevLogIndex)].Term
			entries := make([]LogEntry, len(rf.log[rf.logIndex(prevLogIndex+1):]))
			copy(entries, rf.log[rf.logIndex(prevLogIndex+1):])

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			go rf.sendEntriesForFollower(peer, args)
		}
	}
}

func (rf *Raft) applyConfig(entry LogEntry) error {
	var addrs []string
	buf := bytes.NewBuffer(entry.Command)
	if err := gob.NewDecoder(buf).Decode(&addrs); err != nil {
		log.Printf("failed to decode config entry %v", err)
		return ErrInternalError
	}

	rf.transport.ReplacePeers(addrs)

	rf.mu.Lock()
	rf.configChangePending = false
	for _, peer := range addrs {
		if _, ok := rf.nextIndex[peer]; !ok {
			rf.nextIndex[peer] = rf.lastLogIndex() + 1
			rf.matchIndex[peer] = 0
		}
	}
	rf.mu.Unlock()

	return nil
}

func (rf *Raft) applyTicker() {
	defer rf.wg.Done()

	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			currentTerm := rf.currentTerm
			lastEntry := rf.log[rf.logIndex(rf.lastApplied)]
			entryCurrentTerm := lastEntry.Term

			index := rf.lastApplied
			applyLog := &Log{
				Data:  lastEntry.Command,
				Index: rf.lastApplied,
				Term:  entryCurrentTerm,
			}
			rf.mu.Unlock()
			var result any
			var err error
			switch lastEntry.Type {
			case CommandType:
				result = rf.fsm.Apply(applyLog)
			case NoOpType:
				result = struct{}{}
			case ConfigurationType:
				err = rf.applyConfig(lastEntry)
			}

			rf.futuresMu.Lock()
			if future, ok := rf.futures[index]; ok {
				if future.term != currentTerm {
					future.err = ErrLeadershipChanged
				} else {
					if err != nil {
						future.err = err
					}
					future.response = result
				}
				close(future.done)
				delete(rf.futures, index)
			}
			rf.futuresMu.Unlock()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		if rf.maxraftstate != -1 && rf.PersistBytes() >= rf.maxraftstate {
			rf.Snapshot(rf.lastApplied)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) heartbeatTicker() {
	defer rf.wg.Done()

	for rf.killed() == false {
		rf.mu.Lock()
		if rf.isLeader {
			// Check whether enough followers are still online to make quorum.
			// If we haven't heard back from too many followers, step down.
			acks := 1 // count self
			for _, peer := range rf.transport.Peers() {
				if peer != rf.me && time.Since(rf.lastAckTime[peer]) < rf.electionTimeout {
					acks++
				}
			}
			if acks <= rf.transport.NumPeers()/2 {
				rf.stepDownAsLeader(rf.currentTerm)
			} else {
				rf.heartbeat()
			}
		}
		rf.mu.Unlock()

		time.Sleep(rf.heartbeatInterval)
	}
}
