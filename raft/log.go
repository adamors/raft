package raft

type LogEntry struct {
	Term    int
	Command []byte
}

func (rf *Raft) logIndex(raftIndex int) int {
	return raftIndex - rf.lastIncludedIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}
