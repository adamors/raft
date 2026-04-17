package raft

type LogEntry struct {
	Term    int
	Command []byte
	Addrs   []string
	Type    LogEntryType
}
type LogEntryType uint8

const (
	CommandType LogEntryType = iota
	NoOpType
	ConfigurationType
)

func (rf *Raft) logIndex(raftIndex int) int {
	return raftIndex - rf.lastIncludedIndex
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}
