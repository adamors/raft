package persister

type Persister interface {
	Save(raftstate []byte, snapshot []byte) error
	ReadRaftState() []byte
	ReadSnapshot() []byte
	RaftStateSize() int
	SnapshotSize() int
}
