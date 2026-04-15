package persister

import (
	"bytes"
	"sync"
)

type InMemoryPersister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

var _ Persister = (*InMemoryPersister)(nil)

func NewInMemoryPersister() *InMemoryPersister {
	return &InMemoryPersister{}
}

func (p *InMemoryPersister) Save(state []byte, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.raftstate = bytes.Clone(state)
	p.snapshot = bytes.Clone(snapshot)
	return nil
}

func (p *InMemoryPersister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	return bytes.Clone(p.raftstate)
}

func (p *InMemoryPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	return bytes.Clone(p.snapshot)
}

func (p *InMemoryPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.raftstate)
}

func (p *InMemoryPersister) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.snapshot)
}
