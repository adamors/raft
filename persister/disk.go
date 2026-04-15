package persister

import (
	"os"
	"sync"
)

type DiskPersister struct {
	mu            sync.Mutex
	raftStatePath string
	snapshotPath  string
	InMemoryPersister
}

var _ Persister = (*DiskPersister)(nil)

func NewDiskPersister(raftStatePath, snapshotPath string) (*DiskPersister, error) {
	dp := &DiskPersister{
		mu:                sync.Mutex{},
		raftStatePath:     raftStatePath,
		snapshotPath:      snapshotPath,
		InMemoryPersister: *NewInMemoryPersister(),
	}
	err := dp.loadFromDisk()
	if err != nil {
		return &DiskPersister{}, err
	}
	return dp, nil
}

func (p *DiskPersister) Save(state []byte, snapshot []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.InMemoryPersister.Save(state, snapshot)

	// Snapshot the bytes now so we don't hold InMemoryPersister's lock across I/O.
	savedState := p.InMemoryPersister.ReadRaftState()
	savedSnapshot := p.InMemoryPersister.ReadSnapshot()

	tmpState, err := os.CreateTemp(os.TempDir(), "state")
	if err != nil {
		return err
	}
	defer tmpState.Close()

	tmpSnap, err := os.CreateTemp(os.TempDir(), "snap")
	if err != nil {
		return err
	}
	defer tmpSnap.Close()

	_, err = tmpState.Write(savedState)
	if err != nil {
		return err
	}
	_, err = tmpSnap.Write(savedSnapshot)
	if err != nil {
		return err
	}

	err = os.Rename(tmpState.Name(), p.raftStatePath)
	if err != nil {
		return err
	}

	err = os.Rename(tmpSnap.Name(), p.snapshotPath)
	if err != nil {
		return err
	}

	return nil
}

func (p *DiskPersister) loadFromDisk() error {
	diskState, err := os.ReadFile(p.raftStatePath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	diskSnap, err := os.ReadFile(p.snapshotPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// if one or both of these are nil, it's handled
	p.InMemoryPersister.Save(diskState, diskSnap)
	return nil
}
