package persister_test

import (
	"path/filepath"
	"testing"

	"github.com/adamors/raft/persister"
	"github.com/stretchr/testify/require"
)

func TestInMemoryPersister(t *testing.T) {
	t.Run("empty on creation", func(t *testing.T) {
		p := persister.NewInMemoryPersister()
		require.Nil(t, p.ReadRaftState())
		require.Nil(t, p.ReadSnapshot())
		require.Equal(t, 0, p.RaftStateSize())
		require.Equal(t, 0, p.SnapshotSize())
	})

	t.Run("save and read back", func(t *testing.T) {
		p := persister.NewInMemoryPersister()
		state := []byte("raft state")
		snap := []byte("snapshot data")
		require.NoError(t, p.Save(state, snap))
		require.Equal(t, state, p.ReadRaftState())
		require.Equal(t, snap, p.ReadSnapshot())
		require.Equal(t, len(state), p.RaftStateSize())
		require.Equal(t, len(snap), p.SnapshotSize())
	})

	t.Run("returns copies not references", func(t *testing.T) {
		p := persister.NewInMemoryPersister()
		require.NoError(t, p.Save([]byte("original"), nil))
		got := p.ReadRaftState()
		got[0] = 'X'
		require.Equal(t, []byte("original"), p.ReadRaftState())
	})

	t.Run("overwrite replaces both state and snapshot", func(t *testing.T) {
		p := persister.NewInMemoryPersister()
		require.NoError(t, p.Save([]byte("first"), []byte("snap1")))
		require.NoError(t, p.Save([]byte("second"), []byte("snap2")))
		require.Equal(t, []byte("second"), p.ReadRaftState())
		require.Equal(t, []byte("snap2"), p.ReadSnapshot())
	})

	t.Run("nil state and snapshot are handled", func(t *testing.T) {
		p := persister.NewInMemoryPersister()
		require.NoError(t, p.Save(nil, nil))
		require.Nil(t, p.ReadRaftState())
		require.Nil(t, p.ReadSnapshot())
		require.Equal(t, 0, p.RaftStateSize())
		require.Equal(t, 0, p.SnapshotSize())
	})
}

func TestDiskPersister(t *testing.T) {
	t.Run("empty on first creation", func(t *testing.T) {
		dir := t.TempDir()
		p, err := persister.NewDiskPersister(
			filepath.Join(dir, "state"),
			filepath.Join(dir, "snapshot"),
		)
		require.NoError(t, err)
		require.Nil(t, p.ReadRaftState())
		require.Nil(t, p.ReadSnapshot())
	})

	t.Run("persists to disk and reloads", func(t *testing.T) {
		dir := t.TempDir()
		statePath := filepath.Join(dir, "state")
		snapPath := filepath.Join(dir, "snapshot")

		state := []byte("persistent state")
		snap := []byte("persistent snapshot")

		p1, err := persister.NewDiskPersister(statePath, snapPath)
		require.NoError(t, err)
		require.NoError(t, p1.Save(state, snap))

		p2, err := persister.NewDiskPersister(statePath, snapPath)
		require.NoError(t, err)
		require.Equal(t, state, p2.ReadRaftState())
		require.Equal(t, snap, p2.ReadSnapshot())
	})

	t.Run("overwrites previous data on disk", func(t *testing.T) {
		dir := t.TempDir()
		statePath := filepath.Join(dir, "state")
		snapPath := filepath.Join(dir, "snapshot")

		p1, err := persister.NewDiskPersister(statePath, snapPath)
		require.NoError(t, err)
		require.NoError(t, p1.Save([]byte("first"), []byte("snap1")))

		p2, err := persister.NewDiskPersister(statePath, snapPath)
		require.NoError(t, err)
		require.NoError(t, p2.Save([]byte("second"), []byte("snap2")))

		p3, err := persister.NewDiskPersister(statePath, snapPath)
		require.NoError(t, err)
		require.Equal(t, []byte("second"), p3.ReadRaftState())
		require.Equal(t, []byte("snap2"), p3.ReadSnapshot())
	})

	t.Run("sizes are correct after reload", func(t *testing.T) {
		dir := t.TempDir()
		state := []byte("some state bytes")
		snap := []byte("snap")

		p1, err := persister.NewDiskPersister(
			filepath.Join(dir, "state"),
			filepath.Join(dir, "snapshot"),
		)
		require.NoError(t, err)
		require.NoError(t, p1.Save(state, snap))

		p2, err := persister.NewDiskPersister(
			filepath.Join(dir, "state"),
			filepath.Join(dir, "snapshot"),
		)
		require.NoError(t, err)
		require.Equal(t, len(state), p2.RaftStateSize())
		require.Equal(t, len(snap), p2.SnapshotSize())
	})
}
