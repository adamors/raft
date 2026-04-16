package raft

import "time"

type Config struct {
	// ElectionTimeout is the base timeout before a follower starts an election.
	// A randomized jitter is added on top of this value.
	ElectionTimeout time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats to followers.
	// Should be well below ElectionTimeout.
	HeartbeatInterval time.Duration

	// MaxRaftState is the maximum persisted raft state size in bytes before a
	// snapshot is triggered. Set to -1 to disable snapshotting.
	MaxRaftState int
}

func DefaultConfig() *Config {
	return &Config{
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		MaxRaftState:      -1,
	}
}
