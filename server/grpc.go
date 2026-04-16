package server

import (
	"context"

	pb "github.com/adamors/raft/grpc"
	"github.com/adamors/raft/raft"
)

func (s *GrpcServer) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	args := &raft.RequestVoteArgs{
		Term:         int(req.Term),
		CandidateId:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	s.raft.RequestVote(args, reply)

	return &pb.RequestVoteReply{
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *GrpcServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{Term: int(e.Term), Command: e.Command}
	}

	args := &raft.AppendEntriesArgs{
		Term:         int(req.Term),
		LeaderId:     int(req.LeaderId),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
	}

	s.raft.AppendEntries(args, reply)

	return &pb.AppendEntriesReply{
		Term:    int32(reply.Term),
		Success: reply.Success,
		XTerm:   int32(reply.XTerm),
		XIndex:  int32(reply.XIndex),
		XLen:    int32(reply.XLen),
	}, nil
}

func (s *GrpcServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	reply := &raft.InstallSnapshotReply{}
	args := &raft.InstallSnapshotArgs{
		Term:              int(req.Term),
		LeaderId:          int(req.LeaderId),
		LastIncludedIndex: int(req.LastIncludedIndex),
		LastIncludedTerm:  int(req.LastIncludedTerm),
		Offset:            int(req.Offset),
		Data:              req.Data,
		Done:              req.Done,
	}
	s.raft.InstallSnapshot(args, reply)
	return &pb.InstallSnapshotReply{Term: int32(reply.Term)}, nil
}
