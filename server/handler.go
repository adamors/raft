package server

import (
	"context"

	pb "github.com/adamors/raft/grpc"
	"github.com/adamors/raft/raft"
	"google.golang.org/grpc"
)

type RaftServiceHandler struct {
	pb.UnimplementedRaftServer

	raft *raft.Raft
}

func NewRaftServiceHandler(raft *raft.Raft) *RaftServiceHandler {
	return &RaftServiceHandler{raft: raft}
}

func (h *RaftServiceHandler) Register(s *grpc.Server) {
	pb.RegisterRaftServer(s, h)
}

func (h *RaftServiceHandler) RequestVote(_ context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	args := &raft.RequestVoteArgs{
		Term:         int(req.Term),
		CandidateId:  req.CandidateId,
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	h.raft.RequestVote(args, reply)

	return &pb.RequestVoteReply{
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (h *RaftServiceHandler) AppendEntries(_ context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{Term: int(e.Term), Command: e.Command, Type: raft.LogEntryType(e.Type)}
	}

	args := &raft.AppendEntriesArgs{
		Term:         int(req.Term),
		LeaderId:     req.LeaderId,
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
	}

	h.raft.AppendEntries(args, reply)

	return &pb.AppendEntriesReply{
		Term:    int32(reply.Term),
		Success: reply.Success,
		XTerm:   int32(reply.XTerm),
		XIndex:  int32(reply.XIndex),
		XLen:    int32(reply.XLen),
	}, nil
}

func (h *RaftServiceHandler) InstallSnapshot(_ context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	reply := &raft.InstallSnapshotReply{}
	args := &raft.InstallSnapshotArgs{
		Term:              int(req.Term),
		LeaderId:          req.LeaderId,
		LastIncludedIndex: int(req.LastIncludedIndex),
		LastIncludedTerm:  int(req.LastIncludedTerm),
		Data:              req.Data,
		Done:              req.Done,
	}
	h.raft.InstallSnapshot(args, reply)
	return &pb.InstallSnapshotReply{Term: int32(reply.Term)}, nil
}
