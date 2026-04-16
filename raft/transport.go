package raft

import (
	"context"
	"time"

	pb "github.com/adamors/raft/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport interface {
	Call(server int, method string, args, reply any) bool
	NumPeers() int
	Address(nodeId int) string
}

// GrpcTransport dials a fresh connection per RPC call so there is no
// per-connection backoff state that could block reconnection after a
// network partition is lifted.
type GrpcTransport struct {
	addrs []string
}

func NewGrpcTransport(addrs []string) (*GrpcTransport, error) {
	return &GrpcTransport{addrs: addrs}, nil
}

func (t *GrpcTransport) NumPeers() int {
	return len(t.addrs)
}

func (t *GrpcTransport) Call(server int, method string, args, reply any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(ctx, t.addrs[server], //nolint:staticcheck
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewRaftClient(conn)

	switch method {
	case "Raft.RequestVote":
		a := args.(*RequestVoteArgs)
		pbArgs := &pb.RequestVoteArgs{
			Term:         int32(a.Term),
			CandidateId:  int32(a.CandidateId),
			LastLogIndex: int32(a.LastLogIndex),
			LastLogTerm:  int32(a.LastLogTerm),
		}
		pbReply, err := client.RequestVote(ctx, pbArgs)
		if err != nil {
			return false
		}
		r := reply.(*RequestVoteReply)
		r.Term = int(pbReply.Term)
		r.VoteGranted = pbReply.VoteGranted

	case "Raft.AppendEntries":
		a := args.(*AppendEntriesArgs)
		pbEntries := make([]*pb.LogEntry, len(a.Entries))
		for i, e := range a.Entries {
			pbEntries[i] = &pb.LogEntry{Term: int32(e.Term), Command: e.Command}
		}
		pbArgs := &pb.AppendEntriesArgs{
			Term:         int32(a.Term),
			LeaderId:     int32(a.LeaderId),
			PrevLogIndex: int32(a.PrevLogIndex),
			PrevLogTerm:  int32(a.PrevLogTerm),
			Entries:      pbEntries,
			LeaderCommit: int32(a.LeaderCommit),
		}
		pbReply, err := client.AppendEntries(ctx, pbArgs)
		if err != nil {
			return false
		}
		r := reply.(*AppendEntriesReply)
		r.Term = int(pbReply.Term)
		r.Success = pbReply.Success
		r.XTerm = int(pbReply.XTerm)
		r.XIndex = int(pbReply.XIndex)
		r.XLen = int(pbReply.XLen)

	case "Raft.InstallSnapshot":
		a := args.(*InstallSnapshotArgs)
		pbArgs := &pb.InstallSnapshotArgs{
			Term:              int32(a.Term),
			LeaderId:          int32(a.LeaderId),
			LastIncludedIndex: int32(a.LastIncludedIndex),
			LastIncludedTerm:  int32(a.LastIncludedTerm),
			Offset:            int32(a.Offset),
			Data:              a.Data,
			Done:              a.Done,
		}
		pbReply, err := client.InstallSnapshot(ctx, pbArgs)
		if err != nil {
			return false
		}
		r := reply.(*InstallSnapshotReply)
		r.Term = int(pbReply.Term)

	default:
		return false
	}

	return true
}

func (t *GrpcTransport) Address(nodeId int) string {
	if nodeId < 0 || nodeId >= len(t.addrs) {
		return ""
	}
	return t.addrs[nodeId]
}
