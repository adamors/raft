package raft

import (
	"context"
	"time"

	pb "github.com/adamors/raft/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport interface {
	Call(id string, method string, args, reply any) bool
	Peers() []string
	NumPeers() int
	ReplacePeers(addrs []string)
	Dial(addr string) bool
}

// GrpcTransport dials a fresh connection per RPC call so there is no
// per-connection backoff state that could block reconnection after a
// network partition is lifted.
type GrpcTransport struct {
	addrs    []string
	dialOpts []grpc.DialOption
}

// NewGrpcTransport creates a new gRPC transport, grpc.DialOption(s) can be used
// to configure TLS credentials. Insecure credentials will be used as a fallback.
func NewGrpcTransport(addrs []string, opts ...grpc.DialOption) (*GrpcTransport, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &GrpcTransport{addrs: addrs, dialOpts: opts}, nil
}

func (t *GrpcTransport) NumPeers() int {
	return len(t.addrs)
}

func (t *GrpcTransport) Peers() []string {
	return t.addrs
}

func (t *GrpcTransport) ReplacePeers(addrs []string) {
	t.addrs = addrs
}

func (t *GrpcTransport) Dial(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	opts := append(t.dialOpts, grpc.WithBlock())
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return false
	}
	defer conn.Close()

	return true
}

// Call dials id directly as the address since ID == address in this implementation.
func (t *GrpcTransport) Call(id string, method string, args, reply any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	opts := append(t.dialOpts, grpc.WithBlock())
	conn, err := grpc.DialContext(ctx, id, opts...) //nolint:staticcheck
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
			CandidateId:  a.CandidateId,
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
			pbEntries[i] = &pb.LogEntry{Term: int32(e.Term), Command: e.Command, Type: pb.LogEntryType(e.Type)}
		}
		pbArgs := &pb.AppendEntriesArgs{
			Term:         int32(a.Term),
			LeaderId:     a.LeaderId,
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
			LeaderId:          a.LeaderId,
			LastIncludedIndex: int32(a.LastIncludedIndex),
			LastIncludedTerm:  int32(a.LastIncludedTerm),
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
