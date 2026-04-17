package server

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"net"
	"sync"

	pb "github.com/adamors/raft/grpc"
	"github.com/adamors/raft/persister"
	"github.com/adamors/raft/raft"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	pb.UnimplementedRaftServer

	me          string
	applyErr    string // from apply channel readers
	lastApplied int
	persister   persister.Persister

	mu   sync.Mutex
	raft *raft.Raft
	logs map[int]any // copy of each server's committed entries
	gsrv *grpc.Server
	addr string
}

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(writer io.Writer) error {
	if _, err := io.Copy(writer, s.reader); err != nil {
		return err
	}
	return nil
}

func (s *snapshot) Release() {}

func NewGRPCServer(me string, transport raft.Transport, persister persister.Persister, config *raft.Config) *GrpcServer {
	s := &GrpcServer{
		me:        me,
		logs:      map[int]any{},
		persister: persister,
		gsrv:      grpc.NewServer(),
	}

	s.raft = raft.NewRaft(transport, me, persister, s, config)

	pb.RegisterRaftServer(s.gsrv, s)

	return s
}

func (s *GrpcServer) Apply(l *raft.Log) any {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs[l.Index] = l.Data
	s.lastApplied = l.Index

	return nil
}

func (s *GrpcServer) Snapshot() (raft.FSMSnapshot, error) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(s.lastApplied)
	e.Encode(s.logs)
	return &snapshot{reader: w}, nil
}

func (s *GrpcServer) Restore(snapshot io.Reader) error {
	data, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var lastApplied int
	var logs map[int]any

	if d.Decode(&lastApplied) != nil || d.Decode(&logs) != nil {
		log.Fatal("GrpcServer:Restore Error reading from persistence")
	}
	s.lastApplied = lastApplied
	s.logs = logs

	return nil
}

func (s *GrpcServer) Serve(l net.Listener) error {
	s.addr = l.Addr().String()

	return s.gsrv.Serve(l)
}

func (s *GrpcServer) Address() string {
	return s.addr
}

func (s *GrpcServer) Stop() {
	s.gsrv.Stop()
}

func (s *GrpcServer) ApplyErr() string {
	return s.applyErr
}

func (rs *GrpcServer) Kill() {
	rs.raft.Shutdown().Error()

	rs.mu.Lock()
	rs.raft = nil
	rs.mu.Unlock()
}

func (rs *GrpcServer) GetState() (int, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	return rs.raft.GetState()
}

func (rs *GrpcServer) Raft() *raft.Raft {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft
}

func (rs *GrpcServer) Logs(i int) (any, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	v, ok := rs.logs[i]
	return v, ok
}
