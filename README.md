### Raft

This is an original implementation of Raft. It has two main sources of inspiration:

- The MIT 6.5840 course
- Hashicorp's Raft implementation

I worked through MIT 6.5840 on my own and then wrote this implementation from the ground up.
The project takes inspiration from the labs and from Hashicorp's API design, but the implementation, tests, and application code in this repository are my own.

What is significantly different from the MIT 6.5840 lab design:
- I've adopted Hashicorp's FSM API. Channels are not exposed at all, instead the caller implements `Apply`, `Snapshot`, and `Restore` callbacks
- Raft nodes use real gRPC to communicate with each other
- Membership changes are implemented per Ongaro dissertation §4.1 (single-server changes)
- The test harness is written around [Gosim](https://github.com/jellevandenhooff/gosim), this allows for crashing/restarting/isolating of nodes


#### Usage

```go
import (
    "github.com/adamors/raft/persister"
    "github.com/adamors/raft/raft"
    "github.com/adamors/raft/server"
)
```

**1. Implement the FSM interface**

```go
type FSM interface {
    Apply(*Log) any
    Snapshot() (FSMSnapshot, error)
    Restore(io.Reader) error
}

type FSMSnapshot interface {
    Persist(io.Writer) error
    Release()
}
```

**2. Set up a node**

```go
// Create a disk persister for state and snapshots
p, err := persister.NewDiskPersister("/data/raft/state", "/data/raft/snapshot")

// Create a gRPC transport — pass the node's own address
transport, err := raft.NewGrpcTransport([]string{"127.0.0.1:8400"})

cfg := raft.DefaultConfig()
r := raft.NewRaft(transport, "127.0.0.1:8400", p, fsm, cfg)
```

**3. Register the Raft gRPC service on your server**

```go
grpcSrv := grpc.NewServer()
server.NewRaftServiceHandler(r).Register(grpcSrv)
grpcSrv.Serve(ln)
```

**4. Cluster membership**

Nodes start as a single-node cluster. Add peers dynamically as they are discovered:

```go
// Add a peer (e.g. on Serf join event)
r.AddServer(addr, 10*time.Second).Error()

// Remove a peer (e.g. on Serf leave event)
r.RemoveServer(addr, 10*time.Second).Error()
```

**5. Apply commands**

Only the leader can apply commands. `Apply` returns a future — call `Error()` to block until committed:

```go
future := r.Apply(data, 10*time.Second)
if err := future.Error(); err != nil {
    return err
}
result := future.Response()
```


#### Local setup

**Dependencies**

To regenerate the gRPC stubs after modifying `grpc/raft.proto`, install `protoc` and the Go plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
Then run:
```bash
make compile
```

**Tests**

Regular tests (server integration tests, etc.):
```bash
make test
```

Gosim simulation tests (Note: gosim only works on Linux and needs Go <=1.23)

```bash
make test-gosim
```
