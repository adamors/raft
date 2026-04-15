### Gosim tests

This directory contains all the tests that use gosim https://github.com/jellevandenhooff/gosim to simulate various
failure scenarios that Raft should be resistent to. The main idea here is that gosim starts each Raft node as an
individual machine and can crash/restart/sever connection to/from other machines.

- Helpers are in `helpers_test.go`
- Then tests are grouped by various aspects: election, replication, persistence and snapshot.

Since low level network manipulation doesn't yet exist in Gosim (like dropping packets etc.) unreliable networking tests
don't exist.


#### Usage

I've been using a patched version of gosim to make it work locally, and `go.mod` is set up to expect gosim in a sibling
directory to this repo. The patched version is https://github.com/adamors/gosim

To run the tests from this directory, run
```bash

⟩ go run github.com/jellevandenhooff/gosim/cmd/gosim test -v
=== RUN   TestBackup (seed 1)
--- PASS: TestBackup (3.61s simulated)
=== RUN   TestBasicAgree (seed 1)
--- PASS: TestBasicAgree (1.32s simulated)
=== RUN   TestFigure8 (seed 1)
--- PASS: TestFigure8 (1060.51s simulated)
=== RUN   TestFollowerFailure (seed 1)
--- PASS: TestFollowerFailure (14.62s simulated)
=== RUN   TestInitialElection (seed 1)
--- PASS: TestInitialElection (1.00s simulated)
=== RUN   TestLeaderFailure (seed 1)
--- PASS: TestLeaderFailure (10.14s simulated)
=== RUN   TestManyElections (seed 1)
--- PASS: TestManyElections (1.00s simulated)
=== RUN   TestPersist1 (seed 1)
--- PASS: TestPersist1 (1.93s simulated)
=== RUN   TestPersist2 (seed 1)
--- PASS: TestPersist2 (11.62s simulated)
=== RUN   TestPersist3 (seed 1)
--- PASS: TestPersist3 (142.62s simulated)
=== RUN   TestReElection (seed 1)
--- PASS: TestReElection (6.15s simulated)
=== RUN   TestRejoin (seed 1)
--- PASS: TestRejoin (8.12s simulated)
=== RUN   TestSnapshotAllCrash (seed 1)
--- PASS: TestSnapshotAllCrash (52.83s simulated)
=== RUN   TestSnapshotInstall (seed 1)
--- PASS: TestSnapshotInstall (1012.12s simulated)
ok      translated/github.com/adamors/raft/gosim        16.507s
```
