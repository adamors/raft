package raft

import "time"

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(peer string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.transport.Call(peer, "Raft.RequestVote", args, reply)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.stepDownAsLeader(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != "" && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	// election restriction 5.4.1
	ownLastLogIndex := rf.lastLogIndex()
	ownLastTerm := rf.log[len(rf.log)-1].Term

	if ownLastTerm > args.LastLogTerm {
		reply.VoteGranted = false
		return
	}

	if ownLastTerm == args.LastLogTerm && ownLastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.lastHeartbeat = time.Now()
	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	votes := 1
	for _, peer := range rf.transport.Peers() {
		if peer == rf.me {
			continue
		}

		// concurrently send out votes for every peer
		go func(peer string) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(peer, args, reply); !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.stepDownAsLeader(reply.Term)
			}

			if reply.VoteGranted && rf.currentTerm == term {
				votes++
				if votes > rf.transport.NumPeers()/2 && !rf.isLeader {
					rf.isLeader = true
					now := time.Now()
					for _, p := range rf.transport.Peers() {
						rf.lastAckTime[p] = now
						rf.nextIndex[p] = rf.lastLogIndex() + 1
						rf.matchIndex[p] = 0
					}
					rf.heartbeat()
				}
			}
		}(peer)
	}
}

func (rf *Raft) electionTicker() {
	defer rf.wg.Done()

	for rf.killed() == false {
		rf.mu.Lock()
		if time.Since(rf.lastHeartbeat) > rf.electionTimeout && !rf.isLeader {
			ms := 300 + rf.rng.Int63()%300
			electionTimeout := time.Duration(ms) * time.Millisecond
			rf.electionTimeout = electionTimeout
			rf.mu.Unlock()

			rf.startElection()
			// Sleep for the full election timeout so we don't immediately
			// re-trigger another election if lastHeartbeat is stale (e.g.
			// after a long partition).
			time.Sleep(electionTimeout)
			continue
		}
		rf.mu.Unlock()

		ms := 50 + (rf.rng.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
