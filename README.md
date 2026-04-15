### Raft

This is my own implementation of Raft. It has two main sources of implementation:

- The MIT 6.5840 course
- Hashicorp's Raft implementation


I've done the MIT 6.5840 course on my own and then wrote this implementation from the ground up.
Design decisions are inspired by the labs, however both test and application code is entirely my own.

What is significantly different from the MIT 6.5840's design:
- I've adopted Hashicorp's FSM API. Channels are not exposed at all, instead the caller implements `Apply`, `Snapshot`, and `Restore` callbacks
- Raft nodes use real gRPC to communicate with each other
- Membership changes are implemented per Ongaro dissertation §4.1 (single-server changes)
- The test harness is written around [Gosim](https://github.com/jellevandenhooff/gosim), this allows for crashing/restarting/isolating of nodes


#### Usage

TBD


#### Local setup

TBD
