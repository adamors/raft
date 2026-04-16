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

TBD


#### Local setup

TBD
