# aioraft-ng
![ci](https://github.com/lablup/raft-py/workflows/default/badge.svg)
* Unofficial implementation of `Raft` consensus algorithm written in asyncio-based Python 3.

## Implementation Stages
- [x] Leader Election
- [ ] Log Replication
- [ ] Membership Change

## Rules for Leaders
- Upon becoming leader, append `no-op` entry to log.
- If election timeout elapses without successful round of heartbeats to majority of servers, convert to follower.

## Implementation: Threaded Architecture
* ConsensusState: Holds a `Lock`, `Configuration` and `Log`
* ServiceThreads(InboundRPCs): Several threads handle incoming requests from clients and other servers. These threads wait outside the consensus state monitor for incoming requests, then enter the monitor to carry out each request.
* PeerThreads(OutboundRPCs): There are as many peer threads as there are other servers in the cluster; each peer thread managers the RPCs to one of the other servers. Each thread enters the consensus state monitor, using a condition variable to wait for events that require communication with the given server. Then it leaves the monitor (releasing the lock) and issues an RPC. Once the RPC completes (or fails), the peer thread reenters the consensus state monitor, updates state variables based on the RPC, and waits for the next event that requires communication.
* StateMachineThread: One thread executes the state machine. It enters the consensus state monitor to wait for the next committed log entry; when an entry is available, it leaves the monitor, executes the command, and returns to the monitor to wait for the next command.
* TimerThreads: One thread manages the election timer for both followers and candidates; it starts a new election once a randomized election timeout has elapsed. A second thread causes the server to return to the follower state if, as leader, it is unable to communicate with a majority of the cluster; clients are then able to retry their requests with another server.
* LogSyncThread: When the server is leader, one thread writes log entries durably to disk. This is done without holding the lock on the consensus state, so replication to followers can proceed in parallel. For simplicity, followers and candidate write directly to disk from their service threads while holding the consensus lock; they do not use the log sync thread.
![Threaded architecture](https://user-images.githubusercontent.com/14137676/185047482-f51d1258-771d-44c5-8313-63785de68872.png)

## References
- https://raft.github.io/raft.pdf
- https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
- https://github.com/hashicorp/raft/blob/main/membership.md
- https://tech.kakao.com/2021/12/20/kubernetes-etcd/
