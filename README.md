# Raft
![ci-lint](https://github.com/lablup/raft-py/workflows/lint/badge.svg)
![ci-typecheck](https://github.com/lablup/raft-py/workflows/typecheck/badge.svg)
![ci-unittest](https://github.com/lablup/raft-py/workflows/unittest/badge.svg)
* Unofficial implementation of `Raft` consensus algorithm written in asyncio-based Python.

## About
* Raft tolerates up to $$ { (N-1) \over 2 } $$ failures.

## Rules for Leaders
- Upon becoming leader, append `no-op` entry to log.
- If election timeout elapses without successful round of heartbeats to majority of servers, convert to follower.

## Demo Videos
* Leader Election
![Raft-Leader-Election-01](https://user-images.githubusercontent.com/14137676/175849270-e4a56533-5add-4dde-ad4a-3d935e42ae49.mp4)
* Leader Election with 7 Peers (Tolerate up to 3 failures)
![Raft-Leader-Election-02](https://user-images.githubusercontent.com/14137676/176112247-ecbe3c17-d126-447b-8128-025fa5eab76a.mp4)

## Implementation: Threaded Architecture
* ConsensusState: Holds a `Lock`, `Configuration` and `Log`
* ServiceThreads(InboundRPCs): Several threads handle incoming requests from clients and other servers. These threads wait outside the consensus state monitor for incoming requests, then enter the monitor to carry out each request.
* PeerThreads(OutboundRPCs): There are as many peer threads as there are other servers in the cluster; each peer thread managers the RPCs to one of the other servers. Each thread enters the consensus state monitor, using a condition variable to wait for events that require communication with the given server. Then it leaves the monitor (releasing the lock) and issues an RPC. Once the RPC completes (or fails), the peer thread reenters the consensus state monitor, updates state variables based on the RPC, and waits for the next event that requires communication.
* StateMachineThread: One thread executes the state machine. It enters the consensus state monitor to wait for the next committed log entry; when an entry is available, it leaves the monitor, executes the command, and returns to the monitor to wait for the next command.
* TimerThreads: One thread manages the election timer for both followers and candidates; it starts a new election once a randomized election timeout has elapsed. A second thread causes the server to return to the follower state if, as leader, it is unable to communicate with a majority of the cluster; clients are then able to retry their requests with another server.
* LogSyncThread: When the server is leader, one thread writes log entries durably to disk. This is done without holding the lock on the consensus state, so replication to followers can proceed in parallel. For simplicity, followers and candidate write directly to disk from their service threads while holding the consensus lock; they do not use the log sync thread.
![Threaded architecture](https://user-images.githubusercontent.com/14137676/185047482-f51d1258-771d-44c5-8313-63785de68872.png)
