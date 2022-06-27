import argparse
import asyncio
import enum
import logging
import uuid
from contextlib import suppress
from datetime import datetime
from typing import Iterable, Optional

import grpc

import raft_pb2
import raft_pb2_grpc

# Coroutines to be invoked when the event loop is shutting down.
_cleanup_coroutines = []


def randrangef(start: float, stop: float) -> float:
    import random
    return random.random() * (stop - start) + start


class RaftState(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class AsyncGrpcRaftClient:
    async def request_append_entries(self, address: str, term: int, leader_id: str, entries: Iterable[str]):
        done, pending = await asyncio.wait({
            asyncio.create_task(self._request_append_entries(address, term, leader_id, entries)),
            asyncio.create_task(asyncio.sleep(5.0)),
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            with suppress(asyncio.CancelledError):
                task.cancel()

    async def _request_append_entries(self, address: str, term: int, leader_id: str, entries: Iterable[str]):
        async with grpc.aio.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            request = raft_pb2.AppendEntriesRequest(term=term, leader_id=leader_id, entries=entries)
            try:
                response = await stub.AppendEntries(request)
            except grpc.aio.AioRpcError:
                pass

    async def request_vote(self, address: str, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> bool:
        self._vote_granted = None
        timeout_task = asyncio.create_task(asyncio.sleep(5.0))
        done, pending = await asyncio.wait({
            asyncio.create_task(self._request_vote(address, term, candidate_id, last_log_index, last_log_term)),
            timeout_task,
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            with suppress(asyncio.CancelledError):
                task.cancel()
        if timeout_task in pending:
            return self._vote_granted or False

    async def _request_vote(self, address: str, term: int, candidate_id: str, last_log_index: int, last_log_term: int):
        async with grpc.aio.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            request = raft_pb2.RequestVoteRequest(term=term, candidate_id=candidate_id, last_log_index=last_log_index, last_log_term=last_log_term)
            try:
                response = await stub.RequestVote(request)
                self._vote_granted = response.vote_granted
            except grpc.aio.AioRpcError:
                pass


class AsyncGrpcRaftPeer(raft_pb2_grpc.RaftServiceServicer):
    """
    All Servers:
    - If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
    - If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower.

    Followers:
    - Respond to RPCs from candidates and leaders
    - If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.

    Candidates:
    - On conversion to candidate, start election:
        - Increment currentTerm
        - Vote for self
        - Reset election timer
        - Send RequestVote RPCs to all other servers
    - If votes received from majority of servers: become leader
    - If AppendEntries RPC received from new leader: convert to follower
    - If election timeout elapses: start new election

    Leaders:
    - Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server; repeat during idle periods to prevent election timeouts.
    - If command received from client: append entry to local log, respond after entry applied to state machine.
    - If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex.
        - If successful: update `nextIndex` and `matchIndex` for follower.
        - If `AppendEntries` failes because of log inconsistency: decrement `nextIndex` and retry.
    - If there exists an N such that N > `commitIndex`, a majority of `matchIndex[i]` >= N, and log[N].term == `currentTerm`: set `commitIndex` = N
    """
    def __init__(
        self,
        *,
        peers: Iterable[str] = None,
        heartbeat_interval: float = 0.1,
    ):
        self._id = str(uuid.uuid4())
        self._current_term: int = 0
        self._state = RaftState.FOLLOWER
        self._peers = peers or ()
        self._client = AsyncGrpcRaftClient()

        self._heartbeat_interval: float = heartbeat_interval
        self._last_vote_term: int = 0
        self._leader_id: Optional[str] = None

    async def run(self):
        logging.info(f'[{datetime.now().isoformat()}] [{self.__class__.__name__}] ({self.id}) run()')
        while True:
            self.reset_timeout()
            logging.info(f'[{datetime.now().isoformat()}] [{self.__class__.__name__}] ({self.id}) reset_timeout() :: {self.state}')
            match self.state:
                case RaftState.FOLLOWER.name:
                    await self._wait_for_election_timeout()
                    await self._request_vote()
                case RaftState.LEADER.name:
                    while self.state == RaftState.LEADER.name:
                        await self._publish_heartbeat_message()
                        await asyncio.sleep(self._heartbeat_interval)
            await asyncio.sleep(0)

    def reset_timeout(self):
        self._election_timeout: float = randrangef(0.15, 0.3)
        self._elapsed_time: float = 0.0

    async def _wait_for_election_timeout(self, interval: float = 1.0 / 30):
        while self._elapsed_time < self.election_timeout:
            await asyncio.sleep(interval)
            self._elapsed_time += interval

    async def _request_vote(self):
        self._current_term += 1
        self._state = RaftState.CANDIDATE
        self._last_vote_term = self.current_term
        vote_count = 1  # votes for itself
        logging.info(f'[{datetime.now().isoformat()}] {self.id} Request Vote!')
        results = await asyncio.gather(*[
            asyncio.create_task(self._client.request_vote(peer, term=self.current_term, candidate_id=self.id, last_log_index=0, last_log_term=0))
            for peer in self._peers
        ])
        vote_count += sum(results)
        self._state = RaftState.LEADER if vote_count > (self.number_of_peers / 2) \
                      else RaftState.FOLLOWER
        logging.info(f'[{datetime.now().isoformat()}] {self.id} Vote Result: {self.state}')
        if self.state == RaftState.LEADER.name:
            await self._publish_heartbeat_message()

    async def _publish_heartbeat_message(self):
        await asyncio.wait({
            asyncio.create_task(self._client.request_append_entries(address=address, term=self.current_term, leader_id=self.id, entries=()))
            for address in self._peers
        }, return_when=asyncio.ALL_COMPLETED)

    """
    def check_term_synchronized(self, term: int) -> bool:
        if term > self.current_term:
            self._current_term = term
            self._state = RaftState.FOLLOWER
            return False
        return True
    """

    def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context: grpc.aio.ServicerContext) -> raft_pb2.AppendEntriesResponse:
        logging.info(f'[{datetime.now().isoformat()}] [AppendEntries] (term={request.term} leader={request.leader_id[:4]})')
        self.reset_timeout()
        if request.term >= self.current_term:
            self._current_term = request.term
            self._state = RaftState.FOLLOWER
        # 1. Reply false if term < currentTerm.
        if request.term < self.current_term:
            return raft_pb2.AppendEntriesResponse(term=request.term, success=False)
        # 2. Reply false if log doesn't contain any entry at prevLogIndex whose term matches prevLogTerm.
        # 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it.
        # 4. Append any new entries not already in the log.
        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        return raft_pb2.AppendEntriesResponse(term=request.term, success=True)

    def RequestVote(self, request: raft_pb2.RequestVoteRequest, context: grpc.aio.ServicerContext) -> raft_pb2.RequestVoteResponse:
        logging.info(f'[RequestVote] request={request}')
        """
        self._vote = True
        vote_granted, self._vote = self._vote, False
        """
        # 1. Reply false if term < currentTerm.
        if request.term < self.current_term:
            return raft_pb2.RequestVoteResponse(term=request.term, vote_granted=False)
        self._current_term = request.term
        # 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
        vote_granted = (self._last_vote_term < request.term)
        return raft_pb2.RequestVoteResponse(term=request.term, vote_granted=vote_granted)

    @property
    def id(self) -> str:
        return self._id

    @property
    def current_term(self) -> int:
        return self._current_term

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def number_of_peers(self) -> int:
        return len(self._peers) + 1

    @property
    def election_timeout(self) -> float:
        return self._election_timeout


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=50051)
    parser.add_argument('peers', metavar='peers', type=str, nargs='+',
                        help='"<HOST>:<PORT>" list of peers.')
    return parser.parse_args()


async def _run_raft_server(servicer: raft_pb2_grpc.RaftServiceServicer, port: int = 50051):
    server = grpc.aio.server()
    raft_pb2_grpc.add_RaftServiceServicer_to_server(servicer, server)
    address = f'[::]:{port}'
    server.add_insecure_port(address)
    logging.info(f'[{datetime.now().isoformat()}] Starting server on {address}..')
    await server.start()

    async def server_graceful_shutdown():
        logging.info(f'[{datetime.now().isoformat()}] Starting graceful shutdown...')
        # Shuts down the server with 5 seconds of grace period. During the
        # grate period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await server.stop(5)

    _cleanup_coroutines.append(server_graceful_shutdown())
    logging.info(f'[{datetime.now().isoformat()}] Waiting for termination..')
    await server.wait_for_termination()
    logging.info(f'[{datetime.now().isoformat()}] Terminatd.')


async def _main():
    args = parse_args()
    peer = AsyncGrpcRaftPeer(peers=args.peers)

    done, pending = await asyncio.wait({
        asyncio.create_task(_run_raft_server(servicer=peer, port=args.port)),
        asyncio.create_task(peer.run()),
    }, return_when=asyncio.ALL_COMPLETED)
    for task in pending:
        with suppress(asyncio.CancelledError):
            task.cancel()


async def main():
    try:
        await _main()
    finally:
        await asyncio.gather(*_cleanup_coroutines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
