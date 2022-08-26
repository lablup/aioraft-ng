import asyncio
import logging
import math
from datetime import datetime
from typing import Dict, Final, Iterable, Optional, Set, Tuple

from raft.aio.fsm import RespFSM
from raft.aio.logs import AbstractReplicatedLog, MemoryReplicatedLog
from raft.aio.peers import AbstractRaftPeer
from raft.aio.protocols import AbstractRaftClusterProtocol, AbstractRaftProtocol
from raft.aio.server import AbstractRaftServer
from raft.protos import raft_pb2
from raft.types import (
    AppendEntriesResponse,
    ClientQueryResponse,
    ClientRequestResponse,
    RaftClusterStatus,
    RaftId,
    RaftState,
    RegisterClientResponse,
    RequestVoteResponse,
    aobject,
)
from raft.utils import AtomicInteger, randrangef

logging.basicConfig(level=logging.INFO)

__all__ = ("Raft",)


class Raft(aobject, AbstractRaftProtocol, AbstractRaftClusterProtocol):
    """Rules for Servers
    All Servers
    - If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
    - If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower

    Followers
    - Respond to RPCs from candidates and leaders
    - If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate:
      convert to candidate

    Candidates
    - On conversion to candidate, start election:
        - Increment currentTerm
        - Vote for self
        - Reset election timer
        - Send RequestVote RPCs to all other servers
    - If votes received from majority of servers: become leader
    - If AppendEntries RPC received from new leader: convert to follower
    - If election timeout elapses: start new election

    Leaders
    - Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
      repeat during idle periods to prevent election timeouts
    - If command received from client: append entry to local log, respond after entry applied to state machine
    - If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
        - If successful: update nextIndex and matchIndex for follower
        - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
    - If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm:
      set commitIndex = N
    """

    def __init__(
        self,
        id_: RaftId,
        server: AbstractRaftServer,
        peer: AbstractRaftPeer,
        configuration: Iterable[RaftId],
        **kwargs,
    ):
        self.__id: Final[RaftId] = id_
        self.__server: Final[AbstractRaftServer] = server
        self.__peer: Final[AbstractRaftPeer] = peer
        self.__configuration: Set[RaftId] = set(configuration)

        self.__leader_id: Optional[RaftId] = None

        self.__election_timeout: Final[float] = randrangef(0.15, 0.3)
        self.__heartbeat_timeout: Final[float] = 0.1

        self.__fsm = RespFSM()
        self._response_cache: Dict[str, Tuple[int, Optional[str]]] = {}

        server.bind(raft_protocol=self, raft_cluster_protocol=self)

    async def __ainit__(self, *args, **kwargs):
        await self._initialize_persistent_state()
        await self._initialize_volatile_state()

        await self.__change_state(RaftState.FOLLOWER)
        await self._restart_timeout()

    async def _initialize_persistent_state(self) -> None:
        """Persistent state on all servers
        (Updated on stable storage before responding to RPCs)

        currentTerm (int): latest term server has seen
                           (initialized to 0 on first boot, increases monotonically)
        votedFor (raft.types.RaftId):
            candidateId that received vote in current term (or null if none)
        log (Iterable[raft.protos.raft_pb2.Log]): log entries;
            each entry contains command for state machine, and term when entry was received by leader
            (first index is 1)
        """
        self.__current_term: AtomicInteger = AtomicInteger(0)
        self.__voted_for: Optional[RaftId] = None
        self.raft_log: AbstractReplicatedLog = MemoryReplicatedLog()

    async def _initialize_volatile_state(self) -> None:
        """Volatile state on all servers
        (Reinitialized after election)

        commitIndex (int): index of highest log entry known to be committed
                           (initialized to 0, increases monotonically)
        lastApplied (int): index of highest log entry applied to state machine
                           (initialized to 0, increases monotonically)
        """
        self.__commit_index: int = 0
        self.__last_applied: int = 0

    async def _initialize_leader_volatile_state(self) -> None:
        """Volatile state on leaders
        (Reinitialized after election)

        nextIndex (Dict[raft.types.RaftId, int]):
            for each server, index of the next log entry to send to that server
            (initialized to leader last log index + 1)
        matchIndex (Dict[raft.types.RaftId, int]):
            for each server, index of highest log entry known to be replicated on server
            (initialized to 0, increases monotonically)
        """
        self.__next_index: Dict[RaftId, int] = {}
        self.__match_index: Dict[RaftId, int] = {}

    async def main(self) -> None:
        while True:
            match self.__state:
                case RaftState.FOLLOWER:
                    await FollowerTask.run(self)
                case RaftState.CANDIDATE:
                    while self.__state is RaftState.CANDIDATE:
                        await CandidateTask.run(self)
                        await asyncio.sleep(self.__election_timeout)
                case RaftState.LEADER:
                    # Upon becoming leader, append `no-op` entry to log.
                    await self.proclaim()
                    """
                    # FIXME: Apply and commit previous logs.
                    count = await self.raft_log.count()
                    for i in range(1, count + 1):
                        if entry := await self.raft_log.get(i):
                            await self.__fsm.apply(command=entry.command)
                            await self.commit_log(i)
                            self.__last_applied = i
                    """
                    await LeaderTask.run(self)

    async def _restart_timeout(self) -> None:
        self.__elapsed_time: float = 0.0

    async def _wait_for_election_timeout(self, interval: float = 1.0 / 30) -> None:
        while self.__elapsed_time < self.__election_timeout:
            await asyncio.sleep(interval)
            self.__elapsed_time += interval
        await self.__change_state(RaftState.CANDIDATE)

    async def __synchronize_term(self, term: int) -> None:
        if term > self.current_term:
            self.__current_term.set(term)
            await self.__change_state(RaftState.FOLLOWER)
            self.__voted_for = None

    async def __change_state(self, next_state: RaftState) -> None:
        self.__state: RaftState = next_state

    async def _start_election(self) -> bool:
        self.__current_term.increase()
        self.__voted_for = self.id
        await self._restart_timeout()

        current_term = self.current_term
        logging.info(f"[{datetime.now()}] id={self.id} Campaign(term={current_term})")

        responses = await asyncio.gather(
            *[
                asyncio.create_task(
                    self.__peer.request_vote(
                        to=server,
                        term=current_term,
                        candidate_id=self.id,
                        last_log_index=0,
                        last_log_term=0,
                    ),
                )
                for server in self.__configuration
            ]
        )

        for response in responses:
            if response.term > current_term:
                await self.__synchronize_term(response.term)
                await self.__change_state(RaftState.FOLLOWER)
                break
        else:
            if sum([r.vote_granted for r in responses]) + 1 >= self.quorum:
                await self.__change_state(RaftState.LEADER)
                return True

        return False

    async def send_heartbeat(self) -> bool:
        # TODO: entries..
        return await self.replicate(entries=())

    async def replicate(
        self, entries: Iterable[raft_pb2.Log], strict: bool = False
    ) -> bool:
        if not self.has_leadership():
            return False

        prev_log_index = 0
        prev_log_term = 0

        if entries := tuple(entries):
            if prev_log := await self.raft_log.precede(entries[0].index):
                prev_log_index = prev_log.index
                prev_log_term = prev_log.term

        responses = await asyncio.gather(
            *[
                asyncio.create_task(
                    self.__peer.append_entries(
                        to=server,
                        term=self.current_term,
                        leader_id=self.id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=entries,
                        leader_commit=self.__commit_index,
                    ),
                )
                for server in self.__configuration
            ]
        )
        if strict:
            return sum([r.success for r in responses]) + 1 == self.membership
        return sum([r.success for r in responses]) + 1 >= self.quorum

    """
    async def append_entries(self, to: str) -> bool:
        pass

    async def retry_append_entries(self, to: str):
        while not await self.append_entries(to=to):
            await asyncio.sleep(0.1)
    """

    async def commit_log(self, index: int) -> bool:
        await self.raft_log.commit(index=index)
        self.__commit_index = index
        return True

    def has_leadership(self) -> bool:
        return self.__state is RaftState.LEADER

    async def proclaim(self) -> None:
        logging.info('Upon becoming leader, append "no-op".')
        count = await self.raft_log.count()
        no_op_entry = raft_pb2.Log(
            index=count + 1, term=self.current_term, command=None
        )
        await self.raft_log.append(entries=(no_op_entry,))
        await self.replicate(entries=(no_op_entry,))

    """
    AbstractRaftProtocol
    """

    async def on_append_entries(
        self,
        *,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
    ) -> AppendEntriesResponse:
        await self._restart_timeout()
        # 1. Reply false if term < currentTerm
        if term < (current_term := self.current_term):
            logging.info(
                f"[on_append_entries] success=False :: term={term} current_term={self.current_term}"
            )
            return AppendEntriesResponse(term=current_term, success=False)
        await self.__synchronize_term(term)
        # X. Return early on heartbeat
        self.__leader_id = leader_id
        if not entries:
            return AppendEntriesResponse(term=self.current_term, success=True)
        # 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if prev_log_index > 0:
            prev_log = await self.raft_log.get(index=prev_log_index)
            if prev_log is None or prev_log.term != prev_log_term:
                logging.info(
                    f"[on_append_entries] success=False :: prev_log_index={prev_log_index} prev_log={prev_log}"
                )
                return AppendEntriesResponse(term=self.current_term, success=False)
        # 3. If an existing entry conflicts with a new one (same index but different terms),
        #    delete the existing entry and all that follow it
        for entry in entries:
            log = await self.raft_log.get(index=entry.index)
            if log is not None and log.term != entry.term:
                await self.raft_log.splice(entry.index)
                break
        # 4. Append any new entries not already in the log
        await self.raft_log.append(entries)
        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if leader_commit > self.__commit_index:
            *_, last_new_entry = entries
            # FIXME: Apply?
            self.__commit_index = min(leader_commit, last_new_entry.index)
        logging.info("[on_append_entries] success=True")
        return AppendEntriesResponse(term=self.current_term, success=True)

    async def on_request_vote(
        self,
        *,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> RequestVoteResponse:
        await self._restart_timeout()
        # 1. Reply false if term < currentTerm
        if term < (current_term := self.current_term):
            return RequestVoteResponse(term=current_term, vote_granted=False)

        await self.__synchronize_term(term)

        # 2. If votedFor is null or candidateId, and candidate's log is
        #    at least as up-to-date as receiver's log, grant vote
        if self.voted_for not in [None, candidate_id]:
            logging.info(
                f"\n[on_request_vote] term={term} id={candidate_id[-4:]} vote=False (reason: voted-for={self.voted_for}"
            )
            return RequestVoteResponse(term=self.current_term, vote_granted=False)

        if last_log := await self.raft_log.last():
            if last_log_term < last_log.term or last_log_index < last_log.index:
                logging.info(
                    f"\n[on_request_vote] term={term} id={candidate_id[-4:]} vote=False (reason: term({last_log_term}, {last_log.term}) index({last_log_index}, {last_log.index})"
                )
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

        self.__voted_for = candidate_id

        logging.info(
            f"\n[on_request_vote] term={term} id={candidate_id[-4:]} vote=True"
        )
        return RequestVoteResponse(term=self.current_term, vote_granted=True)

    """
    AbstractRaftClusterProtocol
    """

    async def on_client_request(
        self, *, client_id: str, sequence_num: int, command: str
    ) -> ClientRequestResponse:
        logging.info(
            f'[on_client_request] cid={client_id[:8]} seq={sequence_num} command="{command}"'
        )
        # 1. Reply NOT_LEADER if not leader, providing hint when available
        if not self.has_leadership():
            return ClientRequestResponse(
                status=RaftClusterStatus.NOT_LEADER,
                response=None,
                leader_hint=self.__leader_id,
            )
        # 2. Append command to log, replicate and commit it
        index = await self.raft_log.count() + 1
        entries: Iterable[raft_pb2.Log] = [
            raft_pb2.Log(index=index, term=self.current_term, command=command)
        ]
        await self.raft_log.append(entries)
        await self.replicate(entries=entries)
        _ = await self.commit_log(index=index)
        # 3. Reply SESSION_EXPIRED if no record of clientId or if response for client's sequenceNum already discarded
        if client_id is None:
            return ClientRequestResponse(status=RaftClusterStatus.SESSION_EXPIRED)
        # 4. If sequenceNum already processed from client, reply OK with stored response
        if response_cache := self._response_cache.get(client_id):
            if response_cache[0] == sequence_num:
                return ClientRequestResponse(
                    status=RaftClusterStatus.OK, response=response_cache[1]
                )
        # 5. Apply command in log order
        output = await self.__fsm.apply(command=command)
        self.__last_applied = index
        # 6. Save state machine output with sequenceNum for client, discard any prior response for client
        self._response_cache[client_id] = (sequence_num, output)
        # 7. Reply OK with state machine output
        return ClientRequestResponse(
            status=RaftClusterStatus.OK, response=output, leader_hint=self.id
        )

    async def on_register_client(self) -> RegisterClientResponse:
        """TODO
        # 1. Reply NOT_LEADER if not leader, providing hint when available
        if not self.has_leadership():
            return RegisterClientResponse(
                status=RaftClusterStatus.NOT_LEADER,
                client_id=None,
                leader_hint=self.__leader_id,
            )
        # 2. Append register command to log, replicate and commit it
        client_id = str(uuid.uuid4())
        index = await self.raft_log.count() + 1
        entries: Iterable[raft_pb2.Log] = tuple(raft_pb2.Log(index=index, term=self.current_term, command=f"REG CLIENT {client_id}"))
        await self.raft_log.append(entries)
        await self.replicate(entries=entries)
        output = await self._commit_log(index=index)
        # 3. Apply command in log order, allocating session for new client
        # 4. Reply OK with unique client identifier (the log index of this register command can be used)
        return RegisterClientResponse(status=RaftClusterStatus.OK, client_id=client_id, leader_hint=self.__leader_id)
        """
        return RegisterClientResponse(status=RaftClusterStatus.OK)

    async def on_client_query(self, *, query: str) -> ClientQueryResponse:
        # 1. Reply NOT_LEADER if not leader, providing hint when available
        if not self.has_leadership():
            return ClientQueryResponse(
                status=RaftClusterStatus.NOT_LEADER, leader_hint=self.__leader_id
            )
        # 2. Wait until last committed entry is from this leader's term
        while True:
            if last_committed_entry := await self.raft_log.last(committed=True):
                if last_committed_entry.term == self.current_term:
                    break
            await asyncio.sleep(1.0)
        # 3. Save commitIndex as local variable readIndex (used below)
        read_index = self.__commit_index
        # 4. Send new round of heartbeats, and wait for reply from majority of servers
        while not await self.send_heartbeat():
            await asyncio.sleep(self.__heartbeat_timeout / 2)
        # 5. Wait for state machine to advance at least to the readIndex log entry
        while self.__last_applied < read_index:
            await asyncio.sleep(1.0)
        # 6. Process query
        output = await self.__fsm.query(query)
        # 7. Reply OK with state machine output
        return ClientQueryResponse(status=RaftClusterStatus.OK, response=output)

    @property
    def id(self) -> RaftId:
        return self.__id

    @property
    def current_term(self) -> int:
        return self.__current_term.value

    @property
    def voted_for(self) -> Optional[RaftId]:
        return self.__voted_for

    @property
    def heartbeat_timeout(self) -> float:
        return self.__heartbeat_timeout

    @property
    def membership(self) -> int:
        return len(self.__configuration) + 1

    @property
    def quorum(self) -> int:
        return math.floor(self.membership / 2) + 1


class RaftTask:
    @staticmethod
    async def run(raft: Raft):
        raise NotImplementedError()


class FollowerTask(RaftTask):
    @staticmethod
    async def run(raft: Raft):
        await raft._restart_timeout()
        await raft._wait_for_election_timeout()


class CandidateTask(RaftTask):
    @staticmethod
    async def run(raft: Raft):
        await raft._start_election()
        await raft._initialize_volatile_state()
        if raft.has_leadership():
            await raft._initialize_leader_volatile_state()


class LeaderTask(RaftTask):
    @staticmethod
    async def run(raft: Raft):
        logging.info(f"[{datetime.now()}] LEADER({raft.id}) (term={raft.current_term})")
        while raft.has_leadership():
            await raft.send_heartbeat()
            await asyncio.sleep(raft.heartbeat_timeout)
