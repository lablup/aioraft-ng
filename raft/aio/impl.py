import asyncio
import logging
import math
from datetime import datetime
from typing import Dict, Final, Iterable, Optional, Set, Tuple

from raft.aio.logs import AbstractReplicatedLog, MemoryReplicatedLog
from raft.aio.peers import AbstractRaftPeer
from raft.aio.protocols import AbstractRaftClusterProtocol, AbstractRaftProtocol
from raft.aio.resp import RespInterpreter
from raft.aio.server import AbstractRaftServer
from raft.protos import raft_pb2
from raft.types import (
    ClientQueryResponse,
    ClientRequestResponse,
    RaftClusterStatus,
    RaftId,
    RaftState,
    RegisterClientResponse,
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

        self.__interpreter = RespInterpreter()

        server.bind(self)

    async def __ainit__(self, *args, **kwargs):
        await self._initialize_persistent_state()
        await self._initialize_volatile_state()

        await self.__change_state(RaftState.FOLLOWER)
        await self.__restart_timeout()

    async def main(self) -> None:
        while True:
            match self.__state:
                case RaftState.FOLLOWER:
                    await self.__restart_timeout()
                    await self._wait_for_election_timeout()
                case RaftState.CANDIDATE:
                    while self.__state is RaftState.CANDIDATE:
                        await self._start_election()
                        await self._initialize_volatile_state()
                        if self.has_leadership():
                            await self._initialize_leader_volatile_state()
                            break
                        await asyncio.sleep(self.__election_timeout)
                case RaftState.LEADER:
                    logging.info(
                        f"[{datetime.now()}] LEADER({self.id}, term={self.current_term})"
                    )
                    while self.has_leadership():
                        await self._publish_heartbeat()
                        await asyncio.sleep(self.__heartbeat_timeout)

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
        self.__log: AbstractReplicatedLog = MemoryReplicatedLog()

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

    async def __restart_timeout(self) -> None:
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
        await self.__restart_timeout()

        current_term = self.current_term
        logging.info(f"[{datetime.now()}] id={self.id} Campaign(term={current_term})")

        terms, grants = zip(
            *await asyncio.gather(
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
        )

        for term in terms:
            if term > current_term:
                await self.__synchronize_term(term)
                break
        else:
            if sum(grants) + 1 >= self.quorum:
                await self.__change_state(RaftState.LEADER)
                return True

        return False

    async def _publish_heartbeat(self) -> None:
        await self._replicate_logs(entries=())

    async def _replicate_logs(self, entries: Iterable[raft_pb2.Log]) -> None:
        if not self.has_leadership():
            return
        terms, successes = zip(
            *await asyncio.gather(
                *[
                    asyncio.create_task(
                        self.__peer.append_entries(
                            to=server,
                            term=self.current_term,
                            leader_id=self.id,
                            prev_log_index=0,
                            prev_log_term=0,
                            entries=(),
                            leader_commit=self.__commit_index,
                        ),
                    )
                    for server in self.__configuration
                ]
            )
        )

    async def _commit_log(self, index: int) -> Optional[int]:
        if not (log := await self.__log.get(index=index)):
            return None
        output = self.__interpreter.execute(log.command)
        await self.__log.commit(index=index)
        return output

    def has_leadership(self) -> bool:
        return self.__state is RaftState.LEADER

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
    ) -> Tuple[int, bool]:
        await self.__restart_timeout()
        if term < (current_term := self.current_term):
            return (current_term, False)
        self.__leader_id = leader_id
        await self.__synchronize_term(term)
        return (self.current_term, True)

    async def on_request_vote(
        self,
        *,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        await self.__restart_timeout()
        if term < (current_term := self.current_term):
            return (current_term, False)
        await self.__synchronize_term(term)

        if self.voted_for in [None, candidate_id]:
            self.__voted_for = candidate_id
            return (self.current_term, True)
        return (self.current_term, False)

    """
    AbstractRaftClusterProtocol
    """

    async def on_client_request(
        self, *, client_id: str, sequence_num: int, command: str
    ) -> ClientRequestResponse:
        # 1. Reply NOT_LEADER if not leader, providing hint when available
        if not self.has_leadership():
            return ClientRequestResponse(
                status=RaftClusterStatus.NOT_LEADER,
                response=None,
                leader_hint=self.__leader_id,
            )
        # 2. Append command to log, replicate and commit it
        index = await self.__log.count() + 1
        entries: Iterable[raft_pb2.Log] = [
            raft_pb2.Log(index=index, term=self.current_term, command=command)
        ]
        await self.__log.append(entries)
        await self._replicate_logs(entries=entries)
        output = await self._commit_log(index=index)
        # 3. Reply SESSION_EXPIRED if no record of clientId or if response for client's sequenceNum already discarded
        # 4. If sequenceNum already processed from client, reply OK with stored response
        # 5. Apply command in log order
        # 6. Save state machine output with sequenceNum for client, discard any prior response for client
        # 7. Reply OK with state machine output
        # output = ""
        return ClientRequestResponse(
            status=RaftClusterStatus.OK, response=str(output), leader_hint=self.id
        )

    async def on_register_client(self) -> RegisterClientResponse:
        pass

    async def on_client_query(self, *, query: str) -> ClientQueryResponse:
        pass

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
    def membership(self) -> int:
        return len(self.__configuration) + 1

    @property
    def quorum(self) -> int:
        return math.floor(self.membership / 2) + 1