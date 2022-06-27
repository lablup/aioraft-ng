import asyncio
import enum
import random
import uuid
from contextlib import suppress
from typing import Iterable, Optional

from raft.interface.client import RaftClient
from raft.interface.protocol import RaftProtocol
from raft.interface.server import RaftServer


class RaftState(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class RaftFiniteStateMachine(RaftProtocol):
    def __init__(
        self,
        peers: Optional[Iterable[str]],
        server: RaftServer,
        client: RaftClient,
    ):
        self._id = str(uuid.uuid4())
        self._current_term: int = 0
        self._last_voted_term: int = 0
        self._election_timeout: float = random.random() * 0.15 + 0.15
        self._peers = tuple(peers)
        self._server = server
        self._client = client

        self._task: [asyncio.Task] = None

        self.execute_transition(RaftState.FOLLOWER)
        self._server.bind(self)

    async def main(self):
        while True:
            self.reset_timeout()
            match self._state:
                case RaftState.FOLLOWER:
                    await self._wait_for_election_timeout()
                case RaftState.CANDIDATE:
                    self._task = asyncio.create_task(
                        self._request_vote(),
                    )
                    with suppress(asyncio.CancelledError):
                        await self._task
                case RaftState.LEADER:
                    await self._publish_heartbeat()
            await asyncio.sleep(0.1)

    def execute_transition(self, next_state: RaftState):
        if task := self._task:
            task.cancel()
        self._state = next_state
        print(f'[Raft] State: {self._state.name}')

    """
    External Transitions
    """
    def on_append_entries(
        self,
        *,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[str],
        leader_commit: int,
    ) -> bool:
        print(f'[AppendEntries] term={term}')
        self.reset_timeout()
        if term >= self.current_term:
            self.execute_transition(RaftState.FOLLOWER)
            if task := self._task:
                self._task = None
                task.cancel()
        if term < self.current_term:
            return False
        return True

    def on_request_vote(
        self,
        *,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> bool:
        print(f'[RequestVote] term={term} candidate_id={candidate_id}')
        # 1. Reply false if term < currentTerm.
        if term < self.current_term:
            return False
        # self._current_term = term
        # 2. If votedFor is null or candidateId, and candidate's log is at least up-to-date as receiver's log, grant vote.
        if self._last_voted_term < term:
            self._last_voted_term = term
            return True
        return False

    def reset_timeout(self):
        self._elapsed_time: float = 0.0

    async def _wait_for_election_timeout(self, interval: float = 1.0 / 30):
        while self._elapsed_time < self._election_timeout:
            await asyncio.sleep(interval)
            self._elapsed_time += interval
        self.execute_transition(RaftState.CANDIDATE)

    async def _request_vote(self):
        self._last_vote_term = self._current_term = self._current_term + 1
        results = await asyncio.gather(*[
            asyncio.create_task(
                self._client.request_vote(
                    address=peer, term=self._current_term, candidate_id=self.id,
                    last_log_index=0, last_log_term=0,
                )
            )
            for peer in self._peers
        ])
        if sum(results) + 1 > len(self._peers) / 2:
            self.execute_transition(RaftState.LEADER)
        else:
            self.execute_transition(RaftState.FOLLOWER)

    async def _publish_heartbeat(self):
        await asyncio.wait({
            asyncio.create_task(
                self._client.request_append_entries(
                    address=peer, term=self._current_term,
                    leader_id=self.id, entries=(),
                )
            )
            for peer in self._peers
        }, return_when=asyncio.ALL_COMPLETED)

    @property
    def id(self) -> str:
        return self._id

    @property
    def current_term(self) -> int:
        return self._current_term
