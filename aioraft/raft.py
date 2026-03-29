import asyncio
import inspect
import logging
import math
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Final, Iterable, List, Optional, Set, Tuple

from aioraft.client import AbstractRaftClient
from aioraft.protocol import AbstractRaftProtocol
from aioraft.protos import raft_pb2
from aioraft.server import AbstractRaftServer
from aioraft.state_machine import StateMachine
from aioraft.storage import Storage
from aioraft.types import RaftId, RaftState, aobject
from aioraft.utils import AtomicInteger, randrangef

logging.basicConfig(level=logging.INFO)

__all__ = ("Raft",)

log = logging.getLogger(__name__)


class Raft(aobject, AbstractRaftProtocol):
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
        client: AbstractRaftClient,
        configuration: Iterable[RaftId],
        on_state_changed: Optional[Callable[[RaftState], Awaitable]] = None,
        state_machine: Optional[StateMachine] = None,
        storage: Optional[Storage] = None,
        **kwargs,
    ):
        self.__id: Final[RaftId] = id_
        self.__server: Final[AbstractRaftServer] = server
        self.__client: Final[AbstractRaftClient] = client
        self.__configuration: Set[RaftId] = set(configuration)
        self.__on_state_changed: Optional[
            Callable[[RaftState], Awaitable]
        ] = on_state_changed
        self.__state_machine: Optional[StateMachine] = state_machine
        self.__storage: Optional[Storage] = storage

        self.__state: RaftState = RaftState.FOLLOWER
        self.__heartbeat_timeout: Final[float] = 0.1

        self.__vote_lock = asyncio.Lock()
        self._vote_request_lock = asyncio.Lock()

        self.__leader_id: Optional[RaftId] = None

        server.bind(self)

    async def __ainit__(self, *args, **kwargs):
        if self.__storage:
            await self.__storage.initialize()
            term = await self.__storage.load_term()
            self.__current_term = AtomicInteger(term)
            self.__voted_for = await self.__storage.load_vote()
            self.__log = await self.__storage.load_logs()
        else:
            await self._initialize_persistent_state()
        await self._initialize_volatile_state()

        self.__commit_event = asyncio.Event()

        await self.__change_state(RaftState.FOLLOWER)
        await self.__reset_timeout()
        await self._reset_election_timeout()

    async def main(self) -> None:
        apply_task = asyncio.create_task(self._apply_committed_entries())
        try:
            while True:
                match self.__state:
                    case RaftState.FOLLOWER:
                        await self.__reset_timeout()
                        await self._wait_for_election_timeout()
                    case RaftState.CANDIDATE:
                        while self.__state is RaftState.CANDIDATE:
                            await self._start_election()
                            await self._reset_election_timeout()
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
        finally:
            apply_task.cancel()

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
        self.__log: List[raft_pb2.Log] = []

    async def _initialize_volatile_state(self) -> None:
        """Volatile state on all servers
        (Initialized once on first boot)

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
        last_log_index = len(self.__log)
        self.__next_index: Dict[RaftId, int] = {
            peer: last_log_index + 1 for peer in self.__configuration
        }
        self.__match_index: Dict[RaftId, int] = {
            peer: 0 for peer in self.__configuration
        }

    async def _reset_election_timeout(self) -> None:
        self.__election_timeout: float = randrangef(0.15, 0.3)

    async def __reset_timeout(self) -> None:
        self.__elapsed_time: float = 0.0

    async def _wait_for_election_timeout(self, interval: float = 1.0 / 30) -> None:
        while self.__elapsed_time < self.__election_timeout:
            await asyncio.sleep(interval)
            self.__elapsed_time += interval
        await self.__change_state(RaftState.CANDIDATE)

    async def __synchronize_term(self, term: int) -> None:
        if term > self.current_term:
            if self.__storage:
                await self.__storage.save_term_and_vote(term, None)
            self.__current_term.set(term)
            await self.__change_state(RaftState.FOLLOWER)
            async with self.__vote_lock:
                self.__voted_for = None

    async def __change_state(self, next_state: RaftState) -> None:
        if self.__state is next_state:
            return
        log.debug(
            f"[{self.__id.split(':')[-1]}] change_state(): {self.__state} -> {next_state}"
        )
        self.__state = next_state
        if callback := self.__on_state_changed:
            if inspect.iscoroutinefunction(callback):
                await callback(next_state)
            elif inspect.isfunction(callback):
                callback(next_state)

    async def _start_election(self) -> None:
        new_term = self.__current_term.value + 1
        if self.__storage:
            await self.__storage.save_term_and_vote(new_term, self.id)
        self.__current_term.set(new_term)
        async with self.__vote_lock:
            self.__voted_for = self.id

        current_term = self.current_term
        last_log_index, last_log_term = self._get_last_log_info()
        logging.info(f"[{datetime.now()}] id={self.id} Campaign(term={current_term})")

        terms, grants = zip(
            *await asyncio.gather(
                *[
                    asyncio.create_task(
                        self.__client.request_vote(
                            to=server,
                            term=current_term,
                            candidate_id=self.id,
                            last_log_index=last_log_index,
                            last_log_term=last_log_term,
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

    async def _append_entry(self, command: str) -> raft_pb2.Log:
        """Append a new entry to the local log. Returns the created entry."""
        index = len(self.__log) + 1
        entry = raft_pb2.Log(
            index=index,
            term=self.current_term,
            command=command,
        )
        if self.__storage:
            await self.__storage.save_log_entry(entry)
        self.__log.append(entry)
        return entry

    async def _replicate_to_peer(self, peer_id: RaftId) -> Tuple[int, bool]:
        """Send AppendEntries RPC to a single peer with entries from nextIndex onwards.

        Returns (term, success) from the peer's response.
        """
        next_idx = self.__next_index.get(peer_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = 0
        if prev_log_index > 0:
            prev_entry = self._log_at_index(prev_log_index)
            if prev_entry is not None:
                prev_log_term = prev_entry.term

        # Entries from nextIndex to end of log
        entries = self.__log[next_idx - 1:] if next_idx - 1 < len(self.__log) else []

        term, success = await self.__client.append_entries(
            to=peer_id,
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.__commit_index,
        )
        return term, success

    async def _publish_heartbeat(self) -> None:
        if not self.has_leadership():
            return

        results = await asyncio.gather(
            *[
                asyncio.create_task(self._replicate_to_peer(server))
                for server in self.__configuration
            ]
        )

        for peer_id, (term, success) in zip(self.__configuration, results):
            if term > self.current_term:
                await self.__synchronize_term(term)
                return
            if success:
                # Update nextIndex and matchIndex on success
                last_log_index = len(self.__log)
                self.__next_index[peer_id] = last_log_index + 1
                self.__match_index[peer_id] = last_log_index
            else:
                # Decrement nextIndex on failure (log inconsistency)
                current_next = self.__next_index.get(peer_id, 1)
                if current_next > 1:
                    self.__next_index[peer_id] = current_next - 1

        # After processing all responses, check if we can advance commitIndex
        self._update_leader_commit_index()

    def _update_leader_commit_index(self) -> None:
        """Advance commitIndex if a majority of matchIndex[i] >= N for some N > commitIndex
        and log[N].term == currentTerm."""
        if not self.has_leadership():
            return

        for n in range(len(self.__log), self.__commit_index, -1):
            entry = self._log_at_index(n)
            if entry is None or entry.term != self.current_term:
                continue

            # Count replicas: leader itself counts as 1
            replication_count = 1
            for peer_id in self.__configuration:
                if self.__match_index.get(peer_id, 0) >= n:
                    replication_count += 1

            if replication_count >= self.quorum:
                self._update_commit_index(n)
                break

    async def _wait_for_commit(self, index: int) -> None:
        """Block until commitIndex >= index. Raises RuntimeError if leadership is lost."""
        while self.__commit_index < index:
            if not self.has_leadership():
                raise RuntimeError("lost leadership")
            self.__commit_event.clear()
            if self.__commit_index >= index:
                return
            try:
                await asyncio.wait_for(self.__commit_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue  # re-check leadership

    def has_leadership(self) -> bool:
        return self.__state is RaftState.LEADER

    """
    AbstractRaftProtocol
    """

    async def on_client_request(self, command: str) -> Tuple[bool, str, Optional[str]]:
        """Handle a client command request.

        Returns (success, result, leader_hint).
        """
        if not self.has_leadership():
            return (False, "", self.__leader_id)

        # Append to local log
        entry = await self._append_entry(command)
        target_index = entry.index

        # Wait for the entry to be committed (replicated to a majority)
        try:
            await asyncio.wait_for(self._wait_for_commit(target_index), timeout=5.0)
        except (asyncio.TimeoutError, RuntimeError):
            return (False, "lost leadership or timeout", None)

        # The background _apply_committed_entries loop handles applying to
        # the state machine, so we don't apply inline (avoids double-apply).
        return (True, "", None)

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
        # Rule 1: Reply false if term < currentTerm
        if term < (current_term := self.current_term):
            return (current_term, False)

        await self.__reset_timeout()
        await self.__synchronize_term(term)

        # Track the leader
        self.__leader_id = leader_id

        # Rule 2: Reply false if log doesn't contain an entry at prevLogIndex
        # whose term matches prevLogTerm
        if prev_log_index > 0:
            prev_entry = self._log_at_index(prev_log_index)
            if prev_entry is None or prev_entry.term != prev_log_term:
                return (self.current_term, False)

        # Process entries (rules 3 & 4)
        entries_list = list(entries)
        new_entries_to_append: List[raft_pb2.Log] = []
        truncate_from: Optional[int] = None

        for i, new_entry in enumerate(entries_list):
            insert_index = prev_log_index + 1 + i
            existing = self._log_at_index(insert_index)
            if existing is not None:
                if existing.term != new_entry.term:
                    # Rule 3: conflict - mark truncation point and collect
                    # this entry plus all remaining entries for append
                    truncate_from = insert_index
                    new_entries_to_append = entries_list[i:]
                    break
                else:
                    # Entry already exists with same term, skip
                    pass
            else:
                # Rule 4: collect remaining entries for append
                new_entries_to_append = entries_list[i:]
                break

        # Persist before updating in-memory state
        if truncate_from is not None:
            if self.__storage:
                await self.__storage.truncate_and_append(truncate_from, new_entries_to_append)
            self.__log = self.__log[:truncate_from - 1]
            self.__log.extend(new_entries_to_append)
        elif new_entries_to_append:
            if self.__storage:
                await self.__storage.append_logs(new_entries_to_append)
            self.__log.extend(new_entries_to_append)

        # Rule 5: advance commitIndex
        # Per Raft paper: set commitIndex = min(leaderCommit, index of last new entry)
        if leader_commit > self.__commit_index:
            if entries_list:
                last_new_index = entries_list[-1].index
            else:
                last_new_index = prev_log_index  # heartbeat case
            self.__commit_index = min(leader_commit, last_new_index)
            self.__commit_event.set()

        return (self.current_term, True)

    async def on_request_vote(
        self,
        *,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        async with self._vote_request_lock:
            if term < (current_term := self.current_term):
                log.debug(
                    f"[on_request_vote] FALSE id={self.__id[-5:]} current_term={current_term} candidate={candidate_id[-5:]} term={term}"
                )
                return (current_term, False)
            await self.__synchronize_term(term)

            async with self.__vote_lock:
                if self.voted_for in [None, candidate_id]:
                    # Section 5.4.1: only grant vote if candidate's log is
                    # at least as up-to-date as ours.
                    my_last_index, my_last_term = self._get_last_log_info()
                    if last_log_term < my_last_term:
                        return (self.current_term, False)
                    if last_log_term == my_last_term and last_log_index < my_last_index:
                        return (self.current_term, False)

                    log.debug(
                        f"[on_request_vote] TRUE id={self.__id[-5:]} current_term={current_term} candidate={candidate_id[-5:]} term={term} voted_for={self.voted_for}"
                    )
                    if self.__storage:
                        await self.__storage.save_vote(candidate_id)
                    self.__voted_for = candidate_id
                    await self.__reset_timeout()
                    return (self.current_term, True)
            log.debug(
                f"[on_request_vote] FALSE id={self.__id[-5:]} current_term={current_term} candidate={candidate_id[-5:]} term={term} voted_for={self.voted_for}"
            )
            return (self.current_term, False)

    async def _apply_committed_entries(self) -> None:
        """Background task: apply committed entries to state machine."""
        while True:
            while self.__last_applied < self.__commit_index:
                # Entry is marked applied before execution. If apply() fails,
                # the entry is skipped to maintain consistency with peers
                # (all nodes apply the same sequence).
                self.__last_applied += 1
                entry = self._log_at_index(self.__last_applied)
                if entry and self.__state_machine:
                    try:
                        await self.__state_machine.apply(entry.command)
                    except Exception as e:
                        log.error(f"Failed to apply entry {self.__last_applied}: {e}")
            self.__commit_event.clear()
            # Re-check after clearing to avoid race where a set() between
            # the end of the drain loop and clear() would be lost.
            if self.__last_applied < self.__commit_index:
                continue
            await self.__commit_event.wait()

    def _update_commit_index(self, n: int) -> None:
        """Advance commitIndex to n and wake the apply loop.

        Called by the leader when a majority of matchIndex[i] >= n and
        log[n].term == currentTerm (Raft paper §5.3/§5.4).
        """
        if n > self.__commit_index:
            self.__commit_index = n
            self.__commit_event.set()

    def _get_last_log_info(self) -> Tuple[int, int]:
        """Return (last_log_index, last_log_term) for the local log."""
        if self.__log:
            last = self.__log[-1]
            return (last.index, last.term)
        return (0, 0)

    def _log_at_index(self, index: int) -> Optional[raft_pb2.Log]:
        """Return the log entry at the given 1-based index, or None."""
        if 1 <= index <= len(self.__log):
            return self.__log[index - 1]
        return None

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
    def commit_index(self) -> int:
        return self.__commit_index

    @property
    def last_applied(self) -> int:
        return self.__last_applied

    @property
    def state(self) -> RaftState:
        return self.__state

    @property
    def _elapsed_time(self) -> float:
        return self.__elapsed_time

    @property
    def membership(self) -> int:
        return len(self.__configuration) + 1

    @property
    def quorum(self) -> int:
        return math.floor(self.membership / 2) + 1
