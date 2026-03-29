import asyncio
import contextlib
import inspect
import json
import logging
import math
from collections.abc import Awaitable, Callable, Iterable
from typing import Final

from aioraft.client import AbstractRaftClient
from aioraft.protocol import AbstractRaftProtocol
from aioraft.protos import raft_pb2
from aioraft.server import AbstractRaftServer
from aioraft.state_machine import StateMachine
from aioraft.storage import Storage
from aioraft.types import (
    CONF_CHANGE_ADD,
    CONF_CHANGE_REMOVE,
    RaftId,
    RaftState,
    aobject,
)
from aioraft.utils import MutableInt, randrangef

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

    DEFAULT_SNAPSHOT_THRESHOLD: Final[int] = 1000
    MAX_ENTRIES_PER_BATCH: Final[int] = 100
    MIN_ELECTION_TIMEOUT: Final[float] = 0.15

    def __init__(
        self,
        id_: RaftId,
        server: AbstractRaftServer,
        client: AbstractRaftClient,
        configuration: Iterable[RaftId],
        on_state_changed: Callable[[RaftState], Awaitable] | None = None,
        state_machine: StateMachine | None = None,
        storage: Storage | None = None,
        snapshot_threshold: int | None = None,
        **kwargs,
    ):
        self.__id: Final[RaftId] = id_
        self.__server: Final[AbstractRaftServer] = server
        self.__client: Final[AbstractRaftClient] = client
        self.__configuration: set[RaftId] = set(configuration)
        self.__on_state_changed: Callable[[RaftState], Awaitable] | None = on_state_changed
        self.__state_machine: StateMachine | None = state_machine
        self.__storage: Storage | None = storage
        self.__snapshot_threshold: int = (
            snapshot_threshold if snapshot_threshold is not None else self.DEFAULT_SNAPSHOT_THRESHOLD
        )

        self.__state: RaftState = RaftState.FOLLOWER
        self.__heartbeat_timeout: Final[float] = 0.1
        self.__last_heartbeat_ack: float = 0.0
        self.__replication_in_flight: dict[RaftId, bool] = {}

        self.__vote_lock = asyncio.Lock()
        self._vote_request_lock = asyncio.Lock()

        self.__leader_id: RaftId | None = None

        # Persistent state (may be overwritten in __ainit__ from storage)
        self.__current_term: MutableInt = MutableInt(0)
        self.__voted_for: RaftId | None = None
        self.__log: list[raft_pb2.Log] = []

        # Snapshot metadata
        self.__last_included_index: int = 0
        self.__last_included_term: int = 0

        server.bind(self)

    async def __ainit__(self, *args, **kwargs):
        if self.__storage:
            await self.__storage.initialize()
            term = await self.__storage.load_term()
            self.__current_term = MutableInt(term)
            vote = await self.__storage.load_vote()
            self.__voted_for = RaftId(vote) if vote is not None else None
            self.__log = await self.__storage.load_logs()
            # Restore snapshot metadata
            snapshot = await self.__storage.load_snapshot()
            if snapshot is not None:
                self.__last_included_index = snapshot[0]
                self.__last_included_term = snapshot[1]
            # B3: Load persisted configuration first (covers compacted entries),
            # then replay any log entries after the snapshot point.
            persisted_config = await self.__storage.load_configuration()
            if persisted_config is not None:
                self.__configuration = {RaftId(p) for p in persisted_config}
            # Rebuild configuration from log entries (handles entries after snapshot)
            self._rebuild_configuration_from_log()
        else:
            await self._initialize_persistent_state()
        await self._initialize_volatile_state()

        self.__commit_event = asyncio.Event()
        self.__heartbeat_event = asyncio.Event()

        await self.__change_state(RaftState.FOLLOWER)
        await self._reset_election_timeout()

    async def main(self) -> None:
        apply_task = asyncio.create_task(self._apply_committed_entries())
        try:
            while True:
                match self.__state:
                    case RaftState.FOLLOWER:
                        await self._wait_for_election_timeout()
                    case RaftState.CANDIDATE:
                        while self.__state is RaftState.CANDIDATE:
                            # Pre-vote phase: only proceed to a real election
                            # if a majority would grant their vote.
                            if not await self._start_pre_vote():
                                await self._reset_election_timeout()
                                await asyncio.sleep(self.__election_timeout)
                                continue
                            await self._start_election()
                            await self._reset_election_timeout()
                            if self.has_leadership():
                                await self._initialize_leader_volatile_state()
                                break
                            await asyncio.sleep(self.__election_timeout)
                    case RaftState.LEADER:
                        log.info("LEADER %s term=%d", self.id, self.current_term)
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
        # Already initialized in __init__; this is a no-op but kept
        # for clarity when storage is not provided.
        pass

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
        last_log_index = self.__last_included_index + len(self.__log)
        self.__next_index: dict[RaftId, int] = {peer: last_log_index + 1 for peer in self.__configuration}
        self.__match_index: dict[RaftId, int] = {peer: 0 for peer in self.__configuration}

    async def _reset_election_timeout(self) -> None:
        self.__election_timeout: float = randrangef(self.MIN_ELECTION_TIMEOUT, 0.3)

    async def __reset_timeout(self) -> None:
        self.__heartbeat_event.set()

    async def _wait_for_election_timeout(self) -> None:
        """Wait for election timeout. Returns when timeout elapses without heartbeat."""
        while True:
            self.__heartbeat_event.clear()
            try:
                await asyncio.wait_for(
                    self.__heartbeat_event.wait(),
                    timeout=self.__election_timeout,
                )
                # Heartbeat received — reset and wait again
                continue
            except TimeoutError:
                # No heartbeat within election timeout — become candidate
                await self.__change_state(RaftState.CANDIDATE)
                return

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
        if self.__state is RaftState.LEADER and next_state is not RaftState.LEADER:
            self.__last_heartbeat_ack = 0.0  # invalidate lease on step-down
        log.info("State change %s: %s -> %s", self.__id, self.__state.name, next_state.name)
        self.__state = next_state
        if callback := self.__on_state_changed:
            if inspect.iscoroutinefunction(callback):
                await callback(next_state)
            elif inspect.isfunction(callback):
                callback(next_state)

    async def _start_pre_vote(self) -> bool:
        """Run pre-vote phase (Raft dissertation section 9.6).

        Sends PreVote RPCs to all peers with a prospective term of
        currentTerm + 1.  Returns True if a majority (including self)
        would grant their vote, meaning a real election is likely to
        succeed.  This avoids unnecessary term increments by
        partitioned or lagging nodes.
        """
        prospective_term = self.__current_term.value + 1
        last_log_index, last_log_term = self._get_last_log_info()

        if not self.__configuration:
            # Single-node cluster: pre-vote trivially succeeds.
            return True

        results = await asyncio.gather(
            *[
                asyncio.create_task(
                    self.__client.pre_vote(
                        to=server,
                        term=prospective_term,
                        candidate_id=self.id,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term,
                    ),
                )
                for server in self.__configuration
            ]
        )

        for term, _ in results:
            if term > self.current_term:
                await self.__synchronize_term(term)
                return False

        grants = sum(1 for _, granted in results if granted)
        # +1 counts self (we would vote for ourselves)
        return grants + 1 >= self.quorum

    async def _start_election(self) -> None:
        new_term = self.__current_term.value + 1
        if self.__storage:
            await self.__storage.save_term_and_vote(new_term, self.id)
        self.__current_term.set(new_term)
        async with self.__vote_lock:
            self.__voted_for = self.id

        current_term = self.current_term
        last_log_index, last_log_term = self._get_last_log_info()
        log.info("Campaign started id=%s term=%d", self.id, current_term)

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
            ),
            strict=False,
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
        index = self.__last_included_index + len(self.__log) + 1
        entry = raft_pb2.Log(
            index=index,
            term=self.current_term,
            command=command,
        )
        if self.__storage:
            await self.__storage.save_log_entry(entry)
        self.__log.append(entry)
        return entry

    async def _replicate_to_peer(self, peer_id: RaftId) -> tuple[int, bool, int]:
        """Send AppendEntries RPC to a single peer with entries from nextIndex onwards.
        If the peer is behind the snapshot, send InstallSnapshot instead.

        Returns (term, success, last_sent_index) from the peer's response.
        last_sent_index is the index of the last entry sent, or 0 if no entries were sent.
        """
        next_idx = self.__next_index.get(peer_id, 1)

        # If follower is behind snapshot, send InstallSnapshot
        if next_idx <= self.__last_included_index:
            # Prefer the persisted snapshot so metadata and data are consistent.
            snapshot_data = None
            snap_idx = self.__last_included_index
            snap_term = self.__last_included_term
            if self.__storage:
                persisted = await self.__storage.load_snapshot()
                if persisted is not None:
                    snap_idx, snap_term, snapshot_data = persisted
            # Fall back to a live snapshot with correct metadata when no
            # persisted snapshot is available.
            if snapshot_data is None and self.__state_machine:
                state_data = await self.__state_machine.snapshot()
                # B4: Include configuration in live snapshot
                snapshot_data = self._serialize_snapshot_data(state_data, self.__configuration)
                snap_idx = self.__last_applied
                entry = self._log_at_index(snap_idx)
                snap_term = entry.term if entry else self.__last_included_term
            if snapshot_data is None:
                # Cannot send snapshot; let caller retry later.
                return self.current_term, False, 0
            (resp_term,) = await self.__client.install_snapshot(
                to=peer_id,
                term=self.current_term,
                leader_id=self.id,
                last_included_index=snap_idx,
                last_included_term=snap_term,
                data=snapshot_data,
            )
            if resp_term > self.current_term:
                return resp_term, False, 0
            self.__next_index[peer_id] = snap_idx + 1
            self.__match_index[peer_id] = snap_idx
            return resp_term, True, snap_idx

        prev_log_index = next_idx - 1
        prev_log_term = 0
        if prev_log_index > 0:
            if prev_log_index == self.__last_included_index:
                prev_log_term = self.__last_included_term
            else:
                prev_entry = self._log_at_index(prev_log_index)
                if prev_entry is not None:
                    prev_log_term = prev_entry.term

        # Entries from nextIndex to end of log (adjusted for snapshot offset)
        offset = next_idx - self.__last_included_index - 1
        if offset >= 0 and offset < len(self.__log):
            entries = self.__log[offset : offset + self.MAX_ENTRIES_PER_BATCH]
        else:
            entries = []

        term, success = await self.__client.append_entries(
            to=peer_id,
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.__commit_index,
        )
        last_sent_index = entries[-1].index if entries else 0
        return term, success, last_sent_index

    async def _replicate_and_process(self, peer_id: RaftId) -> tuple[RaftId, int, bool]:
        """Replicate to a peer and process the response."""
        try:
            term, success, last_sent_index = await self._replicate_to_peer(peer_id)
            if term > self.current_term:
                await self.__synchronize_term(term)
                return (peer_id, term, False)
            if success:
                if last_sent_index > 0:
                    self.__next_index[peer_id] = last_sent_index + 1
                    self.__match_index[peer_id] = last_sent_index
                # If last_sent_index == 0, it was a heartbeat with no entries; don't change indices
            else:
                current_next = self.__next_index.get(peer_id, 1)
                if current_next > 1:
                    self.__next_index[peer_id] = current_next - 1
            return (peer_id, term, success)
        finally:
            self.__replication_in_flight[peer_id] = False

    async def _publish_heartbeat(self) -> None:
        if not self.has_leadership():
            return

        peers = list(self.__configuration)
        tasks = []
        for peer in peers:
            if not self.__replication_in_flight.get(peer, False):
                self.__replication_in_flight[peer] = True
                tasks.append(asyncio.create_task(self._replicate_and_process(peer)))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            successful = 0
            for result in results:
                if isinstance(result, tuple) and len(result) == 3:
                    _, _, success = result
                    if success:
                        successful += 1

            if successful + 1 >= self.quorum:  # +1 for self
                self.__last_heartbeat_ack = asyncio.get_running_loop().time()

        # After processing all responses, check if we can advance commitIndex
        self._update_leader_commit_index()

    def _update_leader_commit_index(self) -> None:
        """Advance commitIndex if a majority of matchIndex[i] >= N for some N > commitIndex
        and log[N].term == currentTerm."""
        if not self.has_leadership():
            return

        last_log_index = self.__last_included_index + len(self.__log)
        for n in range(last_log_index, self.__commit_index, -1):
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
            except TimeoutError:
                continue  # re-check leadership

    def _rebuild_configuration_from_log(self) -> None:
        """Replay config change entries from the log to rebuild the configuration set.
        Called on startup after loading logs from storage."""
        for entry in self.__log:
            if entry.command.startswith(CONF_CHANGE_ADD):
                address = RaftId(entry.command.split(":", 1)[1])
                self.__configuration.add(address)
            elif entry.command.startswith(CONF_CHANGE_REMOVE):
                address = RaftId(entry.command.split(":", 1)[1])
                self.__configuration.discard(address)

    @staticmethod
    def _serialize_snapshot_data(state_data: bytes, configuration: set[RaftId]) -> bytes:
        """Wrap state machine data with configuration metadata for snapshots."""
        meta = json.dumps({"config": sorted(configuration)}).encode()
        return len(meta).to_bytes(4, "big") + meta + state_data

    @staticmethod
    def _deserialize_snapshot_data(raw: bytes) -> tuple[bytes, set[RaftId]]:
        """Unwrap snapshot data into state machine data and configuration."""
        if len(raw) < 4:
            return raw, set()
        meta_len = int.from_bytes(raw[:4], "big")
        if meta_len <= 0 or 4 + meta_len > len(raw):
            return raw, set()
        try:
            meta = json.loads(raw[4 : 4 + meta_len])
            state_data = raw[4 + meta_len :]
            return state_data, {RaftId(s) for s in meta.get("config", [])}
        except (json.JSONDecodeError, UnicodeDecodeError):
            return raw, set()

    @staticmethod
    def _is_config_change(command: str) -> bool:
        """Return True if the command is a configuration change entry."""
        return command.startswith(CONF_CHANGE_ADD) or command.startswith(CONF_CHANGE_REMOVE)

    def _has_pending_config_change(self) -> bool:
        """Check if there's an uncommitted config change in the log."""
        for i in range(self.__commit_index + 1, self.__last_included_index + len(self.__log) + 1):
            entry = self._log_at_index(i)
            if entry and self._is_config_change(entry.command):
                return True
        return False

    async def add_server(self, address: RaftId) -> tuple[bool, str]:
        """Add a server to the cluster. Must be called on the leader."""
        if not self.has_leadership():
            return (False, "not leader")
        if address in self.__configuration or address == self.__id:
            return (False, "server already in cluster")
        if self._has_pending_config_change():
            return (False, "another config change is pending")
        # B1: Update configuration BEFORE appending entry so heartbeats
        # immediately see the new peer (avoids stale config window).
        self.__configuration.add(address)
        self.__next_index[address] = 1  # Start from beginning so new server catches up
        self.__match_index[address] = 0
        # Append config change entry
        entry = await self._append_entry(f"{CONF_CHANGE_ADD}:{address}")
        # Persist configuration
        if self.__storage:
            await self.__storage.save_configuration(sorted(self.__configuration))
        # Wait for commit
        try:
            await asyncio.wait_for(self._wait_for_commit(entry.index), timeout=10.0)
        except (TimeoutError, RuntimeError):
            return (False, "failed to commit config change")
        return (True, "")

    async def remove_server(self, address: RaftId) -> tuple[bool, str]:
        """Remove a server from the cluster. Must be called on the leader."""
        if not self.has_leadership():
            return (False, "not leader")
        if address not in self.__configuration and address != self.__id:
            return (False, "server not in cluster")
        if self._has_pending_config_change():
            return (False, "another config change is pending")

        # B6: Handle self-removal explicitly
        if address == self.__id:
            entry = await self._append_entry(f"{CONF_CHANGE_REMOVE}:{address}")
            try:
                await asyncio.wait_for(self._wait_for_commit(entry.index), timeout=10.0)
            except (TimeoutError, RuntimeError):
                return (False, "failed to commit config change")
            await self.__change_state(RaftState.FOLLOWER)
            return (True, "")

        # Config takes effect immediately
        self.__configuration.discard(address)
        entry = await self._append_entry(f"{CONF_CHANGE_REMOVE}:{address}")
        # Persist configuration
        if self.__storage:
            await self.__storage.save_configuration(sorted(self.__configuration))
        # B2: Don't delete replication state yet - keep replicating so the
        # removed server receives the config change entry.
        try:
            await asyncio.wait_for(self._wait_for_commit(entry.index), timeout=10.0)
        except (TimeoutError, RuntimeError):
            return (False, "failed to commit config change")
        # Now safe to clean up replication state after commit
        self.__next_index.pop(address, None)
        self.__match_index.pop(address, None)
        return (True, "")

    def has_leadership(self) -> bool:
        return self.__state is RaftState.LEADER

    def _has_valid_lease(self) -> bool:
        """Check if the leader lease is still valid.

        A leader that has received acknowledgments from a majority within
        the election timeout can serve reads without going through the log.
        """
        if not self.has_leadership():
            return False
        elapsed = asyncio.get_running_loop().time() - self.__last_heartbeat_ack
        # Lease is valid if within the minimum election timeout
        return elapsed < self.MIN_ELECTION_TIMEOUT

    """
    AbstractRaftProtocol
    """

    async def on_client_request(self, command: str) -> tuple[bool, str, str | None]:
        """Handle a client command request.

        Returns (success, result, leader_hint).
        """
        if not self.has_leadership():
            return (False, "", self.__leader_id)

        # B5: Reject commands that look like config changes to prevent injection
        if self._is_config_change(command):
            return (False, "invalid command: reserved prefix", None)

        # Append to local log
        entry = await self._append_entry(command)
        target_index = entry.index

        # Wait for the entry to be committed (replicated to a majority)
        try:
            await asyncio.wait_for(self._wait_for_commit(target_index), timeout=5.0)
        except (TimeoutError, RuntimeError):
            return (False, "lost leadership or timeout", None)

        # The background _apply_committed_entries loop handles applying to
        # the state machine, so we don't apply inline (avoids double-apply).
        return (True, "", None)

    async def on_read_request(self, query: str) -> tuple[bool, str, str | None]:
        """Handle a read-only query. Uses leader lease if valid, otherwise redirects."""
        if not self.has_leadership():
            return (False, "", self.__leader_id)
        if not self._has_valid_lease():
            # Lease expired; ask client to retry instead of polluting the log
            return (False, "lease expired, retry", None)
        if not self.__state_machine:
            return (False, "no state machine", None)
        # Wait for state machine to be up-to-date before serving read
        while self.__last_applied < self.__commit_index:
            self.__commit_event.clear()
            if self.__last_applied >= self.__commit_index:
                break
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.__commit_event.wait(), timeout=0.5)
        try:
            result = await self.__state_machine.query(query)
            return (True, str(result) if result is not None else "", None)
        except Exception as e:
            return (False, str(e), None)

    async def on_append_entries(
        self,
        *,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
    ) -> tuple[int, bool]:
        # Rule 1: Reply false if term < currentTerm
        if term < (current_term := self.current_term):
            return (current_term, False)

        await self.__synchronize_term(term)

        # Track the leader
        self.__leader_id = leader_id

        # Rule 2: Reply false if log doesn't contain an entry at prevLogIndex
        # whose term matches prevLogTerm
        if prev_log_index > 0:
            if prev_log_index == self.__last_included_index:
                # The entry is at the snapshot boundary
                if prev_log_term != self.__last_included_term:
                    return (self.current_term, False)
            else:
                prev_entry = self._log_at_index(prev_log_index)
                if prev_entry is None or prev_entry.term != prev_log_term:
                    return (self.current_term, False)

        # Process entries (rules 3 & 4)
        entries_list = list(entries)
        new_entries_to_append: list[raft_pb2.Log] = []
        truncate_from: int | None = None

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
            adjusted = truncate_from - self.__last_included_index - 1
            self.__log = self.__log[:adjusted]
            self.__log.extend(new_entries_to_append)
        elif new_entries_to_append:
            if self.__storage:
                await self.__storage.append_logs(new_entries_to_append)
            self.__log.extend(new_entries_to_append)

        # Apply configuration changes from new entries immediately
        config_changed = False
        for entry in new_entries_to_append:
            if entry.command.startswith(CONF_CHANGE_ADD):
                address = RaftId(entry.command.split(":", 1)[1])
                self.__configuration.add(address)
                config_changed = True
            elif entry.command.startswith(CONF_CHANGE_REMOVE):
                address = RaftId(entry.command.split(":", 1)[1])
                self.__configuration.discard(address)
                config_changed = True
        # B3: Persist configuration whenever it changes
        if config_changed and self.__storage:
            await self.__storage.save_configuration(sorted(self.__configuration))

        # Rule 5: advance commitIndex
        # Per Raft paper: set commitIndex = min(leaderCommit, index of last new entry)
        if leader_commit > self.__commit_index:
            if entries_list:
                last_new_index = entries_list[-1].index
            else:
                # Heartbeat: use follower's actual last log index
                last_new_index = self.__last_included_index + len(self.__log)
            self.__commit_index = min(leader_commit, last_new_index)
            self.__commit_event.set()

        await self.__reset_timeout()
        return (self.current_term, True)

    async def on_request_vote(
        self,
        *,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> tuple[int, bool]:
        async with self._vote_request_lock:
            if term < (current_term := self.current_term):
                log.debug(
                    "[on_request_vote] FALSE id=%s term=%d candidate=%s req_term=%d",
                    self.__id[-5:],
                    current_term,
                    candidate_id[-5:],
                    term,
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
                        "[on_request_vote] TRUE id=%s term=%d candidate=%s req_term=%d",
                        self.__id[-5:],
                        current_term,
                        candidate_id[-5:],
                        term,
                    )
                    if self.__storage:
                        await self.__storage.save_vote(candidate_id)
                    self.__voted_for = candidate_id
                    await self.__reset_timeout()
                    return (self.current_term, True)
            log.debug(
                "[on_request_vote] FALSE id=%s term=%d candidate=%s req_term=%d",
                self.__id[-5:],
                current_term,
                candidate_id[-5:],
                term,
            )
            return (self.current_term, False)

    async def on_pre_vote(
        self,
        *,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> tuple[int, bool]:
        """PreVote handler (Raft dissertation section 9.6).

        Responds based on whether we *would* vote for the candidate,
        without actually granting a vote, updating our term, or
        resetting our election timer.
        """
        current_term = self.current_term

        # Reject if the prospective term is behind our current term
        if term < current_term:
            return (current_term, False)

        # Reject if we have a current leader (haven't timed out)
        if self.__leader_id is not None and self.__heartbeat_event.is_set():
            return (current_term, False)

        # Check if the candidate's log is at least as up-to-date as ours
        my_last_index, my_last_term = self._get_last_log_info()
        if last_log_term < my_last_term:
            return (current_term, False)
        if last_log_term == my_last_term and last_log_index < my_last_index:
            return (current_term, False)

        return (current_term, True)

    async def _apply_committed_entries(self) -> None:
        """Background task: apply committed entries to state machine."""
        while True:
            while self.__last_applied < self.__commit_index:
                # Entry is marked applied before execution. If apply() fails,
                # the entry is skipped to maintain consistency with peers
                # (all nodes apply the same sequence).
                self.__last_applied += 1
                entry = self._log_at_index(self.__last_applied)
                if entry and self._is_config_change(entry.command):
                    continue  # Config changes don't go to state machine
                if entry and self.__state_machine:
                    try:
                        await self.__state_machine.apply(entry.command)
                    except Exception as e:
                        log.error("Failed to apply entry %d: %s", self.__last_applied, e)
            # Check if log compaction is needed after applying entries
            await self._maybe_create_snapshot()
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

    async def _maybe_create_snapshot(self) -> None:
        """Create a snapshot if the log has grown beyond the threshold."""
        if len(self.__log) <= self.__snapshot_threshold:
            return
        if not self.__state_machine:
            return
        if self.__last_applied <= self.__last_included_index:
            return

        state_data = await self.__state_machine.snapshot()
        last_index = self.__last_applied
        last_entry = self._log_at_index(last_index)
        if last_entry is None:
            return
        last_term = last_entry.term

        # B4: Include configuration in snapshot data so followers can restore it
        data = self._serialize_snapshot_data(state_data, self.__configuration)

        # Discard compacted log entries
        entries_to_discard = last_index - self.__last_included_index
        remaining_logs = self.__log[entries_to_discard:]

        if self.__storage:
            await self.__storage.compact_log_with_snapshot(
                last_index,
                last_term,
                data,
                remaining_logs,
            )
            # B3: Persist configuration alongside snapshot so it survives compaction
            await self.__storage.save_configuration(sorted(self.__configuration))

        self.__log = remaining_logs
        self.__last_included_index = last_index
        self.__last_included_term = last_term

    async def on_install_snapshot(
        self,
        *,
        term: int,
        leader_id: RaftId,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
    ) -> tuple[int]:
        """Handle InstallSnapshot RPC from the leader."""
        if term < self.current_term:
            return (self.current_term,)

        # B4: Guard against stale/duplicate snapshots
        if last_included_index <= self.__last_included_index:
            return (self.current_term,)

        await self.__synchronize_term(term)
        await self.__reset_timeout()
        self.__leader_id = leader_id

        # B4: Deserialize snapshot data to extract configuration and state machine data
        state_data, snapshot_config = self._deserialize_snapshot_data(data)

        # Restore state machine
        if self.__state_machine:
            await self.__state_machine.restore(state_data)

        # B4: Restore configuration from snapshot
        if snapshot_config:
            self.__configuration = snapshot_config

        # Discard entire log up to snapshot
        remaining_logs = [e for e in self.__log if e.index > last_included_index]

        # Persist atomically
        if self.__storage:
            await self.__storage.compact_log_with_snapshot(
                last_included_index,
                last_included_term,
                data,
                remaining_logs,
            )
            # Persist restored configuration
            await self.__storage.save_configuration(sorted(self.__configuration))

        self.__log = remaining_logs
        self.__last_included_index = last_included_index
        self.__last_included_term = last_included_term
        self.__commit_index = max(self.__commit_index, last_included_index)
        self.__last_applied = last_included_index

        return (self.current_term,)

    def _get_last_log_info(self) -> tuple[int, int]:
        """Return (last_log_index, last_log_term) for the local log."""
        if self.__log:
            last = self.__log[-1]
            return (last.index, last.term)
        if self.__last_included_index > 0:
            return (self.__last_included_index, self.__last_included_term)
        return (0, 0)

    def _log_at_index(self, index: int) -> raft_pb2.Log | None:
        """Return the log entry at the given 1-based index, or None.
        Accounts for snapshot offset."""
        if index <= self.__last_included_index:
            return None
        adjusted = index - self.__last_included_index - 1
        if 0 <= adjusted < len(self.__log):
            return self.__log[adjusted]
        return None

    @property
    def id(self) -> RaftId:
        return self.__id

    @property
    def current_term(self) -> int:
        return self.__current_term.value

    @property
    def voted_for(self) -> RaftId | None:
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
    def _heartbeat_event(self) -> asyncio.Event:
        return self.__heartbeat_event

    @property
    def membership(self) -> int:
        return len(self.__configuration) + 1

    @property
    def quorum(self) -> int:
        return math.floor(self.membership / 2) + 1

    @property
    def configuration(self) -> set[RaftId]:
        """Return the current cluster membership (peers, excluding self)."""
        return set(self.__configuration)
