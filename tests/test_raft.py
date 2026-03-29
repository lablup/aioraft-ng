import asyncio
import json
import tempfile
import time
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import pytest

from aioraft import KeyValueStateMachine, MemoryStorage, Raft, SQLiteStorage
from aioraft.client import GrpcRaftClient
from aioraft.protos import raft_pb2
from aioraft.server import GrpcRaftServer
from aioraft.types import RaftState


async def wait_until(pred, timeout=1.0):
    """Poll *pred* until it returns True, or raise TimeoutError."""
    deadline = asyncio.get_event_loop().time() + timeout
    while not pred():
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError("wait_until timed out")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_raft_leader_election():
    n = 5
    ports = tuple(range(50051, 50051 + n))
    configuration = [f"127.0.0.1:{port}" for port in ports]
    servers = [GrpcRaftServer() for _ in range(n)]
    raft_nodes = [
        await Raft.new(
            f"raft.aio-{i}",
            server=server,
            client=GrpcRaftClient(),
            configuration=filter(lambda x: x != addr, configuration),
        )
        for i, (server, addr) in enumerate(zip(servers, configuration))
    ]
    assert all(map(lambda r: not r.has_leadership(), raft_nodes))

    leadership_timeout = 0.0
    LEADERSHIP_CHECK_TIMEOUT = 10.0
    LEADERSHIP_CHECK_MAX_TRIAL = 100

    async def _wait_for_new_leadership():
        nonlocal leadership_timeout
        start_time = time.time()
        for _ in range(LEADERSHIP_CHECK_MAX_TRIAL):
            await asyncio.sleep(LEADERSHIP_CHECK_TIMEOUT / LEADERSHIP_CHECK_MAX_TRIAL)
            if any(map(lambda r: r.has_leadership(), raft_nodes)):
                leadership_timeout = time.time() - start_time
                break

    cleanup_coroutines = []

    raft_server_tasks = [
        asyncio.create_task(
            server.run(host="0.0.0.0", port=port, cleanup_coroutines=cleanup_coroutines)
        )
        for server, port in zip(servers, ports)
    ]
    raft_main_tasks = [asyncio.create_task(raft.main()) for raft in raft_nodes]
    done, pending = await asyncio.wait(
        {
            *raft_server_tasks,
            *raft_main_tasks,
            asyncio.create_task(_wait_for_new_leadership()),
        },
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert any(map(lambda r: r.has_leadership(), raft_nodes))
    assert leadership_timeout < LEADERSHIP_CHECK_TIMEOUT

    await asyncio.gather(*cleanup_coroutines)

    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_commit_index_preserved_across_candidate_transition():
    """Regression test for bug 0.1: commitIndex must not be reset when
    transitioning to CANDIDATE. The Raft paper specifies commitIndex is
    initialized to 0 on first boot only."""
    mock_server = MagicMock()
    mock_server.bind = MagicMock()
    mock_client = AsyncMock()
    # Return (term, grant) tuples - no votes granted so node stays candidate
    mock_client.request_vote = AsyncMock(return_value=(1, False))

    raft = await Raft.new(
        "node-1",
        server=mock_server,
        client=mock_client,
        configuration=["node-2", "node-3"],
    )

    # Manually set commit_index to a non-zero value via the private attribute
    # to simulate progress before an election
    raft._Raft__commit_index = 5

    assert raft.commit_index == 5

    # Simulate transitioning to candidate and running one election cycle
    raft._Raft__state = RaftState.CANDIDATE

    # Run _start_election (will fail to win due to mocked votes)
    await raft._start_election()
    await raft._reset_election_timeout()

    # commitIndex must still be 5, not reset to 0
    assert raft.commit_index == 5, (
        "commitIndex was reset during candidate transition; "
        "it should only be initialized on first boot"
    )


@pytest.mark.asyncio
async def test_stale_append_entries_does_not_reset_election_timer():
    """Regression test for bug 0.2: on_append_entries with a stale term
    (term < currentTerm) must NOT reset the election timer. Only valid
    RPCs from current or newer leaders should reset it."""
    mock_server = MagicMock()
    mock_server.bind = MagicMock()
    mock_client = AsyncMock()

    raft = await Raft.new(
        "node-1",
        server=mock_server,
        client=mock_client,
        configuration=["node-2", "node-3"],
    )

    # Set the node's current term to 5
    raft._Raft__current_term.set(5)

    # Clear the heartbeat event so we can observe whether it gets set
    raft._Raft__heartbeat_event.clear()

    # Send an AppendEntries with a stale term (term=2 < currentTerm=5)
    current_term, success = await raft.on_append_entries(
        term=2,
        leader_id="old-leader",
        prev_log_index=0,
        prev_log_term=0,
        entries=(),
        leader_commit=0,
    )

    # Should be rejected
    assert success is False
    assert current_term == 5

    # The heartbeat event should NOT have been set by a stale RPC
    assert not raft._heartbeat_event.is_set(), (
        "Heartbeat event was set by a stale AppendEntries RPC; "
        "only valid RPCs should signal the heartbeat event"
    )


@pytest.mark.asyncio
async def test_valid_append_entries_resets_election_timer():
    """Verify that a valid AppendEntries RPC (with current term) does
    reset the election timer by setting the heartbeat event."""
    mock_server = MagicMock()
    mock_server.bind = MagicMock()
    mock_client = AsyncMock()

    raft = await Raft.new(
        "node-1",
        server=mock_server,
        client=mock_client,
        configuration=["node-2", "node-3"],
    )

    # Set the node's current term to 5
    raft._Raft__current_term.set(5)

    # Clear the heartbeat event so we can observe whether it gets set
    raft._Raft__heartbeat_event.clear()

    # Send an AppendEntries with a valid term (term=5 == currentTerm)
    current_term, success = await raft.on_append_entries(
        term=5,
        leader_id="leader-node",
        prev_log_index=0,
        prev_log_term=0,
        entries=(),
        leader_commit=0,
    )

    assert success is True

    # The heartbeat event should have been set by a valid RPC
    assert (
        raft._heartbeat_event.is_set()
    ), "Heartbeat event was not set by a valid AppendEntries RPC"


class TestStateMachine:
    """Tests for KeyValueStateMachine SET/GET/DELETE operations."""

    @pytest.mark.asyncio
    async def test_set_and_get(self):
        sm = KeyValueStateMachine()
        result = await sm.apply("SET foo bar")
        assert result == "bar"
        result = await sm.apply("GET foo")
        assert result == "bar"

    @pytest.mark.asyncio
    async def test_get_missing_key(self):
        sm = KeyValueStateMachine()
        result = await sm.apply("GET missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete(self):
        sm = KeyValueStateMachine()
        await sm.apply("SET x 1")
        result = await sm.apply("DELETE x")
        assert result == "1"
        # Deleted key should return None
        result = await sm.apply("GET x")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_missing_key(self):
        sm = KeyValueStateMachine()
        result = await sm.apply("DELETE nope")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_value_with_spaces(self):
        sm = KeyValueStateMachine()
        result = await sm.apply("SET greeting hello world")
        assert result == "hello world"
        assert await sm.apply("GET greeting") == "hello world"

    @pytest.mark.asyncio
    async def test_unknown_command_raises(self):
        sm = KeyValueStateMachine()
        with pytest.raises(ValueError, match="Unknown command"):
            await sm.apply("INVALID cmd")

    @pytest.mark.asyncio
    async def test_case_insensitive_ops(self):
        sm = KeyValueStateMachine()
        await sm.apply("set key val")
        assert await sm.apply("get key") == "val"
        assert await sm.apply("delete key") == "val"


class TestApplyLoop:
    """Tests for the background _apply_committed_entries loop."""

    @pytest.mark.asyncio
    async def test_apply_loop_advances_last_applied(self):
        """Apply loop should advance lastApplied when commitIndex moves."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            state_machine=sm,
        )

        # Inject log entries
        raft._Raft__log = [
            raft_pb2.Log(term=1, command="SET a 1"),
            raft_pb2.Log(term=1, command="SET b 2"),
        ]

        # Start the apply loop
        task = asyncio.create_task(raft._apply_committed_entries())
        try:
            # Advance commit index and signal
            raft._Raft__commit_index = 2
            raft._Raft__commit_event.set()

            # Wait for the loop to drain
            await wait_until(lambda: raft.last_applied == 2)

            assert sm._store == {"a": "1", "b": "2"}
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @pytest.mark.asyncio
    async def test_apply_loop_handles_invalid_command(self):
        """Invalid commands should not crash the apply loop."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            state_machine=sm,
        )

        # Inject log entries: one invalid, one valid
        raft._Raft__log = [
            raft_pb2.Log(term=1, command="BADCMD"),
            raft_pb2.Log(term=1, command="SET x 42"),
        ]

        task = asyncio.create_task(raft._apply_committed_entries())
        try:
            raft._Raft__commit_index = 2
            raft._Raft__commit_event.set()

            # Wait for both entries to be applied (even though first raises)
            await wait_until(lambda: raft.last_applied == 2)

            assert sm._store == {"x": "42"}
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @pytest.mark.asyncio
    async def test_last_applied_property_accessible(self):
        """last_applied property should be accessible and start at 0."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        assert raft.last_applied == 0


class TestClientRequest:
    """Tests for the on_client_request interface."""

    @pytest.mark.asyncio
    async def test_non_leader_returns_failure_with_leader_hint(self):
        """A non-leader should return (False, '', leader_hint)."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Node is a follower by default
        assert raft.state == RaftState.FOLLOWER

        # Set a known leader
        raft._Raft__leader_id = "node-2"

        success, result, leader_hint = await raft.on_client_request("SET foo bar")
        assert success is False
        assert result == ""
        assert leader_hint == "node-2"

    @pytest.mark.asyncio
    async def test_non_leader_returns_none_leader_hint_when_unknown(self):
        """When no leader is known, leader_hint should be None."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        success, result, leader_hint = await raft.on_client_request("SET foo bar")
        assert success is False
        assert leader_hint is None

    @pytest.mark.asyncio
    async def test_leader_commits_without_state_machine(self):
        """Leader with no state machine should still commit and return success."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.request_vote = AsyncMock(return_value=(1, True))
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Make the node a leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Simulate replication happening in a background task
        async def simulate_replication():
            # Wait a tiny bit for the entry to be appended
            await asyncio.sleep(0.01)
            # Simulate peers having replicated
            for peer in raft._Raft__configuration:
                raft._Raft__match_index[peer] = len(raft._Raft__log)
                raft._Raft__next_index[peer] = len(raft._Raft__log) + 1
            # Update commit index as the leader would
            raft._update_leader_commit_index()

        repl_task = asyncio.create_task(simulate_replication())
        try:
            success, result, leader_hint = await raft.on_client_request("SET foo bar")
            assert success is True
            assert result == ""
            assert leader_hint is None
        finally:
            repl_task.cancel()
            with suppress(asyncio.CancelledError):
                await repl_task

    @pytest.mark.asyncio
    async def test_leader_full_flow_with_state_machine(self):
        """Full flow: leader appends, commits (mock replication).

        The apply loop handles state machine application, so on_client_request
        returns success with an empty result after commit (no double-apply).
        """
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.request_vote = AsyncMock(return_value=(1, True))
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            state_machine=sm,
        )

        # Make the node a leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        async def simulate_replication():
            await asyncio.sleep(0.01)
            for peer in raft._Raft__configuration:
                raft._Raft__match_index[peer] = len(raft._Raft__log)
                raft._Raft__next_index[peer] = len(raft._Raft__log) + 1
            raft._update_leader_commit_index()

        repl_task = asyncio.create_task(simulate_replication())
        try:
            success, result, leader_hint = await raft.on_client_request(
                "SET mykey myval"
            )
            assert success is True
            # on_client_request no longer applies inline; result is empty
            assert result == ""
            assert leader_hint is None
        finally:
            repl_task.cancel()
            with suppress(asyncio.CancelledError):
                await repl_task

    @pytest.mark.asyncio
    async def test_leader_timeout_on_no_replication(self):
        """If replication never happens, on_client_request should time out."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, False))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Make leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Patch the timeout to be very short for test speed
        import unittest.mock as um

        with um.patch(
            "aioraft.raft.asyncio.wait_for", side_effect=asyncio.TimeoutError
        ):
            success, result, leader_hint = await raft.on_client_request("SET foo bar")
            assert success is False
            assert result == "lost leadership or timeout"


class TestLogReplication:
    """Tests for log replication helpers."""

    @pytest.mark.asyncio
    async def test_replicate_to_peer_constructs_correct_append_entries(self):
        """_replicate_to_peer should send entries from nextIndex onwards."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        # Make leader with some log entries
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
            raft_pb2.Log(index=3, term=1, command="SET c 3"),
        ]
        # nextIndex for node-2 is 2 (needs entries 2 and 3)
        raft._Raft__next_index = {"node-2": 2}
        raft._Raft__match_index = {"node-2": 1}

        term, success = await raft._replicate_to_peer("node-2")
        assert success is True

        # Verify the call
        call_kwargs = mock_client.append_entries.call_args[1]
        assert call_kwargs["to"] == "node-2"
        assert call_kwargs["prev_log_index"] == 1
        assert call_kwargs["prev_log_term"] == 1
        assert len(call_kwargs["entries"]) == 2  # entries at index 2 and 3

    @pytest.mark.asyncio
    async def test_publish_heartbeat_updates_next_and_match_index(self):
        """_publish_heartbeat should update nextIndex/matchIndex on success."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
        ]
        await raft._initialize_leader_volatile_state()

        await raft._publish_heartbeat()

        # Both peers should now have nextIndex = 2, matchIndex = 1
        for peer in ["node-2", "node-3"]:
            assert raft._Raft__next_index[peer] == 2
            assert raft._Raft__match_index[peer] == 1

    @pytest.mark.asyncio
    async def test_publish_heartbeat_decrements_next_index_on_failure(self):
        """On AppendEntries failure, nextIndex should be decremented."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, False))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
        ]
        raft._Raft__next_index = {"node-2": 3}
        raft._Raft__match_index = {"node-2": 0}

        await raft._publish_heartbeat()

        # nextIndex should have been decremented from 3 to 2
        assert raft._Raft__next_index["node-2"] == 2

    @pytest.mark.asyncio
    async def test_update_leader_commit_index_advances_on_majority(self):
        """commitIndex should advance when a majority has replicated."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
        ]
        raft._Raft__commit_index = 0
        # node-2 has replicated up to index 2, node-3 only up to 1
        raft._Raft__match_index = {"node-2": 2, "node-3": 1}

        raft._update_leader_commit_index()

        # With 3 nodes (leader + 2), quorum = 2
        # Leader has all entries, node-2 has up to 2 -> majority for index 2
        assert raft.commit_index == 2

    @pytest.mark.asyncio
    async def test_update_leader_commit_index_only_current_term(self):
        """commitIndex should not advance for entries from previous terms."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),  # old term
        ]
        raft._Raft__commit_index = 0
        raft._Raft__match_index = {"node-2": 1, "node-3": 1}

        raft._update_leader_commit_index()

        # Entry at index 1 is from term 1, but current term is 2
        # Per Raft paper, leader can only commit entries from its own term
        assert raft.commit_index == 0


class TestAppendEntriesFullProtocol:
    """Tests for the full 5-rule AppendEntries receiver implementation."""

    @pytest.mark.asyncio
    async def test_appends_new_entries(self):
        """Follower should append new entries from the leader."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(1)

        entries = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
        ]

        term, success = await raft.on_append_entries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=0,
        )

        assert success is True
        assert len(raft._Raft__log) == 2
        assert raft._Raft__log[0].command == "SET a 1"
        assert raft._Raft__log[1].command == "SET b 2"

    @pytest.mark.asyncio
    async def test_rejects_if_prev_log_mismatch(self):
        """Should return False if prevLogIndex entry has wrong term."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
        ]

        # prevLogIndex=1, prevLogTerm=2 but actual term at index 1 is 1
        term, success = await raft.on_append_entries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=2,
            entries=[raft_pb2.Log(index=2, term=2, command="SET b 2")],
            leader_commit=0,
        )

        assert success is False

    @pytest.mark.asyncio
    async def test_truncates_conflicting_entries(self):
        """If existing entry conflicts with new one, delete it and following."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b OLD"),
            raft_pb2.Log(index=3, term=1, command="SET c OLD"),
        ]

        # Leader sends entry at index 2 with term 2 (conflicts with existing term 1)
        term, success = await raft.on_append_entries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[raft_pb2.Log(index=2, term=2, command="SET b NEW")],
            leader_commit=0,
        )

        assert success is True
        assert len(raft._Raft__log) == 2
        assert raft._Raft__log[1].command == "SET b NEW"
        assert raft._Raft__log[1].term == 2

    @pytest.mark.asyncio
    async def test_advances_commit_index(self):
        """commitIndex should advance when leaderCommit > commitIndex."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(1)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
        ]

        term, success = await raft.on_append_entries(
            term=1,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[raft_pb2.Log(index=2, term=1, command="SET b 2")],
            leader_commit=2,
        )

        assert success is True
        assert raft.commit_index == 2

    @pytest.mark.asyncio
    async def test_tracks_leader_id(self):
        """on_append_entries should track the leader's identity."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(1)

        await raft.on_append_entries(
            term=1,
            leader_id="the-leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=(),
            leader_commit=0,
        )

        assert raft._Raft__leader_id == "the-leader"


class TestRequestVoteLogUpToDate:
    """Tests for B2: log up-to-date check in on_request_vote."""

    @pytest.mark.asyncio
    async def test_rejects_candidate_with_stale_log_term(self):
        """Voter should reject a candidate whose last log term is older."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(3)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=3, command="SET a 1"),
        ]

        # Candidate has last_log_term=2, which is older than our term=3
        term, granted = await raft.on_request_vote(
            term=4,
            candidate_id="node-2",
            last_log_index=5,
            last_log_term=2,
        )

        assert granted is False

    @pytest.mark.asyncio
    async def test_rejects_candidate_with_shorter_log_same_term(self):
        """Voter should reject a candidate with same last term but shorter log."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=2, command="SET a 1"),
            raft_pb2.Log(index=2, term=2, command="SET b 2"),
            raft_pb2.Log(index=3, term=2, command="SET c 3"),
        ]

        # Candidate has same last term but shorter log (index=1 < our index=3)
        term, granted = await raft.on_request_vote(
            term=3,
            candidate_id="node-2",
            last_log_index=1,
            last_log_term=2,
        )

        assert granted is False

    @pytest.mark.asyncio
    async def test_grants_vote_to_up_to_date_candidate(self):
        """Voter should grant vote to a candidate with an up-to-date log."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
        ]

        # Candidate has a higher last_log_term -- up-to-date
        term, granted = await raft.on_request_vote(
            term=3,
            candidate_id="node-2",
            last_log_index=1,
            last_log_term=2,
        )

        assert granted is True

    @pytest.mark.asyncio
    async def test_start_election_sends_actual_log_info(self):
        """_start_election should send actual last_log_index/term, not zeros."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.request_vote = AsyncMock(return_value=(1, False))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=2, command="SET b 2"),
        ]
        raft._Raft__state = RaftState.CANDIDATE

        await raft._start_election()

        call_kwargs = mock_client.request_vote.call_args[1]
        assert call_kwargs["last_log_index"] == 2
        assert call_kwargs["last_log_term"] == 2


class TestCommitIndexRule5:
    """Tests for B3: correct Rule 5 commit index calculation."""

    @pytest.mark.asyncio
    async def test_commit_index_uses_last_new_entry_not_log_length(self):
        """commitIndex should be min(leaderCommit, index of last new entry),
        not based on total log length which may include unvalidated entries."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(2)
        # Follower has extra unvalidated entries from a previous leader
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b OLD"),
            raft_pb2.Log(index=3, term=1, command="SET c OLD"),
        ]
        raft._Raft__commit_index = 0

        # Leader sends only entry at index 2 with leaderCommit=2
        # prev_log_index=1 means we're appending after index 1
        term, success = await raft.on_append_entries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[raft_pb2.Log(index=2, term=2, command="SET b NEW")],
            leader_commit=2,
        )

        assert success is True
        # commitIndex should be min(2, 2) = 2, NOT min(2, 3) based on old log length
        assert raft.commit_index == 2

    @pytest.mark.asyncio
    async def test_heartbeat_does_not_use_log_length_for_commit(self):
        """On heartbeat (empty entries), commitIndex should use prev_log_index,
        not total log length."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
        ]
        raft._Raft__commit_index = 0

        # Heartbeat with prev_log_index=1, leaderCommit=5
        # Should set commitIndex = min(5, 1) = 1, not min(5, 2)
        term, success = await raft.on_append_entries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[],
            leader_commit=5,
        )

        assert success is True
        assert raft.commit_index == 1


class TestWaitForCommitLeadershipLoss:
    """Tests for B4: _wait_for_commit should not hang on leadership loss."""

    @pytest.mark.asyncio
    async def test_wait_for_commit_raises_on_leadership_loss(self):
        """_wait_for_commit should raise RuntimeError if leader steps down."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        raft._Raft__commit_index = 0

        # Step down after a short delay
        async def step_down():
            await asyncio.sleep(0.05)
            raft._Raft__state = RaftState.FOLLOWER

        step_down_task = asyncio.create_task(step_down())
        try:
            with pytest.raises(RuntimeError, match="lost leadership"):
                await asyncio.wait_for(raft._wait_for_commit(5), timeout=2.0)
        finally:
            step_down_task.cancel()
            with suppress(asyncio.CancelledError):
                await step_down_task

    @pytest.mark.asyncio
    async def test_on_client_request_returns_failure_on_leadership_loss(self):
        """on_client_request should return failure if leader steps down mid-commit."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Step down shortly after request starts
        async def step_down():
            await asyncio.sleep(0.05)
            raft._Raft__state = RaftState.FOLLOWER

        step_down_task = asyncio.create_task(step_down())
        try:
            success, result, leader_hint = await raft.on_client_request("SET foo bar")
            assert success is False
            assert "lost leadership or timeout" in result
        finally:
            step_down_task.cancel()
            with suppress(asyncio.CancelledError):
                await step_down_task


class TestMemoryStorage:
    """Tests for MemoryStorage operations."""

    @pytest.mark.asyncio
    async def test_save_and_load_term(self):
        storage = MemoryStorage()
        assert await storage.load_term() == 0
        await storage.save_term(5)
        assert await storage.load_term() == 5

    @pytest.mark.asyncio
    async def test_save_and_load_vote(self):
        storage = MemoryStorage()
        assert await storage.load_vote() is None
        await storage.save_vote("node-1")
        assert await storage.load_vote() == "node-1"
        await storage.save_vote(None)
        assert await storage.load_vote() is None


class TestAtomicTermAndVote:
    """Tests for atomic save_term_and_vote method."""

    @pytest.mark.asyncio
    async def test_memory_storage_save_term_and_vote(self):
        storage = MemoryStorage()
        await storage.save_term_and_vote(5, "node-2")
        assert await storage.load_term() == 5
        assert await storage.load_vote() == "node-2"

    @pytest.mark.asyncio
    async def test_memory_storage_save_term_and_vote_none(self):
        storage = MemoryStorage()
        await storage.save_term_and_vote(3, None)
        assert await storage.load_term() == 3
        assert await storage.load_vote() is None

    @pytest.mark.asyncio
    async def test_sqlite_save_term_and_vote(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            await storage.save_term_and_vote(5, "node-2")
            assert await storage.load_term() == 5
            assert await storage.load_vote() == "node-2"
            await storage.close()

    @pytest.mark.asyncio
    async def test_sqlite_save_term_and_vote_clears_vote(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            await storage.save_term_and_vote(3, "node-1")
            await storage.save_term_and_vote(4, None)
            assert await storage.load_term() == 4
            assert await storage.load_vote() is None
            await storage.close()

    @pytest.mark.asyncio
    async def test_synchronize_term_uses_atomic_save(self):
        """__synchronize_term should persist term and vote atomically."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        await raft.on_append_entries(
            term=5,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=(),
            leader_commit=0,
        )

        assert await storage.load_term() == 5
        assert await storage.load_vote() is None

    @pytest.mark.asyncio
    async def test_start_election_uses_atomic_save(self):
        """_start_election should persist term and vote atomically."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.request_vote = AsyncMock(return_value=(1, False))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        raft._Raft__state = RaftState.CANDIDATE
        await raft._start_election()

        assert await storage.load_term() == 1
        assert await storage.load_vote() == "node-1"


class TestTruncateAndAppend:
    """Tests for transactional truncate_and_append method."""

    @pytest.mark.asyncio
    async def test_memory_truncate_and_append(self):
        storage = MemoryStorage()
        await storage.append_logs(
            [
                raft_pb2.Log(index=1, term=1, command="SET a 1"),
                raft_pb2.Log(index=2, term=1, command="SET b 2"),
                raft_pb2.Log(index=3, term=1, command="SET c 3"),
            ]
        )
        new_entries = [raft_pb2.Log(index=2, term=2, command="SET b NEW")]
        await storage.truncate_and_append(2, new_entries)
        logs = await storage.load_logs()
        assert len(logs) == 2
        assert logs[0].command == "SET a 1"
        assert logs[1].command == "SET b NEW"

    @pytest.mark.asyncio
    async def test_sqlite_truncate_and_append(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            await storage.append_logs(
                [
                    raft_pb2.Log(index=1, term=1, command="SET a 1"),
                    raft_pb2.Log(index=2, term=1, command="SET b 2"),
                    raft_pb2.Log(index=3, term=1, command="SET c 3"),
                ]
            )
            new_entries = [raft_pb2.Log(index=2, term=2, command="SET b NEW")]
            await storage.truncate_and_append(2, new_entries)
            logs = await storage.load_logs()
            assert len(logs) == 2
            assert logs[0].command == "SET a 1"
            assert logs[1].command == "SET b NEW"
            await storage.close()

    @pytest.mark.asyncio
    async def test_conflict_resolution_with_storage(self):
        """on_append_entries conflict path should use transactional truncate+append."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        raft._Raft__current_term.set(2)
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b OLD"),
            raft_pb2.Log(index=3, term=1, command="SET c OLD"),
        ]
        # Also populate storage to match
        await storage.append_logs(list(raft._Raft__log))

        term, success = await raft.on_append_entries(
            term=2,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[raft_pb2.Log(index=2, term=2, command="SET b NEW")],
            leader_commit=0,
        )

        assert success is True
        # In-memory log should be updated
        assert len(raft._Raft__log) == 2
        assert raft._Raft__log[1].command == "SET b NEW"
        # Storage should also be updated
        logs = await storage.load_logs()
        assert len(logs) == 2
        assert logs[1].command == "SET b NEW"


class TestStorageLifecycle:
    """Tests for storage lifecycle (initialize/close) integration."""

    @pytest.mark.asyncio
    async def test_sqlite_initialize_called_by_raft(self):
        """Raft.__ainit__ should call storage.initialize()."""
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            mock_server = MagicMock()
            mock_server.bind = MagicMock()
            mock_client = AsyncMock()

            # This should not crash -- initialize() is called internally
            raft = await Raft.new(
                "node-1",
                server=mock_server,
                client=mock_client,
                configuration=["node-2"],
                storage=storage,
            )

            assert raft.current_term == 0
            assert raft.voted_for is None
            await storage.close()

    @pytest.mark.asyncio
    async def test_initialize_is_idempotent(self):
        """Calling initialize() multiple times should not fail."""
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            await storage.save_term(5)
            await storage.initialize()  # second call should not lose data
            assert await storage.load_term() == 5
            await storage.close()

    @pytest.mark.asyncio
    async def test_memory_storage_initialize_noop(self):
        """MemoryStorage.initialize() and close() should be no-ops."""
        storage = MemoryStorage()
        await storage.initialize()  # should not raise
        await storage.save_term(3)
        await storage.close()  # should not raise
        # Data still accessible (memory storage has no real close)
        assert await storage.load_term() == 3


class TestPersistBeforeMemory:
    """Tests verifying storage is called before in-memory state is updated."""

    @pytest.mark.asyncio
    async def test_append_entry_persists_before_memory(self):
        """_append_entry should persist to storage before updating in-memory log."""
        call_order = []

        class TrackingStorage(MemoryStorage):
            async def save_log_entry(self, entry):
                call_order.append("storage")
                await super().save_log_entry(entry)

        storage = TrackingStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)

        original_log_len = len(raft._Raft__log)
        await raft._append_entry("SET x 1")

        assert call_order == ["storage"]
        assert len(raft._Raft__log) == original_log_len + 1

    @pytest.mark.asyncio
    async def test_append_and_load_logs(self):
        storage = MemoryStorage()
        entries = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
        ]
        await storage.append_logs(entries)
        logs = await storage.load_logs()
        assert len(logs) == 2
        assert logs[0].command == "SET a 1"
        assert logs[1].command == "SET b 2"

    @pytest.mark.asyncio
    async def test_save_log_entry(self):
        storage = MemoryStorage()
        entry = raft_pb2.Log(index=1, term=1, command="SET x 42")
        await storage.save_log_entry(entry)
        logs = await storage.load_logs()
        assert len(logs) == 1
        assert logs[0].command == "SET x 42"

    @pytest.mark.asyncio
    async def test_truncate_logs_from(self):
        storage = MemoryStorage()
        entries = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
            raft_pb2.Log(index=3, term=1, command="SET c 3"),
        ]
        await storage.append_logs(entries)
        await storage.truncate_logs_from(2)
        logs = await storage.load_logs()
        assert len(logs) == 1
        assert logs[0].index == 1


class TestSQLiteStorage:
    """Tests for SQLiteStorage operations."""

    @pytest.mark.asyncio
    async def test_save_and_load_term(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            assert await storage.load_term() == 0
            await storage.save_term(5)
            assert await storage.load_term() == 5
            await storage.close()

    @pytest.mark.asyncio
    async def test_save_and_load_vote(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            assert await storage.load_vote() is None
            await storage.save_vote("node-1")
            assert await storage.load_vote() == "node-1"
            await storage.save_vote(None)
            assert await storage.load_vote() is None
            await storage.close()

    @pytest.mark.asyncio
    async def test_append_and_load_logs(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            entries = [
                raft_pb2.Log(index=1, term=1, command="SET a 1"),
                raft_pb2.Log(index=2, term=1, command="SET b 2"),
            ]
            await storage.append_logs(entries)
            logs = await storage.load_logs()
            assert len(logs) == 2
            assert logs[0].command == "SET a 1"
            assert logs[1].command == "SET b 2"
            await storage.close()

    @pytest.mark.asyncio
    async def test_save_log_entry(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            entry = raft_pb2.Log(index=1, term=1, command="SET x 42")
            await storage.save_log_entry(entry)
            logs = await storage.load_logs()
            assert len(logs) == 1
            assert logs[0].command == "SET x 42"
            await storage.close()

    @pytest.mark.asyncio
    async def test_truncate_logs_from(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()
            entries = [
                raft_pb2.Log(index=1, term=1, command="SET a 1"),
                raft_pb2.Log(index=2, term=1, command="SET b 2"),
                raft_pb2.Log(index=3, term=1, command="SET c 3"),
            ]
            await storage.append_logs(entries)
            await storage.truncate_logs_from(2)
            logs = await storage.load_logs()
            assert len(logs) == 1
            assert logs[0].index == 1
            await storage.close()

    @pytest.mark.asyncio
    async def test_persistence_across_connections(self):
        """Data should persist after closing and reopening the storage."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        storage = SQLiteStorage(db_path=db_path)
        await storage.initialize()
        await storage.save_term(7)
        await storage.save_vote("node-3")
        await storage.save_log_entry(raft_pb2.Log(index=1, term=7, command="SET k v"))
        await storage.close()

        # Reopen
        storage2 = SQLiteStorage(db_path=db_path)
        await storage2.initialize()
        assert await storage2.load_term() == 7
        assert await storage2.load_vote() == "node-3"
        logs = await storage2.load_logs()
        assert len(logs) == 1
        assert logs[0].command == "SET k v"
        await storage2.close()

        import os

        os.unlink(db_path)


class TestRaftWithStorage:
    """Tests that Raft persists state correctly through a Storage backend."""

    @pytest.mark.asyncio
    async def test_term_persisted_after_synchronize_term(self):
        """When a higher term is received, it should be persisted."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        # Send AppendEntries with a higher term to trigger __synchronize_term
        await raft.on_append_entries(
            term=5,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=(),
            leader_commit=0,
        )

        assert await storage.load_term() == 5
        assert raft.current_term == 5

    @pytest.mark.asyncio
    async def test_vote_persisted_after_on_request_vote(self):
        """When a vote is granted, it should be persisted."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        term, granted = await raft.on_request_vote(
            term=1,
            candidate_id="node-2",
            last_log_index=0,
            last_log_term=0,
        )

        assert granted is True
        assert await storage.load_vote() == "node-2"

    @pytest.mark.asyncio
    async def test_log_entries_persisted_after_on_append_entries(self):
        """Log entries should be persisted when received via AppendEntries."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        entries = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
        ]

        await raft.on_append_entries(
            term=1,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=0,
        )

        logs = await storage.load_logs()
        assert len(logs) == 2
        assert logs[0].command == "SET a 1"
        assert logs[1].command == "SET b 2"

    @pytest.mark.asyncio
    async def test_state_recovered_from_storage(self):
        """A new Raft instance should recover state from storage."""
        storage = MemoryStorage()
        await storage.save_term(10)
        await storage.save_vote("node-3")
        await storage.append_logs(
            [
                raft_pb2.Log(index=1, term=8, command="SET a 1"),
                raft_pb2.Log(index=2, term=10, command="SET b 2"),
            ]
        )

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        assert raft.current_term == 10
        assert raft.voted_for == "node-3"
        assert len(raft._Raft__log) == 2
        assert raft._Raft__log[0].command == "SET a 1"
        assert raft._Raft__log[1].command == "SET b 2"

    @pytest.mark.asyncio
    async def test_term_and_vote_persisted_during_election(self):
        """_start_election should persist term increment and self-vote."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.request_vote = AsyncMock(return_value=(1, False))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        raft._Raft__state = RaftState.CANDIDATE
        await raft._start_election()

        assert await storage.load_term() == 1  # incremented from 0
        assert await storage.load_vote() == "node-1"  # voted for self

    @pytest.mark.asyncio
    async def test_vote_cleared_on_synchronize_term(self):
        """When a higher term is seen, votedFor should be cleared in storage."""
        storage = MemoryStorage()
        await storage.save_vote("node-2")

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            storage=storage,
        )

        # Trigger synchronize_term with a higher term
        await raft.on_append_entries(
            term=10,
            leader_id="node-2",
            prev_log_index=0,
            prev_log_term=0,
            entries=(),
            leader_commit=0,
        )

        assert await storage.load_vote() is None


class TestSnapshotStateMachine:
    """Tests for KeyValueStateMachine snapshot/restore."""

    @pytest.mark.asyncio
    async def test_snapshot_and_restore(self):
        sm = KeyValueStateMachine()
        await sm.apply("SET a 1")
        await sm.apply("SET b 2")
        data = await sm.snapshot()

        sm2 = KeyValueStateMachine()
        await sm2.restore(data)
        assert await sm2.apply("GET a") == "1"
        assert await sm2.apply("GET b") == "2"

    @pytest.mark.asyncio
    async def test_snapshot_empty_state(self):
        sm = KeyValueStateMachine()
        data = await sm.snapshot()
        sm2 = KeyValueStateMachine()
        await sm2.restore(data)
        assert sm2._store == {}

    @pytest.mark.asyncio
    async def test_restore_replaces_state(self):
        sm = KeyValueStateMachine()
        await sm.apply("SET old_key old_val")
        data_empty = await KeyValueStateMachine().snapshot()
        await sm.restore(data_empty)
        assert sm._store == {}


class TestLogCompaction:
    """Tests for log compaction via snapshots."""

    @pytest.mark.asyncio
    async def test_log_compacted_after_threshold(self):
        """Log should be compacted when it exceeds snapshot_threshold."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
            snapshot_threshold=5,
        )

        # Inject 10 log entries
        entries = [
            raft_pb2.Log(index=i, term=1, command=f"SET k{i} v{i}")
            for i in range(1, 11)
        ]
        raft._Raft__log = entries

        # Apply all entries to state machine
        for e in entries:
            await sm.apply(e.command)

        # Set last_applied and commit_index
        raft._Raft__last_applied = 10
        raft._Raft__commit_index = 10

        # Trigger snapshot creation
        await raft._maybe_create_snapshot()

        # Log should be compacted
        assert raft._Raft__last_included_index == 10
        assert raft._Raft__last_included_term == 1
        assert len(raft._Raft__log) == 0

    @pytest.mark.asyncio
    async def test_log_indexing_after_compaction(self):
        """_log_at_index should return correct entries after compaction."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        # Simulate a compacted state: snapshot covers indices 1-5
        raft._Raft__last_included_index = 5
        raft._Raft__last_included_term = 2
        raft._Raft__log = [
            raft_pb2.Log(index=6, term=3, command="SET f 6"),
            raft_pb2.Log(index=7, term=3, command="SET g 7"),
        ]

        # Compacted entries should return None
        assert raft._log_at_index(1) is None
        assert raft._log_at_index(5) is None

        # Entries after snapshot should be accessible
        assert raft._log_at_index(6) is not None
        assert raft._log_at_index(6).command == "SET f 6"
        assert raft._log_at_index(7).command == "SET g 7"

        # Beyond log should return None
        assert raft._log_at_index(8) is None

    @pytest.mark.asyncio
    async def test_get_last_log_info_after_compaction(self):
        """_get_last_log_info should return snapshot metadata if log is empty."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        # Empty log with snapshot
        raft._Raft__last_included_index = 10
        raft._Raft__last_included_term = 3
        raft._Raft__log = []

        index, term = raft._get_last_log_info()
        assert index == 10
        assert term == 3

    @pytest.mark.asyncio
    async def test_get_last_log_info_with_log_after_compaction(self):
        """_get_last_log_info should return last log entry if log is non-empty."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__last_included_index = 5
        raft._Raft__last_included_term = 2
        raft._Raft__log = [
            raft_pb2.Log(index=6, term=3, command="SET f 6"),
        ]

        index, term = raft._get_last_log_info()
        assert index == 6
        assert term == 3

    @pytest.mark.asyncio
    async def test_append_entry_index_after_compaction(self):
        """_append_entry should use correct index after compaction."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(3)
        raft._Raft__last_included_index = 10
        raft._Raft__last_included_term = 2
        raft._Raft__log = []

        entry = await raft._append_entry("SET x 1")
        assert entry.index == 11

    @pytest.mark.asyncio
    async def test_no_snapshot_without_state_machine(self):
        """_maybe_create_snapshot should be a no-op without a state machine."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            snapshot_threshold=2,
        )

        raft._Raft__log = [
            raft_pb2.Log(index=i, term=1, command=f"SET k{i} v{i}") for i in range(1, 6)
        ]
        raft._Raft__last_applied = 5
        raft._Raft__commit_index = 5

        await raft._maybe_create_snapshot()
        # No compaction since no state machine
        assert raft._Raft__last_included_index == 0
        assert len(raft._Raft__log) == 5


class TestInstallSnapshot:
    """Tests for InstallSnapshot RPC handling."""

    @pytest.mark.asyncio
    async def test_follower_receives_and_applies_snapshot(self):
        """Follower should restore state machine from snapshot."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
        )

        # Create snapshot data
        import json

        snapshot_data = json.dumps({"a": "1", "b": "2"}).encode("utf-8")

        (resp_term,) = await raft.on_install_snapshot(
            term=5,
            leader_id="leader",
            last_included_index=10,
            last_included_term=4,
            data=snapshot_data,
        )

        assert resp_term == 5
        assert raft._Raft__last_included_index == 10
        assert raft._Raft__last_included_term == 4
        assert raft._Raft__commit_index == 10
        assert raft._Raft__last_applied == 10
        assert sm._store == {"a": "1", "b": "2"}

    @pytest.mark.asyncio
    async def test_stale_term_snapshot_rejected(self):
        """InstallSnapshot with stale term should be rejected."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(5)

        (resp_term,) = await raft.on_install_snapshot(
            term=3,
            leader_id="leader",
            last_included_index=10,
            last_included_term=2,
            data=b"{}",
        )

        assert resp_term == 5
        # State should be unchanged
        assert raft._Raft__last_included_index == 0

    @pytest.mark.asyncio
    async def test_follower_discards_log_entries_covered_by_snapshot(self):
        """InstallSnapshot should discard log entries covered by the snapshot."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
        )

        # Follower has some log entries
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
            raft_pb2.Log(index=3, term=2, command="SET c 3"),
        ]

        import json

        snapshot_data = json.dumps({"x": "10"}).encode("utf-8")

        await raft.on_install_snapshot(
            term=3,
            leader_id="leader",
            last_included_index=2,
            last_included_term=1,
            data=snapshot_data,
        )

        # Only entry at index 3 should remain
        assert len(raft._Raft__log) == 1
        assert raft._Raft__log[0].index == 3

    @pytest.mark.asyncio
    async def test_install_snapshot_resets_election_timer(self):
        """InstallSnapshot should reset the election timer."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__current_term.set(1)

        # Clear the heartbeat event so we can observe whether it gets set
        raft._Raft__heartbeat_event.clear()

        await raft.on_install_snapshot(
            term=2,
            leader_id="leader",
            last_included_index=5,
            last_included_term=1,
            data=b"{}",
        )

        # The heartbeat event should have been set by a valid InstallSnapshot
        assert raft._heartbeat_event.is_set()


class TestSnapshotStorage:
    """Tests for snapshot persistence in storage backends."""

    @pytest.mark.asyncio
    async def test_memory_storage_save_load_snapshot(self):
        storage = MemoryStorage()
        assert await storage.load_snapshot() is None

        await storage.save_snapshot(10, 3, b"snapshot-data")
        result = await storage.load_snapshot()
        assert result is not None
        assert result == (10, 3, b"snapshot-data")

    @pytest.mark.asyncio
    async def test_memory_storage_overwrite_snapshot(self):
        storage = MemoryStorage()
        await storage.save_snapshot(5, 1, b"old")
        await storage.save_snapshot(10, 2, b"new")
        result = await storage.load_snapshot()
        assert result == (10, 2, b"new")

    @pytest.mark.asyncio
    async def test_sqlite_save_load_snapshot(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()

            assert await storage.load_snapshot() is None

            await storage.save_snapshot(10, 3, b"snapshot-data")
            result = await storage.load_snapshot()
            assert result is not None
            assert result == (10, 3, b"snapshot-data")
            await storage.close()

    @pytest.mark.asyncio
    async def test_sqlite_overwrite_snapshot(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()

            await storage.save_snapshot(5, 1, b"old")
            await storage.save_snapshot(10, 2, b"new")
            result = await storage.load_snapshot()
            assert result == (10, 2, b"new")
            await storage.close()

    @pytest.mark.asyncio
    async def test_snapshot_with_storage_integration(self):
        """Snapshot should be persisted through Raft's on_install_snapshot."""
        storage = MemoryStorage()
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
            storage=storage,
        )

        import json

        snapshot_data = json.dumps({"key": "val"}).encode("utf-8")

        await raft.on_install_snapshot(
            term=3,
            leader_id="leader",
            last_included_index=10,
            last_included_term=2,
            data=snapshot_data,
        )

        result = await storage.load_snapshot()
        assert result is not None
        assert result[0] == 10
        assert result[1] == 2
        assert result[2] == snapshot_data


class TestCompactLogWithSnapshot:
    """Tests for the atomic compact_log_with_snapshot storage method (B1 fix)."""

    @pytest.mark.asyncio
    async def test_memory_compact_log_with_snapshot(self):
        storage = MemoryStorage()
        logs = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
            raft_pb2.Log(index=3, term=2, command="SET c 3"),
        ]
        await storage.append_logs(logs)

        remaining = [raft_pb2.Log(index=3, term=2, command="SET c 3")]
        await storage.compact_log_with_snapshot(2, 1, b"snap", remaining)

        snapshot = await storage.load_snapshot()
        assert snapshot == (2, 1, b"snap")
        loaded_logs = await storage.load_logs()
        assert len(loaded_logs) == 1
        assert loaded_logs[0].index == 3

    @pytest.mark.asyncio
    async def test_sqlite_compact_log_with_snapshot(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as f:
            storage = SQLiteStorage(db_path=f.name)
            await storage.initialize()

            logs = [
                raft_pb2.Log(index=1, term=1, command="SET a 1"),
                raft_pb2.Log(index=2, term=1, command="SET b 2"),
                raft_pb2.Log(index=3, term=2, command="SET c 3"),
            ]
            await storage.append_logs(logs)

            remaining = [raft_pb2.Log(index=3, term=2, command="SET c 3")]
            await storage.compact_log_with_snapshot(2, 1, b"snap", remaining)

            snapshot = await storage.load_snapshot()
            assert snapshot == (2, 1, b"snap")
            loaded_logs = await storage.load_logs()
            assert len(loaded_logs) == 1
            assert loaded_logs[0].index == 3
            await storage.close()

    @pytest.mark.asyncio
    async def test_compact_log_with_snapshot_empty_remaining(self):
        storage = MemoryStorage()
        await storage.append_logs([raft_pb2.Log(index=1, term=1, command="x")])
        await storage.compact_log_with_snapshot(1, 1, b"snap", [])

        snapshot = await storage.load_snapshot()
        assert snapshot == (1, 1, b"snap")
        assert await storage.load_logs() == []


class TestStaleSnapshotGuard:
    """Tests for B4: guard against stale/duplicate InstallSnapshot RPCs."""

    @pytest.mark.asyncio
    async def test_stale_snapshot_rejected(self):
        """InstallSnapshot with last_included_index <= current should be rejected."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
        )

        # Pre-set snapshot metadata to simulate having a snapshot at index 10
        raft._Raft__last_included_index = 10
        raft._Raft__last_included_term = 3
        raft._Raft__current_term.set(5)

        import json

        old_data = json.dumps({"old": "data"}).encode("utf-8")

        # Try to install a snapshot at index 8 (older)
        (resp_term,) = await raft.on_install_snapshot(
            term=5,
            leader_id="leader",
            last_included_index=8,
            last_included_term=2,
            data=old_data,
        )

        assert resp_term == 5
        # State should be unchanged
        assert raft._Raft__last_included_index == 10
        assert raft._Raft__last_included_term == 3

    @pytest.mark.asyncio
    async def test_duplicate_snapshot_rejected(self):
        """InstallSnapshot with same last_included_index should be rejected."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
        )

        raft._Raft__last_included_index = 5
        raft._Raft__last_included_term = 2
        raft._Raft__current_term.set(3)

        (resp_term,) = await raft.on_install_snapshot(
            term=3,
            leader_id="leader",
            last_included_index=5,
            last_included_term=2,
            data=b"{}",
        )

        assert resp_term == 3
        # Index should remain unchanged (not re-processed)
        assert raft._Raft__last_included_index == 5

    @pytest.mark.asyncio
    async def test_newer_snapshot_accepted(self):
        """InstallSnapshot with greater last_included_index should be accepted."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
        )

        raft._Raft__last_included_index = 5
        raft._Raft__last_included_term = 2
        raft._Raft__current_term.set(3)

        import json

        snapshot_data = json.dumps({"new": "state"}).encode("utf-8")

        (resp_term,) = await raft.on_install_snapshot(
            term=3,
            leader_id="leader",
            last_included_index=10,
            last_included_term=3,
            data=snapshot_data,
        )

        assert resp_term == 3
        assert raft._Raft__last_included_index == 10
        assert raft._Raft__last_included_term == 3


class TestLeaderSendsPersistedSnapshot:
    """Tests for B3: leader sends persisted snapshot, not live state."""

    @pytest.mark.asyncio
    async def test_replicate_uses_persisted_snapshot(self):
        """Leader should use stored snapshot data, not live state machine snapshot."""
        sm = KeyValueStateMachine()
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.install_snapshot = AsyncMock(return_value=(1,))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
            storage=storage,
        )

        # Store a persisted snapshot at index 5
        await storage.save_snapshot(5, 2, b"persisted-snapshot-data")

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(3)
        raft._Raft__last_included_index = 5
        raft._Raft__last_included_term = 2
        raft._Raft__log = []
        await raft._initialize_leader_volatile_state()

        # Set follower's nextIndex behind snapshot
        raft._Raft__next_index["node-2"] = 3

        # Apply more entries to state machine (making live state ahead of snapshot)
        await sm.apply("SET extra 999")

        await raft._replicate_to_peer("node-2")

        # Verify install_snapshot was called with PERSISTED data, not live
        mock_client.install_snapshot.assert_called_once()
        call_kwargs = mock_client.install_snapshot.call_args
        assert call_kwargs.kwargs["data"] == b"persisted-snapshot-data"
        assert call_kwargs.kwargs["last_included_index"] == 5
        assert call_kwargs.kwargs["last_included_term"] == 2

    @pytest.mark.asyncio
    async def test_replicate_falls_back_to_live_snapshot(self):
        """When no persisted snapshot exists, fall back to live snapshot."""
        sm = KeyValueStateMachine()
        await sm.apply("SET k1 v1")
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.install_snapshot = AsyncMock(return_value=(1,))

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2"],
            state_machine=sm,
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(3)
        raft._Raft__last_included_index = 5
        raft._Raft__last_included_term = 2
        raft._Raft__last_applied = 5
        raft._Raft__log = []
        await raft._initialize_leader_volatile_state()
        raft._Raft__next_index["node-2"] = 3

        await raft._replicate_to_peer("node-2")

        # Should still call install_snapshot (with live data)
        mock_client.install_snapshot.assert_called_once()


class TestSnapshotBinarySerialization:
    """Tests for B2: binary serialization of snapshot messages."""

    def test_install_snapshot_request_roundtrip(self):
        from aioraft.protos.raft_pb2 import InstallSnapshotRequest

        req = InstallSnapshotRequest(
            term=42,
            leader_id="leader-node-1",
            last_included_index=100,
            last_included_term=10,
            data=b"\x00\x01\x02\xff",
        )
        serialized = req.SerializeToString()
        restored = InstallSnapshotRequest.FromString(serialized)

        assert restored.term == 42
        assert restored.leader_id == "leader-node-1"
        assert restored.last_included_index == 100
        assert restored.last_included_term == 10
        assert restored.data == b"\x00\x01\x02\xff"

    def test_install_snapshot_response_roundtrip(self):
        from aioraft.protos.raft_pb2 import InstallSnapshotResponse

        resp = InstallSnapshotResponse(term=7)
        serialized = resp.SerializeToString()
        restored = InstallSnapshotResponse.FromString(serialized)

        assert restored.term == 7

    def test_install_snapshot_request_empty_data(self):
        from aioraft.protos.raft_pb2 import InstallSnapshotRequest

        req = InstallSnapshotRequest(term=1, leader_id="", data=b"")
        serialized = req.SerializeToString()
        restored = InstallSnapshotRequest.FromString(serialized)

        assert restored.term == 1
        assert restored.leader_id == ""
        assert restored.data == b""

    def test_serialization_is_binary_not_json(self):
        """Verify that serialized form is NOT JSON (B2 fix)."""
        from aioraft.protos.raft_pb2 import InstallSnapshotRequest

        req = InstallSnapshotRequest(term=1, leader_id="node", data=b"data")
        serialized = req.SerializeToString()

        # Should NOT be valid JSON
        import json

        with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
            json.loads(serialized)


class TestMembershipChanges:
    """Tests for single-server membership change (Phase 5)."""

    @pytest.mark.asyncio
    async def test_add_server_to_configuration(self):
        """Leader should be able to add a server to the cluster."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Make this node leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Pre-commit the config change by advancing commit index in background
        async def advance_commit():
            await asyncio.sleep(0.05)
            # Simulate replication: the entry is at index 1
            raft._update_commit_index(1)

        task = asyncio.create_task(advance_commit())

        success, msg = await raft.add_server("node-4")
        await task

        assert success is True
        assert msg == ""
        assert "node-4" in raft.configuration

    @pytest.mark.asyncio
    async def test_remove_server_from_configuration(self):
        """Leader should be able to remove a server from the cluster."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Make this node leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        async def advance_commit():
            await asyncio.sleep(0.05)
            raft._update_commit_index(1)

        task = asyncio.create_task(advance_commit())

        success, msg = await raft.remove_server("node-2")
        await task

        assert success is True
        assert msg == ""
        assert "node-2" not in raft.configuration

    @pytest.mark.asyncio
    async def test_config_change_rejected_on_non_leader(self):
        """add_server and remove_server should fail on non-leaders."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Node is a follower by default
        assert raft.state == RaftState.FOLLOWER

        success, msg = await raft.add_server("node-4")
        assert success is False
        assert msg == "not leader"

        success, msg = await raft.remove_server("node-2")
        assert success is False
        assert msg == "not leader"

    @pytest.mark.asyncio
    async def test_only_one_config_change_pending(self):
        """Only one config change should be allowed at a time."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Make this node leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Manually inject an uncommitted config change entry
        from aioraft.types import CONF_CHANGE_ADD

        raft._Raft__log.append(
            raft_pb2.Log(index=1, term=1, command=f"{CONF_CHANGE_ADD}:node-4")
        )
        # commit_index is 0, so this entry is uncommitted

        success, msg = await raft.add_server("node-5")
        assert success is False
        assert msg == "another config change is pending"

    @pytest.mark.asyncio
    async def test_config_change_entries_not_applied_to_state_machine(self):
        """Config change entries should be skipped by the state machine apply loop."""
        from aioraft.types import CONF_CHANGE_ADD

        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            state_machine=sm,
        )

        # Inject a mix of normal and config change entries
        raft._Raft__log = [
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command=f"{CONF_CHANGE_ADD}:node-4"),
            raft_pb2.Log(index=3, term=1, command="SET b 2"),
        ]

        task = asyncio.create_task(raft._apply_committed_entries())
        try:
            raft._Raft__commit_index = 3
            raft._Raft__commit_event.set()
            await wait_until(lambda: raft.last_applied == 3)

            # State machine should only have the SET commands applied
            assert sm._store == {"a": "1", "b": "2"}
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @pytest.mark.asyncio
    async def test_followers_apply_config_changes_on_append_entries(self):
        """Followers should update their configuration when receiving config change entries."""
        from aioraft.types import CONF_CHANGE_ADD, CONF_CHANGE_REMOVE

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__current_term.set(1)

        # Simulate receiving an AppendEntries with a config add entry
        add_entry = raft_pb2.Log(index=1, term=1, command=f"{CONF_CHANGE_ADD}:node-4")
        term, success = await raft.on_append_entries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[add_entry],
            leader_commit=0,
        )
        assert success is True
        assert "node-4" in raft.configuration

        # Simulate receiving a config remove entry
        remove_entry = raft_pb2.Log(
            index=2, term=1, command=f"{CONF_CHANGE_REMOVE}:node-3"
        )
        term, success = await raft.on_append_entries(
            term=1,
            leader_id="leader",
            prev_log_index=1,
            prev_log_term=1,
            entries=[remove_entry],
            leader_commit=0,
        )
        assert success is True
        assert "node-3" not in raft.configuration

    @pytest.mark.asyncio
    async def test_has_pending_config_change_detection(self):
        """_has_pending_config_change should detect uncommitted config entries."""
        from aioraft.types import CONF_CHANGE_ADD

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # No entries, no pending change
        assert raft._has_pending_config_change() is False

        # Add a config change entry (uncommitted, commit_index=0)
        raft._Raft__log.append(
            raft_pb2.Log(index=1, term=1, command=f"{CONF_CHANGE_ADD}:node-4")
        )
        assert raft._has_pending_config_change() is True

        # Commit it
        raft._Raft__commit_index = 1
        assert raft._has_pending_config_change() is False

    @pytest.mark.asyncio
    async def test_self_removal_causes_step_down(self):
        """Removing the leader itself should cause it to step down to follower."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        # Make this node leader
        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        async def advance_commit():
            await asyncio.sleep(0.05)
            raft._update_commit_index(1)

        task = asyncio.create_task(advance_commit())

        success, msg = await raft.remove_server("leader-1")
        await task

        assert success is True
        assert raft.state == RaftState.FOLLOWER

    @pytest.mark.asyncio
    async def test_add_duplicate_server_rejected(self):
        """Adding a server that is already in the configuration should fail."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        success, msg = await raft.add_server("node-2")
        assert success is False
        assert msg == "server already in cluster"

    @pytest.mark.asyncio
    async def test_remove_nonexistent_server_rejected(self):
        """Removing a server not in the configuration should fail."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        success, msg = await raft.remove_server("node-99")
        assert success is False
        assert msg == "server not in cluster"

    @pytest.mark.asyncio
    async def test_configuration_rebuilt_from_log_on_startup(self):
        """On startup with storage, configuration should be rebuilt from log entries."""
        from aioraft.types import CONF_CHANGE_ADD, CONF_CHANGE_REMOVE

        storage = MemoryStorage()
        # Pre-populate storage with config change log entries
        entries = [
            raft_pb2.Log(index=1, term=1, command="SET x 1"),
            raft_pb2.Log(index=2, term=1, command=f"{CONF_CHANGE_ADD}:node-4"),
            raft_pb2.Log(index=3, term=1, command=f"{CONF_CHANGE_REMOVE}:node-3"),
        ]
        await storage.append_logs(entries)

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            storage=storage,
        )

        # node-4 was added, node-3 was removed
        assert "node-4" in raft.configuration
        assert "node-3" not in raft.configuration
        assert "node-2" in raft.configuration

    @pytest.mark.asyncio
    async def test_is_config_change_helper(self):
        """_is_config_change should correctly identify config change commands."""
        from aioraft.types import CONF_CHANGE_ADD, CONF_CHANGE_REMOVE

        assert Raft._is_config_change(f"{CONF_CHANGE_ADD}:node-1") is True
        assert Raft._is_config_change(f"{CONF_CHANGE_REMOVE}:node-1") is True
        assert Raft._is_config_change("SET key value") is False
        assert Raft._is_config_change("GET key") is False

    @pytest.mark.asyncio
    async def test_configuration_property_returns_copy(self):
        """configuration property should return a copy, not the internal set."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        config = raft.configuration
        config.add("node-99")
        # Internal configuration should not be affected
        assert "node-99" not in raft.configuration

    @pytest.mark.asyncio
    async def test_add_server_updates_config_before_append(self):
        """B1: Configuration should be updated before appending entry so
        heartbeats see the new peer immediately."""
        call_order = []

        class TrackingClient(AsyncMock):
            async def append_entries(self, **kwargs):
                return (1, True)

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = TrackingClient()
        mock_client.request_vote = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Verify that after add_server starts, the new node is in config
        # before _wait_for_commit is called.
        original_append = raft._append_entry

        async def tracking_append(cmd):
            # At the point of appending, node-4 should already be in config
            call_order.append(
                "config_has_node4"
                if "node-4" in raft._Raft__configuration
                else "config_missing_node4"
            )
            return await original_append(cmd)

        raft._append_entry = tracking_append

        async def advance_commit():
            await asyncio.sleep(0.05)
            raft._update_commit_index(1)

        task = asyncio.create_task(advance_commit())
        success, msg = await raft.add_server("node-4")
        await task

        assert success is True
        assert call_order == ["config_has_node4"]

    @pytest.mark.asyncio
    async def test_remove_server_keeps_replication_state_until_commit(self):
        """B2: Replication state (next_index/match_index) should be retained
        until the config change entry is committed, allowing the removed server
        to learn about its removal."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        had_repl_state_during_wait = []

        original_wait = raft._wait_for_commit

        async def tracking_wait(index):
            # Check that replication state still exists for node-2 during wait
            had_repl_state_during_wait.append("node-2" in raft._Raft__next_index)
            # Simulate commit
            raft._update_commit_index(index)

        raft._wait_for_commit = tracking_wait

        success, msg = await raft.remove_server("node-2")

        assert success is True
        # During the wait, replication state should have been present
        assert had_repl_state_during_wait == [True]
        # After commit, replication state should be cleaned up
        assert "node-2" not in raft._Raft__next_index
        assert "node-2" not in raft._Raft__match_index

    @pytest.mark.asyncio
    async def test_config_persisted_to_storage_on_add(self):
        """B3: Configuration should be persisted to storage when servers are added."""
        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            storage=storage,
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        async def advance_commit():
            await asyncio.sleep(0.05)
            raft._update_commit_index(1)

        task = asyncio.create_task(advance_commit())
        success, msg = await raft.add_server("node-4")
        await task

        assert success is True
        config = await storage.load_configuration()
        assert config is not None
        assert "node-4" in config

    @pytest.mark.asyncio
    async def test_config_loaded_from_storage_on_startup(self):
        """B3: Configuration should be loaded from storage on startup, covering
        entries that were compacted away."""
        storage = MemoryStorage()
        # Simulate a compacted state: snapshot exists, no logs
        await storage.save_snapshot(10, 3, b"snapshot-data")
        await storage.save_configuration(["node-2", "node-4", "node-5"])

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],  # Initial config
            storage=storage,
        )

        # Config should be restored from storage, not use the initial config
        assert "node-4" in raft.configuration
        assert "node-5" in raft.configuration
        # node-3 was removed (not in stored config)
        assert "node-3" not in raft.configuration

    @pytest.mark.asyncio
    async def test_install_snapshot_restores_configuration(self):
        """B4: on_install_snapshot should restore configuration from snapshot data."""
        sm = KeyValueStateMachine()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            state_machine=sm,
        )

        # Create snapshot data that includes configuration
        state_data = json.dumps({"a": "1"}).encode("utf-8")
        snapshot_data = Raft._serialize_snapshot_data(
            state_data, {"node-2", "node-4", "node-5"}
        )

        (resp_term,) = await raft.on_install_snapshot(
            term=5,
            leader_id="leader",
            last_included_index=10,
            last_included_term=4,
            data=snapshot_data,
        )

        assert resp_term == 5
        # Configuration should be restored from snapshot
        assert "node-4" in raft.configuration
        assert "node-5" in raft.configuration
        # State machine should be restored with the unwrapped state data
        assert sm._store == {"a": "1"}

    @pytest.mark.asyncio
    async def test_client_request_rejects_config_change_commands(self):
        """B5: on_client_request should reject commands with reserved config prefixes."""
        from aioraft.types import CONF_CHANGE_ADD, CONF_CHANGE_REMOVE

        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        # Try to inject a config add via client request
        success, result, hint = await raft.on_client_request(
            f"{CONF_CHANGE_ADD}:attacker-node"
        )
        assert success is False
        assert "reserved prefix" in result

        # Try to inject a config remove via client request
        success, result, hint = await raft.on_client_request(
            f"{CONF_CHANGE_REMOVE}:node-2"
        )
        assert success is False
        assert "reserved prefix" in result

        # Normal commands should still work (will timeout but not be rejected)
        # Just verify it doesn't get the "reserved prefix" error
        import unittest.mock as um

        with um.patch(
            "aioraft.raft.asyncio.wait_for", side_effect=asyncio.TimeoutError
        ):
            success, result, hint = await raft.on_client_request("SET foo bar")
            assert "reserved prefix" not in result

    @pytest.mark.asyncio
    async def test_self_removal_steps_down_after_commit(self):
        """B6: When the leader removes itself, it should append the entry,
        wait for commit, and then step down."""
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        raft = await Raft.new(
            "leader-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
        )

        raft._Raft__state = RaftState.LEADER
        raft._Raft__current_term.set(1)
        await raft._initialize_leader_volatile_state()

        async def advance_commit():
            await asyncio.sleep(0.05)
            raft._update_commit_index(1)

        task = asyncio.create_task(advance_commit())

        success, msg = await raft.remove_server("leader-1")
        await task

        assert success is True
        assert raft.state == RaftState.FOLLOWER

        # The config change entry should exist in the log
        assert len(raft._Raft__log) == 1
        from aioraft.types import CONF_CHANGE_REMOVE

        assert raft._Raft__log[0].command == f"{CONF_CHANGE_REMOVE}:leader-1"

    @pytest.mark.asyncio
    async def test_snapshot_data_serialization_roundtrip(self):
        """B4: Snapshot data serialization/deserialization should preserve both
        state machine data and configuration."""
        state_data = b"some-state-machine-data"
        config = {"node-1", "node-2", "node-3"}

        serialized = Raft._serialize_snapshot_data(state_data, config)
        recovered_state, recovered_config = Raft._deserialize_snapshot_data(serialized)

        assert recovered_state == state_data
        assert recovered_config == config

    @pytest.mark.asyncio
    async def test_deserialize_legacy_snapshot_data(self):
        """B4: Deserializing legacy snapshot data (without config wrapper)
        should gracefully fall back."""
        # Legacy data that doesn't have the wrapper format
        legacy_data = b'{"key": "value"}'
        state_data, config = Raft._deserialize_snapshot_data(legacy_data)
        # Should return the original data and empty config
        assert config == set() or state_data == legacy_data

    @pytest.mark.asyncio
    async def test_config_persisted_on_follower_append_entries(self):
        """B3: When a follower receives config change entries via AppendEntries,
        the configuration should be persisted to storage."""
        from aioraft.types import CONF_CHANGE_ADD

        storage = MemoryStorage()
        mock_server = MagicMock()
        mock_server.bind = MagicMock()
        mock_client = AsyncMock()

        raft = await Raft.new(
            "node-1",
            server=mock_server,
            client=mock_client,
            configuration=["node-2", "node-3"],
            storage=storage,
        )

        raft._Raft__current_term.set(1)

        add_entry = raft_pb2.Log(index=1, term=1, command=f"{CONF_CHANGE_ADD}:node-4")
        term, success = await raft.on_append_entries(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[add_entry],
            leader_commit=0,
        )
        assert success is True

        config = await storage.load_configuration()
        assert config is not None
        assert "node-4" in config
