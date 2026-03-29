import asyncio
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

    # Simulate some elapsed time (as if election timer is counting down)
    raft._Raft__elapsed_time = 0.1

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

    # The election timer should NOT have been reset (elapsed_time stays 0.1)
    assert raft._elapsed_time == pytest.approx(0.1), (
        "Election timer was reset by a stale AppendEntries RPC; "
        "only valid RPCs should reset the timer"
    )


@pytest.mark.asyncio
async def test_valid_append_entries_resets_election_timer():
    """Verify that a valid AppendEntries RPC (with current term) does
    reset the election timer."""
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

    # Simulate some elapsed time
    raft._Raft__elapsed_time = 0.1

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

    # The election timer should have been reset (elapsed_time back to 0.0)
    assert raft._elapsed_time == pytest.approx(0.0), (
        "Election timer was not reset by a valid AppendEntries RPC"
    )


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
            success, result, leader_hint = await raft.on_client_request("SET mykey myval")
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
        with um.patch("aioraft.raft.asyncio.wait_for", side_effect=asyncio.TimeoutError):
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
        await storage.append_logs([
            raft_pb2.Log(index=1, term=1, command="SET a 1"),
            raft_pb2.Log(index=2, term=1, command="SET b 2"),
            raft_pb2.Log(index=3, term=1, command="SET c 3"),
        ])
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
            await storage.append_logs([
                raft_pb2.Log(index=1, term=1, command="SET a 1"),
                raft_pb2.Log(index=2, term=1, command="SET b 2"),
                raft_pb2.Log(index=3, term=1, command="SET c 3"),
            ])
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
        await storage.append_logs([
            raft_pb2.Log(index=1, term=8, command="SET a 1"),
            raft_pb2.Log(index=2, term=10, command="SET b 2"),
        ])

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
