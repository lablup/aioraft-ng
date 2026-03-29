import asyncio
import time
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import pytest

from aioraft import KeyValueStateMachine, Raft
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
