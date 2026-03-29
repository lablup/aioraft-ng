import asyncio
import time
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import pytest

from aioraft import Raft
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer
from aioraft.types import RaftState


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
    assert raft.elapsed_time == pytest.approx(0.1), (
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
    assert raft.elapsed_time == pytest.approx(0.0), (
        "Election timer was not reset by a valid AppendEntries RPC"
    )
