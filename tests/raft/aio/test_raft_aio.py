import asyncio
from contextlib import suppress

import pytest

from raft.aio import Raft
from raft.aio.client import GrpcRaftClient
from raft.aio.server import GrpcRaftServer


@pytest.mark.asyncio
async def test_raft_aio_leader_election():
    n = 5
    ports = tuple(range(50051, 50051 + n))
    configurations = [f"127.0.0.1:{port}" for port in ports]
    servers = [GrpcRaftServer() for _ in range(n)]
    raft_nodes = [
        await Raft.new(
            f"raft.aio-{i}",
            server,
            GrpcRaftClient(),
            filter(lambda x: x != addr, configurations),
        )
        for i, (server, addr) in enumerate(zip(servers, configurations))
    ]
    assert all(map(lambda r: not r.has_leadership(), raft_nodes))

    leadership_timeout = 0.0
    LEADERSHIP_CHECK_INTERVAL = 0.1
    LEADERSHIP_CHECK_MAX_TRIAL = 100

    async def _wait_for_new_leadership():
        nonlocal leadership_timeout
        for _ in range(LEADERSHIP_CHECK_MAX_TRIAL):
            await asyncio.sleep(LEADERSHIP_CHECK_INTERVAL)
            leadership_timeout += LEADERSHIP_CHECK_INTERVAL
            if any(map(lambda r: r.has_leadership(), raft_nodes)):
                break

    raft_server_tasks = [
        asyncio.create_task(server.run(host="0.0.0.0", port=port))
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
    assert leadership_timeout <= LEADERSHIP_CHECK_INTERVAL * LEADERSHIP_CHECK_MAX_TRIAL

    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
