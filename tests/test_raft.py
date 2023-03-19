import asyncio
import time
from contextlib import suppress

import pytest

from aioraft import Raft
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer


@pytest.mark.asyncio
async def test_raft_leader_election():
    n = 5
    ports = tuple(range(50051, 50051 + n))
    configuration = [f"127.0.0.1:{port}" for port in ports]
    servers = [GrpcRaftServer(host="0.0.0.0", port=port) for port in ports]
    raft_nodes = []
    for i, (server, addr) in enumerate(zip(servers, configuration)):
        config = tuple(filter(lambda x: x != addr, configuration))
        clients = [GrpcRaftClient(to=to) for to in config]
        raft_nodes.append(
            await Raft.new(
                f"raft.aio-{i}",
                server=server,
                clients=clients,
                configuration=config,
            )
        )
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
        asyncio.create_task(server.run(cleanup_coroutines=cleanup_coroutines))
        for server in servers
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
