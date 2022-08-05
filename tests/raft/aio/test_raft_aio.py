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
        await Raft.new(f"raft.aio-{i}", server, GrpcRaftClient(), filter(lambda x: x != addr, configurations))
        for i, (server, addr) in enumerate(zip(servers, configurations))
    ]
    assert all(map(lambda r: not r.has_leadership(), raft_nodes))

    raft_tasks = [asyncio.create_task(raft.main()) for raft in raft_nodes]
    done, pending = await asyncio.wait(
        {
            *[
                asyncio.create_task(server.run(host="0.0.0.0", port=port))
                for server, port in zip(servers, ports)
            ],
            *raft_tasks,
            asyncio.create_task(asyncio.sleep(1.0)),
        },
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert any(map(lambda r: r.has_leadership(), raft_nodes))

    for task in raft_tasks:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
