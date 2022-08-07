import time
from threading import Thread

from raft import Raft
from raft.client import GrpcRaftClient
from raft.server import GrpcRaftServer


def test_raft_leader_election():
    n = 5
    ports = tuple(range(50051, 50051 + n))
    configurations = [f"127.0.0.1:{port}" for port in ports]
    servers = [GrpcRaftServer() for _ in range(n)]
    raft_nodes = [
        Raft(f"raft-{i}", server, GrpcRaftClient(), filter(lambda x: x != addr, configurations))
        for i, (server, addr) in enumerate(zip(servers, configurations))
    ]
    assert all(map(lambda r: not r.has_leadership(), raft_nodes))

    server_threads, raft_threads = zip(*[
        (
            Thread(target=server.run, kwargs={"host": "0.0.0.0", "port": port}, daemon=True),
            Thread(target=raft.main, daemon=True),
        )
        for raft, server, port in zip(raft_nodes, servers, ports)
    ])
    for server_thread, raft_thread in zip(server_threads, raft_threads):
        server_thread.start()
        raft_thread.start()
    time.sleep(3.0)
    assert any(map(lambda r: r.has_leadership(), raft_nodes))
