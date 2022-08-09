import random
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
        Raft(
            f"raft-{i}",
            server,
            GrpcRaftClient(),
            filter(lambda x: x != addr, configurations),
        )
        for i, (server, addr) in enumerate(zip(servers, configurations))
    ]
    assert all(map(lambda r: not r.has_leadership(), raft_nodes))

    raft_server_threads = [
        Thread(
            target=server.run,
            kwargs={"host": "0.0.0.0", "port": port},
            daemon=True,
        )
        for raft, server, port in zip(raft_nodes, servers, ports)
    ]
    for raft_server_thread in raft_server_threads:
        raft_server_thread.start()

    random_node = random.choice(raft_nodes)
    raft_election_thread = Thread(target=random_node.start_election, daemon=True)
    raft_election_thread.start()
    raft_election_thread.join()
    assert random_node.has_leadership() is True
