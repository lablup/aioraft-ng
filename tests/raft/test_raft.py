import time
from threading import Thread

from raft import Raft
from raft.peers import GrpcRaftPeer
from raft.server import GrpcRaftServer


def test_raft_leader_election():
    n = 5
    ports = tuple(range(50051, 50051 + n))
    configuration = [f"127.0.0.1:{port}" for port in ports]
    servers = [GrpcRaftServer() for _ in range(n)]
    raft_nodes = [
        Raft(
            f"raft-{i}",
            server=server,
            peer=GrpcRaftPeer(),
            configuration=filter(lambda x: x != addr, configuration),
        )
        for i, (server, addr) in enumerate(zip(servers, configuration))
    ]
    assert all(map(lambda r: not r.has_leadership(), raft_nodes))

    leadership_timeout = 0.0
    LEADERSHIP_CHECK_INTERVAL = 0.1
    LEADERSHIP_CHECK_MAX_TRIAL = 100

    def _wait_for_new_leadership():
        nonlocal leadership_timeout
        for _ in range(LEADERSHIP_CHECK_MAX_TRIAL):
            time.sleep(LEADERSHIP_CHECK_INTERVAL)
            leadership_timeout += LEADERSHIP_CHECK_INTERVAL
            if any(map(lambda r: r.has_leadership(), raft_nodes)):
                break

    raft_server_threads = [
        Thread(
            target=server.run,
            kwargs={"host": "0.0.0.0", "port": port},
            daemon=True,
        )
        for server, port in zip(servers, ports)
    ]
    for raft_server_thread in raft_server_threads:
        raft_server_thread.start()

    raft_main_threads = [Thread(target=raft.main, daemon=True) for raft in raft_nodes]
    for raft_main_thread in raft_main_threads:
        raft_main_thread.start()

    leadership_timeout_thread = Thread(target=_wait_for_new_leadership, daemon=True)
    leadership_timeout_thread.start()
    leadership_timeout_thread.join()

    assert any(map(lambda r: r.has_leadership(), raft_nodes))
    assert leadership_timeout <= LEADERSHIP_CHECK_INTERVAL * LEADERSHIP_CHECK_MAX_TRIAL
