import argparse
from pathlib import Path
from threading import Thread
from typing import Coroutine, Final, List

import tomli

from raft import Raft
from raft.peers import AbstractRaftPeer, GrpcRaftPeer
from raft.server import GrpcRaftServer
from raft.utils import build_loopback_ip

_cleanup_coroutines: List[Coroutine] = []


def load_config():
    path = Path(__file__).parent / "config.toml"
    return tomli.loads(path.read_text())


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, default=50051)
    return parser.parse_args()


def main():
    args = parse_args()
    public_ip = build_loopback_ip()
    public_id = f"{public_ip}:{args.port}"

    config = load_config()
    configuration = tuple(
        server
        for server in config["raft"]["configuration"]
        if not server.endswith(str(args.port))
    )

    server = GrpcRaftServer()
    peer: Final[AbstractRaftPeer] = GrpcRaftPeer()
    raft = Raft(public_id, server=server, peer=peer, configuration=configuration)

    threads = [
        Thread(
            target=server.run,
            kwargs={"host": "0.0.0.0", "port": args.port},
            daemon=True,
        ),
        Thread(target=raft.main, daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
