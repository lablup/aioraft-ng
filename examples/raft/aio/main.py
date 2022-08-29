import argparse
import asyncio
from contextlib import suppress
from pathlib import Path
from typing import Coroutine, List

import tomli

from raft.aio import Raft
from raft.aio.client import GrpcRaftClient
from raft.aio.server import GrpcRaftServer
from raft.types import RaftState
from raft.utils import build_loopback_ip

_cleanup_coroutines: List[Coroutine] = []


def load_config():
    path = Path(__file__).parent.parent / "config.toml"
    return tomli.loads(path.read_text())


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", "-p", type=int, default=50051)
    return parser.parse_args()


async def _main():
    args = parse_args()
    public_ip = build_loopback_ip()
    public_id = f"{public_ip}:{args.port}"

    config = load_config()
    configuration = tuple(
        server
        for server in config["raft"]["configuration"]
        if not server.endswith(str(args.port))
    )

    async def _on_state_changed(next_state: RaftState):
        print(f"[_on_state_changed] next_state: {next_state}")

    server = GrpcRaftServer()
    client = GrpcRaftClient()
    raft = await Raft.new(
        public_id,
        server=server,
        client=client,
        configuration=configuration,
        on_state_changed=_on_state_changed,
    )

    done, pending = await asyncio.wait(
        {
            asyncio.create_task(
                server.run(
                    host="0.0.0.0",
                    port=args.port,
                    cleanup_coroutines=_cleanup_coroutines,
                ),
            ),
            asyncio.create_task(raft.main()),
        },
        return_when=asyncio.FIRST_EXCEPTION,
    )
    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def main():
    try:
        await _main()
    finally:
        await asyncio.gather(*_cleanup_coroutines)


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
