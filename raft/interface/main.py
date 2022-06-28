import argparse
import asyncio
from contextlib import suppress
from typing import Coroutine, List

from raft.interface.client import AsyncGrpcRaftClient
from raft.interface.fsm import RaftFiniteStateMachine
from raft.interface.server import GrpcRaftServer

_cleanup_coroutines: List[Coroutine] = []


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=50051)
    parser.add_argument('peers', metavar='peers', type=str, nargs='+',
                        help='"<HOST>:<PORT>" list of peers.')
    return parser.parse_args()


async def _main():
    args = parse_args()

    client = AsyncGrpcRaftClient()
    server = GrpcRaftServer()
    raft = RaftFiniteStateMachine(peers=args.peers, server=server, client=client)

    done, pending = await asyncio.wait({
        asyncio.create_task(
            GrpcRaftServer.run(server, cleanup_coroutines=_cleanup_coroutines, port=args.port),
        ),
        asyncio.create_task(raft.main()),
    }, return_when=asyncio.FIRST_EXCEPTION)
    for task in pending:
        with suppress(asyncio.CancelledError):
            task.cancel()


async def main():
    try:
        await _main()
    finally:
        await asyncio.gather(*_cleanup_coroutines)


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
