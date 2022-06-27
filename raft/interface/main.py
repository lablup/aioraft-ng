import argparse
import asyncio
from contextlib import suppress

from raft.interface.client import AsyncGrpcRaftClient
from raft.interface.fsm import RaftFiniteStateMachine
from raft.interface.server import GrpcRaftServer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=50051)
    parser.add_argument('peers', metavar='peers', type=str, nargs='+',
                        help='"<HOST>:<PORT>" list of peers.')
    return parser.parse_args()


async def main():
    args = parse_args()

    client = AsyncGrpcRaftClient()
    server = GrpcRaftServer()
    raft = RaftFiniteStateMachine(peers=args.peers, server=server, client=client)

    done, pending = await asyncio.wait({
        asyncio.create_task(
            GrpcRaftServer.run(server, port=args.port),
        ),
        asyncio.create_task(raft.main()),
    }, return_when=asyncio.ALL_COMPLETED)
    for task in pending:
        with suppress(asyncio.CancelledError):
            task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
