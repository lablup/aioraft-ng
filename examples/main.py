import argparse
import asyncio
import logging
from contextlib import suppress
from typing import Coroutine, List

from aioraft import Raft
from aioraft.client import GrpcRaftClient
from aioraft.common.logging_utils import BraceStyleAdapter
from aioraft.server import GrpcRaftServer
from aioraft.types import HostPortPair, RaftState
from aioraft.utils import build_loopback_ip

log = BraceStyleAdapter(logging.getLogger(__name__))

_cleanup_coroutines: List[Coroutine] = []

PORTS = (50051, 50052, 50053, 50054, 50055)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", "-i", type=int)
    return parser.parse_args()


async def _main():
    args = parse_args()
    port = PORTS[args.id]
    public_ip = build_loopback_ip()
    public_id = HostPortPair(public_ip, port)

    configuration = tuple(
        HostPortPair(public_ip, port_) for port_ in set(PORTS) - set(port)
    )

    async def _on_state_changed(next_state: RaftState):
        log.info("STATE_CHANGE (n:{} next:{})", public_id, next_state)

    server = GrpcRaftServer()
    client = GrpcRaftClient()
    raft = await Raft.new(
        str(public_id),
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
