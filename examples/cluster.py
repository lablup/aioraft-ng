import argparse
import asyncio
import logging
from contextlib import suppress
from typing import Coroutine, List, Optional

from aioraft import Raft
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer
from aioraft.types import HostPortPair, RaftState
from aioraft.utils import build_loopback_ip

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

_cleanup_coroutines: List[Coroutine] = []


class BandwithAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace,
        values,
        option_string: Optional[str] = None,
    ):
        if isinstance(values, int) and values < 3:
            parser.error(f"Minimum bandwith for {option_string} is 3")
        setattr(namespace, self.dest, values)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", action=BandwithAction, type=int, default=5)
    return parser.parse_args()


async def _main():
    args = parse_args()
    public_ip = build_loopback_ip()
    base_port = 50051
    configuration = tuple(HostPortPair(public_ip, base_port + i) for i in range(args.n))
    cluster = []
    tasks = []
    for host_port_pair in configuration:
        server = GrpcRaftServer()
        client = GrpcRaftClient()

        async def _on_state_changed(next_state: RaftState):
            log.info(f"STATE_CHANGE (n:{host_port_pair} next:{next_state})")

        raft = await Raft.new(
            str(host_port_pair),
            server=server,
            client=client,
            configuration=tuple(map(str, configuration)),
            on_state_changed=_on_state_changed,
        )
        cluster.append(raft)

        tasks.extend(
            [
                asyncio.create_task(
                    server.run(
                        host="0.0.0.0",
                        port=host_port_pair.port,
                        cleanup_coroutines=_cleanup_coroutines,
                    ),
                ),
                asyncio.create_task(raft.main()),
            ]
        )

    done, pending = await asyncio.wait(set(tasks), return_when=asyncio.FIRST_EXCEPTION)

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
