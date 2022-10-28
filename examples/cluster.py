import asyncio
import logging
from contextlib import suppress
from multiprocessing import Process
from pathlib import Path
from typing import Coroutine, List, Tuple

import tomli

from aioraft import Raft
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer
from aioraft.types import RaftState

logging.basicConfig(level=logging.INFO)


async def _main(
    pidx: int,
    address: str,
    configuration: Tuple[str, ...],
    cleanup_coroutines: List[Coroutine],
):
    async def on_state_changed(next_state: RaftState):
        logging.info(f"[pidx={pidx}] state: {next_state}")

    server = GrpcRaftServer()
    client = GrpcRaftClient()
    raft = await Raft.new(
        address,
        server=server,
        client=client,
        configuration=configuration,
        on_state_changed=on_state_changed,
    )

    host, port = address.split(":")

    done, pending = await asyncio.wait(
        {
            asyncio.create_task(
                server.run(
                    host=host,
                    port=int(port),
                    cleanup_coroutines=cleanup_coroutines,
                )
            ),
            asyncio.create_task(raft.main()),
        },
        return_when=asyncio.FIRST_EXCEPTION,
    )
    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def amain(pidx: int, address: str, configuration: Tuple[str, ...]):
    cleanup_coroutines: List[Coroutine] = []
    try:
        await _main(
            pidx=pidx,
            address=address,
            configuration=configuration,
            cleanup_coroutines=cleanup_coroutines,
        )
    finally:
        await asyncio.gather(*cleanup_coroutines)


def run(pidx: int, address: str, configuration: Tuple[str, ...]):
    asyncio.run(amain(pidx=pidx, address=address, configuration=configuration))


def main():
    config = tomli.loads((Path(__file__).parent / "config.toml").read_text().strip())
    configuration = config["raft"]["configuration"]

    processes = [
        Process(
            target=run,
            args=(i, address, tuple(filter(lambda x: x != address, configuration))),
        )
        for i, address in enumerate(configuration)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
