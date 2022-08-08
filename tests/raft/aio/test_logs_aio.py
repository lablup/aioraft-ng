import random

import pytest

from raft.aio.logs import SqliteReplicatedLog
from raft.protos import raft_pb2


@pytest.mark.asyncio
async def test_sqlite_replicated_log_aio(log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await log.append(entries)

    count = await log.count()
    assert count == len(entries)

    random_index = random.randint(1, count)
    row = await log.get(random_index)
    assert row is not None
    assert row.index == random_index

    row = await log.last()
    assert row is not None
    assert row.index == n


@pytest.mark.asyncio
async def test_sqlite_replicated_log_aio__slice(log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await log.append(entries)

    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = await log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


@pytest.mark.asyncio
async def test_sqlite_replicated_log_aio__splice(log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await log.append(entries)

    random_start = random.randint(1, n)
    await log.splice(random_start)

    count = await log.count()
    assert count == (random_start - 1)
