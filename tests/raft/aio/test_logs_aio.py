import random

import pytest

from raft.aio.logs import MemoryReplicatedLog, SqliteReplicatedLog
from raft.protos import raft_pb2


@pytest.mark.asyncio
async def test_memory_replicated_log(
    memory_replicated_log: MemoryReplicatedLog,
) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await memory_replicated_log.append(entries)

    count = await memory_replicated_log.count()
    assert count == len(entries)

    random_index = random.randint(1, n)
    row = await memory_replicated_log.get(random_index)
    assert row is not None
    assert row.index == random_index

    row = await memory_replicated_log.precede(1)
    assert row is None

    random_index = random.randint(2, n)
    row = await memory_replicated_log.precede(random_index)
    assert row is not None
    assert row.index == random_index - 1

    row = await memory_replicated_log.last()
    assert row is not None
    assert row.index == n

    random_commit_index = random.randint(1, n)
    await memory_replicated_log.commit(index=random_commit_index)
    committed_row = await memory_replicated_log.last(committed=True)
    assert committed_row is not None
    assert committed_row.index == random_commit_index


@pytest.mark.asyncio
async def test_memory_replicated_log__slice(
    memory_replicated_log: MemoryReplicatedLog,
) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await memory_replicated_log.append(entries)

    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = await memory_replicated_log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


@pytest.mark.asyncio
async def test_memory_replicated_log__splice(
    memory_replicated_log: MemoryReplicatedLog,
) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await memory_replicated_log.append(entries)

    random_start = random.randint(1, n)
    await memory_replicated_log.splice(random_start)

    count = await memory_replicated_log.count()
    assert count == (random_start - 1)


@pytest.mark.asyncio
async def test_sqlite_replicated_log_aio(
    sqlite_replicated_log: SqliteReplicatedLog,
) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await sqlite_replicated_log.append(entries)

    count = await sqlite_replicated_log.count()
    assert count == len(entries)

    random_index = random.randint(1, n)
    row = await sqlite_replicated_log.get(random_index)
    assert row is not None
    assert row.index == random_index

    row = await sqlite_replicated_log.precede(1)
    assert row is None

    random_index = random.randint(2, n)
    row = await sqlite_replicated_log.precede(random_index)
    assert row is not None
    assert row.index == random_index - 1

    row = await sqlite_replicated_log.last()
    assert row is not None
    assert row.index == n

    random_commit_index = random.randint(1, n)
    await sqlite_replicated_log.commit(index=random_commit_index)
    committed_row = await sqlite_replicated_log.last(committed=True)
    assert committed_row is not None
    assert committed_row.index == random_commit_index


@pytest.mark.asyncio
async def test_sqlite_replicated_log_aio__slice(
    sqlite_replicated_log: SqliteReplicatedLog,
) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await sqlite_replicated_log.append(entries)

    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = await sqlite_replicated_log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


@pytest.mark.asyncio
async def test_sqlite_replicated_log_aio__splice(
    sqlite_replicated_log: SqliteReplicatedLog,
) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await sqlite_replicated_log.append(entries)

    random_start = random.randint(1, n)
    await sqlite_replicated_log.splice(random_start)

    count = await sqlite_replicated_log.count()
    assert count == (random_start - 1)
