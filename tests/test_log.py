import random

import pytest

from aioraft.logs import (
    AbstractLogReplication,
    InMemoryLogReplication,
    SqliteLogReplication,
)
from aioraft.protos import raft_pb2


async def _initialize_replicated_logs(log: AbstractLogReplication, n: int = 16) -> None:
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    await log.append(entries)


async def _test_log_replication(log: AbstractLogReplication, n: int) -> None:
    count = await log.count()
    assert count == n

    random_index = random.randint(1, count)
    row = await log.get(random_index)
    assert row is not None
    assert row.index == random_index

    random_index = random.randint(2, count)
    row = await log.precede(random_index)
    assert row.index == (random_index - 1)

    row = await log.last()
    assert row is not None
    assert row.index == n

    random_commit_index = random.randint(1, count)
    await log.commit(index=random_commit_index)
    last_committed_row = await log.last(committed=True)
    assert last_committed_row is not None
    assert last_committed_row.index == random_commit_index


async def _test_log_replication__slice(log: AbstractLogReplication, n: int) -> None:
    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = await log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


async def _test_log_replication__splice(log: AbstractLogReplication, n: int) -> None:
    random_start = random.randint(1, n)
    await log.splice(random_start)

    count = await log.count()
    assert count == (random_start - 1)


# ================ IN MEMORY LOG REPLICATION ================


@pytest.mark.asyncio
async def test_in_memory_log_replication(
    in_memory_log: InMemoryLogReplication, n: int
) -> None:
    await _initialize_replicated_logs(in_memory_log, n=n)
    await _test_log_replication(in_memory_log, n=n)


@pytest.mark.asyncio
async def test_in_memory_log_replication__slice(
    in_memory_log: InMemoryLogReplication, n: int
) -> None:
    await _initialize_replicated_logs(in_memory_log, n=n)
    await _test_log_replication__slice(in_memory_log, n=n)


@pytest.mark.asyncio
async def test_in_memory_log_replication__splice(
    in_memory_log: InMemoryLogReplication, n: int
) -> None:
    await _initialize_replicated_logs(in_memory_log, n=n)
    await _test_log_replication__splice(in_memory_log, n=n)


# ================ SQLITE LOG REPLICATION ================


@pytest.mark.asyncio
async def test_sqlite_log_replication(sqlite_log: SqliteLogReplication, n: int) -> None:
    await _initialize_replicated_logs(sqlite_log, n=n)
    await _test_log_replication(sqlite_log, n=n)


@pytest.mark.asyncio
async def test_sqlite_log_replication__slice(
    sqlite_log: SqliteLogReplication, n: int
) -> None:
    await _initialize_replicated_logs(sqlite_log, n=n)
    await _test_log_replication__slice(sqlite_log, n=n)


@pytest.mark.asyncio
async def test_sqlite_log_replication__splice(
    sqlite_log: SqliteLogReplication, n: int
) -> None:
    await _initialize_replicated_logs(sqlite_log, n=n)
    await _test_log_replication__splice(sqlite_log, n=n)
