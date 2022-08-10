import random

from raft.logs import MemoryReplicatedLog, SqliteReplicatedLog
from raft.protos import raft_pb2


def test_memory_replicated_log(memory_replicated_log: MemoryReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    memory_replicated_log.append(entries)

    count = memory_replicated_log.count()
    assert count == len(entries)

    random_index = random.randint(1, count)
    row = memory_replicated_log.get(random_index)
    assert row is not None
    assert row.index == random_index

    row = memory_replicated_log.last()
    assert row is not None
    assert row.index == n


def test_memory_replicated_log__slice(memory_replicated_log: MemoryReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    memory_replicated_log.append(entries)

    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = memory_replicated_log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


def test_memory_replicated_log__splice(memory_replicated_log: MemoryReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    memory_replicated_log.append(entries)

    random_start = random.randint(1, n)
    memory_replicated_log.splice(random_start)

    count = memory_replicated_log.count()
    assert count == (random_start - 1)


def test_sqlite_replicated_log(sqlite_replicated_log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    sqlite_replicated_log.append(entries)

    count = sqlite_replicated_log.count()
    assert count == len(entries)

    random_index = random.randint(1, count)
    row = sqlite_replicated_log.get(random_index)
    assert row is not None
    assert row.index == random_index

    row = sqlite_replicated_log.last()
    assert row is not None
    assert row.index == n


def test_sqlite_replicated_log__slice(sqlite_replicated_log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    sqlite_replicated_log.append(entries)

    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = sqlite_replicated_log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


def test_sqlite_replicated_log__splice(sqlite_replicated_log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    sqlite_replicated_log.append(entries)

    random_start = random.randint(1, n)
    sqlite_replicated_log.splice(random_start)

    count = sqlite_replicated_log.count()
    assert count == (random_start - 1)
