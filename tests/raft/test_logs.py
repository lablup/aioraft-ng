import random

from raft.logs import SqliteReplicatedLog
from raft.protos import raft_pb2


def test_sqlite_replicated_log(log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    log.append(entries)

    count = log.count()
    assert count == len(entries)

    random_index = random.randint(1, count)
    row = log.get(random_index)
    assert row is not None
    assert row.index == random_index

    row = log.last()
    assert row is not None
    assert row.index == n


def test_sqlite_replicated_log__slice(log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    log.append(entries)

    random_start = random.randint(1, n)
    random_stop = random.randint(random_start + 1, n + 1)
    rows = log.slice(random_start, random_stop)
    assert len(rows) == (random_stop - random_start)
    assert rows[0].index == random_start
    assert rows[-1].index == (random_stop - 1)


def test_sqlite_replicated_log__splice(log: SqliteReplicatedLog) -> None:
    n = 16
    entries = tuple(
        raft_pb2.Log(index=i, term=1, command="INCR image-pull-rate")
        for i in range(1, n + 1)
    )
    log.append(entries)

    random_start = random.randint(1, n)
    log.splice(random_start)

    count = log.count()
    assert count == (random_start - 1)
