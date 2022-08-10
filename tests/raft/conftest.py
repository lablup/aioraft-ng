import uuid

import pytest

from raft.logs import MemoryReplicatedLog, SqliteReplicatedLog


@pytest.fixture(scope="function")
def memory_replicated_log() -> MemoryReplicatedLog:
    return MemoryReplicatedLog()


@pytest.fixture(scope="function")
def sqlite_replicated_log() -> SqliteReplicatedLog:
    return SqliteReplicatedLog(id_=str(uuid.uuid4()), volatile=True)
