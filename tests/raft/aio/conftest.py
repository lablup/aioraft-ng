import uuid

import pytest

from raft.aio.logs import MemoryReplicatedLog, SqliteReplicatedLog


@pytest.fixture(scope="function")
async def memory_replicated_log() -> MemoryReplicatedLog:
    return MemoryReplicatedLog()


@pytest.fixture(scope="function")
async def sqlite_replicated_log() -> SqliteReplicatedLog:
    return await SqliteReplicatedLog.new(id_=str(uuid.uuid4()), volatile=True)
