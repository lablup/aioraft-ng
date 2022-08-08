import uuid

import pytest

from raft.aio.logs import SqliteReplicatedLog


@pytest.fixture(scope="function")
async def log() -> SqliteReplicatedLog:
    return await SqliteReplicatedLog.new(id_=str(uuid.uuid4()), volatile=True)
