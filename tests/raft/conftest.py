import uuid

import pytest

from raft.logs import SqliteReplicatedLog


@pytest.fixture(scope="function")
def log() -> SqliteReplicatedLog:
    return SqliteReplicatedLog(id_=str(uuid.uuid4()), volatile=True)
