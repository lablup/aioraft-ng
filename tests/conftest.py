import pytest

from aioraft.logs import InMemoryLogReplication, SqliteLogReplication


@pytest.fixture
def n() -> int:
    return 16


@pytest.fixture(scope="function")
def in_memory_log() -> InMemoryLogReplication:
    return InMemoryLogReplication()


@pytest.fixture(scope="function")
async def sqlite_log() -> SqliteLogReplication:
    return await SqliteLogReplication.new(volatile=True)
