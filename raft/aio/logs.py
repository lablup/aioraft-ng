import abc
import uuid
from pathlib import Path
from typing import Final, Iterable, Optional, Tuple

import aiosqlite

from raft.protos import raft_pb2
from raft.types import aobject


class AbstractReplicatedLog(abc.ABC):
    @abc.abstractmethod
    async def append(self, entries: Iterable[raft_pb2.Log]) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def last(self) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def slice(
        self, start: int, stop: Optional[int] = None
    ) -> Tuple[raft_pb2.Log, ...]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def splice(self, start: int) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def count(self) -> int:
        raise NotImplementedError()


class SqliteReplicatedLog(aobject, AbstractReplicatedLog):
    def __init__(self, *args, **kwargs):
        self._id: str = kwargs.get("id_") or str(uuid.uuid4())
        self._volatile: bool = kwargs.get("volatile", False)
        if path := kwargs.get("path"):
            self._database = Path(path)
        self._database = Path(__file__).parent / f"{self._id}.db"
        self._table: Final[str] = "raft"

    async def __ainit__(self, *args, **kwargs):
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table}
                (idx INTEGER PRIMARY KEY, term INTEGER, command TEXT, committed BOOLEAN)
            """
            )
            await conn.commit()

    def __del__(self):
        if self._volatile:
            self._database.unlink(missing_ok=True)

    async def append(self, entries: Iterable[raft_pb2.Log]) -> None:
        rows = tuple(
            (entry.index, entry.term, entry.command, False) for entry in entries
        )
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.executemany(
                f"INSERT INTO {self._table} VALUES (?, ?, ?, ?)", rows
            )
            await conn.commit()

    async def get(self, index: int) -> Optional[raft_pb2.Log]:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            cursor = await cursor.execute(
                f"SELECT * FROM {self._table} WHERE idx = :index", {"index": index}
            )
            if row := await cursor.fetchone():
                return raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            return None

    async def last(self) -> Optional[raft_pb2.Log]:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            cursor = await cursor.execute(
                f"SELECT * FROM {self._table} ORDER BY idx DESC LIMIT 1"
            )
            if row := await cursor.fetchone():
                return raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            return None

    async def slice(
        self, start: int, stop: Optional[int] = None
    ) -> Tuple[raft_pb2.Log, ...]:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            query = f"SELECT * FROM {self._table} WHERE idx >= :start"
            parameters = {"start": start}
            if stop is not None:
                query += " AND idx < :stop"
                parameters.update({"stop": stop})
            cursor = await cursor.execute(query, parameters)
            rows = await cursor.fetchall()
            return tuple(
                raft_pb2.Log(index=row[0], term=row[1], command=row[2]) for row in rows
            )

    async def splice(self, start: int) -> None:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                f"DELETE FROM {self._table} WHERE idx >= :start", {"start": start}
            )
            await conn.commit()

    async def count(self) -> int:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            cursor = await cursor.execute(f"SELECT COUNT(*) FROM {self._table}")
            if result := await cursor.fetchone():
                return result[0]
            return 0
