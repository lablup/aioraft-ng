import abc
from asyncio import Lock
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterable, List, Optional, Tuple
from uuid import uuid4

import aiosqlite

from aioraft.protos import raft_pb2
from aioraft.types import aobject


@dataclass
class Entry:
    index: int
    term: int
    command: Optional[str] = None
    committed: bool = False

    def proto(self) -> raft_pb2.Log:
        return raft_pb2.Log(index=self.index, term=self.term, command=self.command)

    @classmethod
    def parse(cls, proto: raft_pb2.Log) -> "Entry":
        return cls(index=proto.index, term=proto.term, command=proto.command)


class AbstractLogReplication(abc.ABC):
    @abc.abstractmethod
    async def append(
        self, entries: Iterable[raft_pb2.Log]
    ) -> None:  # FIXME: *entries: Tuple
        raise NotImplementedError

    @abc.abstractmethod
    async def commit(self, index: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError

    @abc.abstractmethod
    async def precede(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError

    @abc.abstractmethod
    async def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        raise NotImplementedError

    @abc.abstractmethod
    async def slice(
        self, start: int, stop: Optional[int] = None
    ) -> Tuple[raft_pb2.Log, ...]:
        raise NotImplementedError

    @abc.abstractmethod
    async def splice(self, start: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def count(self) -> int:
        raise NotImplementedError


class InMemoryLogReplication(AbstractLogReplication):
    def __init__(self):
        self._lock = Lock()
        self._entries: List[Entry] = []

    async def append(self, entries: Iterable[raft_pb2.Log]) -> None:
        async with self._lock:
            self._entries.extend(map(lambda x: Entry.parse(x), entries))
            self._entries.sort(key=lambda x: x.index)

    async def commit(self, index: int) -> None:
        async with self._lock:
            for entry in self._entries:
                if entry.index <= index:
                    entry.committed = True

    async def get(self, index: int) -> Optional[raft_pb2.Log]:
        async with self._lock:
            for entry in self._entries:
                if entry.index == index:
                    return entry.proto()
        return None

    async def precede(self, index: int) -> Optional[raft_pb2.Log]:
        async with self._lock:
            for i, entry in enumerate(self._entries):
                if entry.index == index:
                    return self._entries[i - 1].proto() if i > 0 else None
        return None

    async def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        async with self._lock:
            if committed:
                entries = [entry for entry in self._entries if entry.committed]
                return entries[-1].proto() if entries else None
            return self._entries[-1].proto() if self._entries else None

    async def slice(
        self, start: int, stop: Optional[int] = None
    ) -> Tuple[raft_pb2.Log, ...]:
        assert stop is None or start <= stop
        async with self._lock:
            start_idx, stop_idx = (
                0,
                None,
            )  # NOTE: Convert given list index to log index.
            for idx, entry in enumerate(self._entries):
                if entry.index == start:
                    start_idx = idx
                if entry.index == stop:
                    stop_idx = idx
            return tuple(map(lambda x: x.proto(), self._entries[start_idx:stop_idx]))

    async def splice(self, start: int) -> None:
        async with self._lock:
            for idx, log in enumerate(self._entries):
                if log.index == start:
                    del self._entries[idx:]
                    return

    async def count(self) -> int:
        return len(self._entries)


class SqliteLogReplication(aobject, AbstractLogReplication):  # TODO: SQLAlchemy
    def __init__(
        self,
        id: Optional[str] = None,
        path: Optional[str] = None,
        volatile: bool = False,
    ) -> None:
        self._id: str = id or str(uuid4())
        self._volatile: bool = volatile
        self._database: Final[Path] = (
            Path(path) if path else Path(__file__).parent / f"{self._id}.db"
        )
        self._table: Final[str] = "raft"

    async def __ainit__(self) -> None:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table}
                (idx INTEGER PRIMARY KEY, term INTEGER, command TEXT, committed BOOLEAN)
                """
            )
            await conn.commit()

    def __del__(self) -> None:
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

    async def commit(self, index: int) -> None:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                f"UPDATE {self._table} SET committed = TRUE WHERE idx <= :index",
                {"index": index},
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

    async def precede(self, index: int) -> Optional[raft_pb2.Log]:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            cursor = await cursor.execute(
                f"SELECT * FROM {self._table} WHERE idx < :index ORDER BY idx DESC LIMIT 1",
                {"index": index},
            )
            if row := await cursor.fetchone():
                return raft_pb2.Log(index=row[0], term=row[1], command=row[2])
        return None

    async def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            cursor = await cursor.execute(
                f"SELECT * FROM {self._table}"
                f" {'WHERE committed = TRUE' if committed else ''}"
                " ORDER BY idx DESC LIMIT 1"
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
            params = {"start": start}
            if stop is not None:
                query += " AND idx < :stop"
                params.update({"stop": stop})
            cursor = await cursor.execute(query, params)
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
