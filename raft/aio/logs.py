import abc
import uuid
from multiprocessing import Lock
from pathlib import Path
from typing import Final, Iterable, List, Optional, Tuple

import aiosqlite

from raft.protos import raft_pb2
from raft.types import Log, aobject


class AbstractReplicatedLog(abc.ABC):
    @abc.abstractmethod
    async def append(self, entries: Iterable[raft_pb2.Log]) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def commit(self, index: int) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def precede(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
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


class MemoryReplicatedLog(AbstractReplicatedLog):
    def __init__(self, *args, **kwargs):
        self._lock = Lock()
        self._logs: List[Log] = []

    async def append(self, entries: Iterable[raft_pb2.Log]) -> bool:
        with self._lock:
            self._logs.extend(map(lambda x: Log.parse(x), entries))
            self._logs.sort(key=lambda x: x.index)
        return True

    async def commit(self, index: int) -> bool:
        with self._lock:
            for entry in self._logs:
                if entry.index <= index:
                    entry.committed = True
        return True

    async def get(self, index: int) -> Optional[raft_pb2.Log]:
        with self._lock:
            for entry in self._logs:
                if entry.index == index:
                    return entry.proto()
        return None

    async def precede(self, index: int) -> Optional[raft_pb2.Log]:
        with self._lock:
            for i, entry in enumerate(self._logs):
                if entry.index == index:
                    return self._logs[i - 1].proto() if i > 0 else None
        return None

    async def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        with self._lock:
            if committed:
                entries = [entry for entry in self._logs if entry.committed]
                return entries[-1].proto() if entries else None
            return self._logs[-1].proto() if self._logs else None

    async def slice(
        self, start: int, stop: Optional[int] = None
    ) -> Tuple[raft_pb2.Log, ...]:
        with self._lock:
            start_idx, stop_idx = 0, None
            for idx, log in enumerate(self._logs):
                if log.index == start:
                    start_idx = idx
                if log.index == stop:
                    stop_idx = idx
            return tuple(map(lambda x: x.proto(), self._logs[start_idx:stop_idx]))

    async def splice(self, start: int) -> None:
        with self._lock:
            start_idx = None
            for idx, log in enumerate(self._logs):
                if log.index == start:
                    start_idx = idx
                    break
            self._logs = self._logs[:start_idx]

    async def count(self) -> int:
        return len(self._logs)


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

    async def append(self, entries: Iterable[raft_pb2.Log]) -> bool:
        rows = tuple(
            (entry.index, entry.term, entry.command, False) for entry in entries
        )
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.executemany(
                f"INSERT INTO {self._table} VALUES (?, ?, ?, ?)", rows
            )
            await conn.commit()
        return True

    async def commit(self, index: int) -> bool:
        async with aiosqlite.connect(self._database) as conn:
            cursor = await conn.cursor()
            await cursor.execute(
                f"UPDATE {self._table} SET committed = TRUE WHERE idx <= :index",
                {"index": index},
            )
            await conn.commit()
        return True

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