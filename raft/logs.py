import abc
import sqlite3
import uuid
from multiprocessing import Lock
from pathlib import Path
from typing import Final, Iterable, List, Optional, Tuple

from raft.protos import raft_pb2
from raft.types import Log


class AbstractReplicatedLog(abc.ABC):
    @abc.abstractmethod
    def append(self, entries: Iterable[raft_pb2.Log]) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def commit(self, index: int) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    def slice(self, start: int, stop: Optional[int] = None) -> Tuple[raft_pb2.Log, ...]:
        raise NotImplementedError()

    @abc.abstractmethod
    def splice(self, start: int) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def count(self) -> int:
        raise NotImplementedError()


class MemoryReplicatedLog(AbstractReplicatedLog):
    def __init__(self, *args, **kwargs):
        self._lock = Lock()
        self._logs: List[Log] = []

    def append(self, entries: Iterable[raft_pb2.Log]) -> bool:
        with self._lock:
            self._logs.extend(map(lambda x: Log.parse(x), entries))
            self._logs.sort(key=lambda x: x.index)
        return True

    def commit(self, index: int) -> bool:
        with self._lock:
            for entry in self._logs:
                if entry.index <= index:
                    entry.committed = True
        return True

    def get(self, index: int) -> Optional[raft_pb2.Log]:
        with self._lock:
            for log in self._logs:
                if log.index == index:
                    return log
        return None

    def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        with self._lock:
            if committed:
                entries = [entry for entry in self._logs if entry.committed]
                return entries[-1].proto() if entries else None
            return self._logs[-1].proto() if self._logs else None

    def slice(self, start: int, stop: Optional[int] = None) -> Tuple[raft_pb2.Log, ...]:
        with self._lock:
            start_idx, stop_idx = 0, None
            for idx, log in enumerate(self._logs):
                if log.index == start:
                    start_idx = idx
                if log.index == stop:
                    stop_idx = idx
            return tuple(self._logs[start_idx:stop_idx])

    def splice(self, start: int) -> None:
        with self._lock:
            start_idx = None
            for idx, log in enumerate(self._logs):
                if log.index == start:
                    start_idx = idx
                    break
            self._logs = self._logs[:start_idx]

    def count(self) -> int:
        return len(self._logs)


class SqliteReplicatedLog(AbstractReplicatedLog):
    def __init__(self, *args, **kwargs):
        self._id: str = kwargs.get("id_") or str(uuid.uuid4())
        self._volatile: bool = kwargs.get("volatile", False)
        if path := kwargs.get("path"):
            self._database = Path(path)
        self._database = Path(__file__).parent / f"{self._id}.db"
        self._table: Final[str] = "raft"

        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table}
                (idx INTEGER PRIMARY KEY, term INTEGER, command TEXT, committed BOOLEAN)
            """
            )
            conn.commit()

    def __del__(self):
        if self._volatile:
            self._database.unlink(missing_ok=True)

    def append(self, entries: Iterable[raft_pb2.Log]) -> bool:
        rows = tuple(
            (entry.index, entry.term, entry.command, False) for entry in entries
        )
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            cursor.executemany(f"INSERT INTO {self._table} VALUES (?, ?, ?, ?)", rows)
            conn.commit()
        return True

    def commit(self, index: int) -> bool:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {self._table} SET committed = TRUE WHERE idx <= :index",
                {"index": index},
            )
            conn.commit()
        return True

    def get(self, index: int) -> Optional[raft_pb2.Log]:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            if row := cursor.execute(
                f"SELECT * FROM {self._table} WHERE idx = :index",
                {"index": index},
            ).fetchone():
                row = raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            return row

    def last(self, committed: bool = False) -> Optional[raft_pb2.Log]:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            if row := cursor.execute(
                f"SELECT * FROM {self._table}"
                f" {'WHERE committed = TRUE' if committed else ''}"
                " ORDER BY idx DESC LIMIT 1"
            ).fetchone():
                return raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            return None

    def slice(self, start: int, stop: Optional[int] = None) -> Tuple[raft_pb2.Log, ...]:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            query = f"SELECT * FROM {self._table} WHERE idx >= :start"
            parameters = {"start": start}
            if stop is not None:
                query += " AND idx < :stop"
                parameters.update({"stop": stop})
            rows = cursor.execute(query, parameters).fetchall()
            return tuple(
                raft_pb2.Log(index=row[0], term=row[1], command=row[2]) for row in rows
            )

    def splice(self, start: int) -> None:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM {self._table} WHERE idx >= :start", {"start": start}
            )
            conn.commit()

    def count(self) -> int:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            count, *_ = cursor.execute(f"SELECT COUNT(*) FROM {self._table}").fetchone()
            return count
