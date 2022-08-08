import abc
import sqlite3
import uuid
from pathlib import Path
from typing import Final, Iterable, Optional, Tuple

from raft.protos import raft_pb2
from raft.types import aobject


class AbstractReplicatedLog(abc.ABC):
    @abc.abstractmethod
    def append(self, entries: Iterable[raft_pb2.Log]) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, index: int) -> Optional[raft_pb2.Log]:
        raise NotImplementedError()

    @abc.abstractmethod
    def last(self) -> Optional[raft_pb2.Log]:
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


class SqliteReplicatedLog(aobject, AbstractReplicatedLog):
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

    def append(self, entries: Iterable[raft_pb2.Log]) -> None:
        rows = tuple(
            (entry.index, entry.term, entry.command, False) for entry in entries
        )
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            cursor.executemany(f"INSERT INTO {self._table} VALUES (?, ?, ?, ?)", rows)
            conn.commit()

    def get(self, index: int) -> Optional[raft_pb2.Log]:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            if row := cursor.execute(
                f"SELECT * FROM {self._table} WHERE idx = :index",
                {"index": index},
            ).fetchone():
                row = raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            return row

    def last(self) -> Optional[raft_pb2.Log]:
        with sqlite3.connect(self._database) as conn:
            cursor = conn.cursor()
            if row := cursor.execute(
                f"SELECT * FROM {self._table} ORDER BY idx DESC LIMIT 1"
            ).fetchone():
                row = raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            return row

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
