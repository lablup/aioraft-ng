import abc
import asyncio
import sqlite3
from typing import List, Optional

from aioraft.protos import raft_pb2

__all__ = ("Storage", "MemoryStorage", "SQLiteStorage")


class Storage(abc.ABC):
    """Abstract persistent storage for Raft state."""

    async def initialize(self) -> None:
        """Optional lifecycle hook: set up storage (e.g. create tables).

        Default implementation is a no-op.
        """

    async def close(self) -> None:
        """Optional lifecycle hook: release resources.

        Default implementation is a no-op.
        """

    @abc.abstractmethod
    async def save_term(self, term: int) -> None: ...

    @abc.abstractmethod
    async def load_term(self) -> int: ...

    @abc.abstractmethod
    async def save_vote(self, voted_for: Optional[str]) -> None: ...

    @abc.abstractmethod
    async def load_vote(self) -> Optional[str]: ...

    @abc.abstractmethod
    async def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        """Persist term and votedFor atomically (single transaction)."""
        ...

    @abc.abstractmethod
    async def append_logs(self, entries: List[raft_pb2.Log]) -> None: ...

    @abc.abstractmethod
    async def truncate_logs_from(self, index: int) -> None: ...

    @abc.abstractmethod
    async def truncate_and_append(self, from_index: int, entries: List[raft_pb2.Log]) -> None:
        """Atomically truncate logs from *from_index* and append *entries*."""
        ...

    @abc.abstractmethod
    async def load_logs(self) -> List[raft_pb2.Log]: ...

    @abc.abstractmethod
    async def save_log_entry(self, entry: raft_pb2.Log) -> None: ...


class MemoryStorage(Storage):
    """In-memory storage for testing."""

    def __init__(self):
        self._term = 0
        self._voted_for: Optional[str] = None
        self._logs: List[raft_pb2.Log] = []

    async def save_term(self, term: int) -> None:
        self._term = term

    async def load_term(self) -> int:
        return self._term

    async def save_vote(self, voted_for: Optional[str]) -> None:
        self._voted_for = voted_for

    async def load_vote(self) -> Optional[str]:
        return self._voted_for

    async def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self._term = term
        self._voted_for = voted_for

    async def append_logs(self, entries: List[raft_pb2.Log]) -> None:
        self._logs.extend(entries)

    async def save_log_entry(self, entry: raft_pb2.Log) -> None:
        self._logs.append(entry)

    async def truncate_logs_from(self, index: int) -> None:
        self._logs = [e for e in self._logs if e.index < index]

    async def truncate_and_append(self, from_index: int, entries: List[raft_pb2.Log]) -> None:
        self._logs = [e for e in self._logs if e.index < from_index]
        self._logs.extend(entries)

    async def load_logs(self) -> List[raft_pb2.Log]:
        return list(self._logs)


class SQLiteStorage(Storage):
    """SQLite-based persistent storage."""

    def __init__(self, db_path: str = "raft.db"):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    async def initialize(self) -> None:
        """Create tables if they don't exist."""
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS raft_state (key TEXT PRIMARY KEY, value TEXT)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS raft_log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, command TEXT NOT NULL)"
        )
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    # -- term --

    async def save_term(self, term: int) -> None:
        await asyncio.to_thread(self._save_term_sync, term)

    def _save_term_sync(self, term: int) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_state (key, value) VALUES ('current_term', ?)",
            (str(term),),
        )
        self._conn.commit()

    async def load_term(self) -> int:
        return await asyncio.to_thread(self._load_term_sync)

    def _load_term_sync(self) -> int:
        cursor = self._conn.execute(
            "SELECT value FROM raft_state WHERE key = 'current_term'"
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    # -- vote --

    async def save_vote(self, voted_for: Optional[str]) -> None:
        await asyncio.to_thread(self._save_vote_sync, voted_for)

    def _save_vote_sync(self, voted_for: Optional[str]) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_state (key, value) VALUES ('voted_for', ?)",
            (voted_for,),
        )
        self._conn.commit()

    async def load_vote(self) -> Optional[str]:
        return await asyncio.to_thread(self._load_vote_sync)

    def _load_vote_sync(self) -> Optional[str]:
        cursor = self._conn.execute(
            "SELECT value FROM raft_state WHERE key = 'voted_for'"
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else None

    # -- atomic term + vote --

    async def save_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        await asyncio.to_thread(self._save_term_and_vote_sync, term, voted_for)

    def _save_term_and_vote_sync(self, term: int, voted_for: Optional[str]) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_state (key, value) VALUES ('current_term', ?)",
            (str(term),),
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_state (key, value) VALUES ('voted_for', ?)",
            (voted_for,),
        )
        self._conn.commit()

    # -- logs --

    async def append_logs(self, entries: List[raft_pb2.Log]) -> None:
        await asyncio.to_thread(self._append_logs_sync, entries)

    def _append_logs_sync(self, entries: List[raft_pb2.Log]) -> None:
        self._conn.executemany(
            "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
            [(e.index, e.term, e.command) for e in entries],
        )
        self._conn.commit()

    async def save_log_entry(self, entry: raft_pb2.Log) -> None:
        await asyncio.to_thread(self._save_log_entry_sync, entry)

    def _save_log_entry_sync(self, entry: raft_pb2.Log) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
            (entry.index, entry.term, entry.command),
        )
        self._conn.commit()

    async def truncate_logs_from(self, index: int) -> None:
        await asyncio.to_thread(self._truncate_logs_from_sync, index)

    def _truncate_logs_from_sync(self, index: int) -> None:
        self._conn.execute("DELETE FROM raft_log WHERE idx >= ?", (index,))
        self._conn.commit()

    async def truncate_and_append(self, from_index: int, entries: List[raft_pb2.Log]) -> None:
        await asyncio.to_thread(self._truncate_and_append_sync, from_index, entries)

    def _truncate_and_append_sync(self, from_index: int, entries: List[raft_pb2.Log]) -> None:
        self._conn.execute("DELETE FROM raft_log WHERE idx >= ?", (from_index,))
        if entries:
            self._conn.executemany(
                "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
                [(e.index, e.term, e.command) for e in entries],
            )
        self._conn.commit()

    async def load_logs(self) -> List[raft_pb2.Log]:
        return await asyncio.to_thread(self._load_logs_sync)

    def _load_logs_sync(self) -> List[raft_pb2.Log]:
        cursor = self._conn.execute(
            "SELECT idx, term, command FROM raft_log ORDER BY idx"
        )
        return [
            raft_pb2.Log(index=row[0], term=row[1], command=row[2])
            for row in cursor.fetchall()
        ]
