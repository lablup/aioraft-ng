import abc
import json
from typing import Any


class StateMachine(abc.ABC):
    @abc.abstractmethod
    async def apply(self, command: str) -> Any:
        """Apply a committed log entry. Returns the result."""
        ...

    @abc.abstractmethod
    async def snapshot(self) -> bytes:
        """Serialize current state to bytes."""
        ...

    @abc.abstractmethod
    async def restore(self, data: bytes) -> None:
        """Restore state from a snapshot."""
        ...


class KeyValueStateMachine(StateMachine):
    """Simple dict-based KV store. Commands are 'SET key value', 'GET key', 'DELETE key'."""

    def __init__(self):
        self._store: dict[str, str] = {}

    async def apply(self, command: str) -> Any:
        parts = command.split(maxsplit=2)
        op = parts[0].upper()
        if op == "SET" and len(parts) == 3:
            self._store[parts[1]] = parts[2]
            return parts[2]
        elif op == "GET" and len(parts) == 2:
            return self._store.get(parts[1])
        elif op == "DELETE" and len(parts) == 2:
            return self._store.pop(parts[1], None)
        else:
            raise ValueError(f"Unknown command: {command}")

    async def snapshot(self) -> bytes:
        """Serialize current state to bytes using JSON."""
        return json.dumps(self._store).encode("utf-8")

    async def restore(self, data: bytes) -> None:
        """Restore state from a JSON snapshot."""
        self._store = json.loads(data.decode("utf-8"))
