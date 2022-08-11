import logging
from typing import Dict, Optional

from raft.utils import AtomicInteger

logging.basicConfig(level=logging.INFO)


class RespInterpreter:
    def __init__(self):
        self._dict: Dict[str, AtomicInteger] = {}

    def execute(self, command: str) -> Optional[int]:
        match command.split():
            case ["DECRBY", key, value]:
                self._dict[key] = self._dict.get(key, AtomicInteger(0)).decrease(
                    int(value)
                )
            case ["DECR", key]:
                self._dict[key] = self._dict.get(key, AtomicInteger(0)).decrease()
            case ["INCRBY", key, value]:
                self._dict[key] = self._dict.get(key, AtomicInteger(0)).increase(
                    int(value)
                )
            case ["INCR", key]:
                self._dict[key] = self._dict.get(key, AtomicInteger(0)).increase()
            case ["GET", key]:
                if atomic_value := self._dict.get(key):
                    return atomic_value.value
            case ["SET", key, value]:
                try:
                    self._dict[key] = AtomicInteger(int(value))
                except ValueError:
                    pass
        return None
