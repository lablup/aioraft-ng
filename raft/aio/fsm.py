import logging
from typing import Dict, List, Optional

from raft.utils import AtomicInteger

logging.basicConfig(level=logging.INFO)


class RespFSM:
    def __init__(self):
        self._dict: Dict[str, AtomicInteger] = {}
        self._clients: List[str] = []
        # self._sessions: List[str] = []

    def apply(self, command: str) -> Optional[str]:
        match command.split():
            case ["DECRBY", key, value]:
                x = self._dict.get(key, AtomicInteger(0)).decrease(int(value))
                self._dict[key] = x
                return str(x.value)
            case ["DECR", key]:
                x = self._dict.get(key, AtomicInteger(0)).decrease()
                self._dict[key] = x
                return str(x.value)
            case ["INCRBY", key, value]:
                x = self._dict.get(key, AtomicInteger(0)).increase(int(value))
                self._dict[key] = x
                return str(x.value)
            case ["INCR", key]:
                x = self._dict.get(key, AtomicInteger(0)).increase()
                self._dict[key] = x
                return str(x.value)
            case ["GET", key]:
                if atomic_value := self._dict.get(key):
                    return str(atomic_value.value)
            case ["SET", key, value]:
                try:
                    x = AtomicInteger(int(value))
                    self._dict[key] = x
                    return str(x.value)
                except ValueError:
                    pass
        """
        case ["REG", "CLIENT", client_id]:
            if client_id not in self._clients:
                self._clients.append(client_id)
        """
        return None
