from pathlib import Path

from .raft import Raft  # noqa: F401
from .state_machine import KeyValueStateMachine, StateMachine  # noqa: F401
from .storage import MemoryStorage, SQLiteStorage, Storage  # noqa: F401
from .types import CONF_CHANGE_ADD, CONF_CHANGE_REMOVE  # noqa: F401

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()
