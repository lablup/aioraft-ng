from pathlib import Path

from .raft import Raft  # noqa: F401

__version__ = (Path(__file__).parent / "VERSION").read_text().strip()
