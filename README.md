# aioraft-ng

[![ci](https://github.com/lablup/aioraft-ng/actions/workflows/default.yml/badge.svg)](https://github.com/lablup/aioraft-ng/actions/workflows/default.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

An implementation of the [Raft consensus algorithm](https://raft.github.io/) written in asyncio-based Python.

## Features

- **Leader Election** — Randomized election timeouts with `asyncio.Event`-based timer
- **Log Replication** — Full AppendEntries pipeline with `nextIndex`/`matchIndex` tracking
- **State Machine** — Pluggable `StateMachine` interface with built-in key-value store
- **Client Interface** — gRPC-based client request handling with leader redirect
- **Persistent Storage** — `Storage` ABC with SQLite backend (WAL mode, crash-safe transactions)
- **Log Compaction** — Snapshot-based compaction with `InstallSnapshot` RPC
- **Membership Changes** — Single-server changes with configuration persistence

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Quick Start

```bash
# Clone and install
git clone https://github.com/lablup/aioraft-ng.git
cd aioraft-ng
uv sync --group dev

# Run tests
uv run pytest tests/

# Lint and typecheck
uv run ruff check .
uv run mypy aioraft/
```

## Usage

```python
import asyncio
from aioraft import Raft, KeyValueStateMachine
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer
from aioraft.storage import SQLiteStorage

async def main():
    server = GrpcRaftServer()
    client = GrpcRaftClient()
    storage = SQLiteStorage("node1.db")
    state_machine = KeyValueStateMachine()

    raft = await Raft.new(
        "127.0.0.1:50051",
        server=server,
        client=client,
        configuration=["127.0.0.1:50052", "127.0.0.1:50053"],
        state_machine=state_machine,
        storage=storage,
    )

    # Run the server and raft main loop concurrently
    await asyncio.gather(
        server.run(host="0.0.0.0", port=50051),
        raft.main(),
    )

asyncio.run(main())
```

## Architecture

```
aioraft/
├── raft.py           # Core Raft state machine
├── protocol.py       # Abstract protocol interface (ABC)
├── server.py         # gRPC server (RPC receiver)
├── client.py         # gRPC client (RPC sender)
├── state_machine.py  # StateMachine ABC + KeyValueStateMachine
├── storage.py        # Storage ABC + MemoryStorage + SQLiteStorage
├── types.py          # RaftId, RaftState, config change constants
├── utils.py          # MutableInt, helpers
└── protos/
    └── raft.proto    # Protobuf service & message definitions
```

### Key Abstractions

| Interface | Purpose | Implementations |
|-----------|---------|-----------------|
| `StateMachine` | Apply committed commands | `KeyValueStateMachine` |
| `Storage` | Persist term, vote, log, snapshots | `MemoryStorage`, `SQLiteStorage` |
| `AbstractRaftServer` | Receive RPCs | `GrpcRaftServer` |
| `AbstractRaftClient` | Send RPCs | `GrpcRaftClient` |

## RPC Methods

| RPC | Description |
|-----|-------------|
| `AppendEntries` | Log replication and heartbeats |
| `RequestVote` | Leader election |
| `InstallSnapshot` | Snapshot transfer to far-behind followers |
| `ClientRequest` | Client command submission with leader redirect |

## Configuration

### Raft Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `snapshot_threshold` | 1000 | Log entries before automatic compaction |
| Heartbeat interval | 100ms | Leader heartbeat frequency |
| Election timeout | 150-300ms | Randomized per the Raft paper |

### Cluster Membership

```python
# Add a server (leader only)
success, error = await raft.add_server("127.0.0.1:50054")

# Remove a server (leader only)
success, error = await raft.remove_server("127.0.0.1:50054")
```

Single-server changes only — one add/remove at a time to guarantee safety.

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run all checks
uv run ruff check .          # lint
uv run ruff format --check . # format
uv run mypy aioraft/         # typecheck
uv run pytest tests/         # test (121 tests)
```

## References

- [In Search of an Understandable Consensus Algorithm (Raft paper)](https://raft.github.io/raft.pdf)
- [Raft PhD Dissertation](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [HashiCorp Raft Membership](https://github.com/hashicorp/raft/blob/main/membership.md)

## License

Apache License 2.0
