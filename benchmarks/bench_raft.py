"""Benchmarks for aioraft-ng core operations.

Run: uv run python benchmarks/bench_raft.py
"""

import asyncio
import contextlib
import logging
import statistics
import time
from unittest.mock import AsyncMock, MagicMock

from aioraft import KeyValueStateMachine, Raft
from aioraft.protos import raft_pb2
from aioraft.types import RaftState

# Suppress Raft internal logging during benchmarks
logging.disable(logging.CRITICAL)


async def _make_raft(
    node_id: str = "node-1",
    peers: list[str] | None = None,
    state_machine: KeyValueStateMachine | None = None,
) -> Raft:
    """Create a Raft node with mocked server/client for benchmarking."""
    if peers is None:
        peers = ["node-2", "node-3"]
    mock_server = MagicMock()
    mock_server.bind = MagicMock()
    mock_client = AsyncMock()
    raft = await Raft.new(
        node_id,
        server=mock_server,
        client=mock_client,
        configuration=peers,
        state_machine=state_machine,
    )
    return raft


def _make_leader(raft: Raft) -> None:
    """Force a Raft node into LEADER state and initialize leader volatile state."""
    raft._Raft__state = RaftState.LEADER
    raft._Raft__current_term.set(1)
    peers = list(raft._Raft__configuration)
    last_log_index = raft._Raft__last_included_index + len(raft._Raft__log)
    raft._Raft__next_index = {peer: last_log_index + 1 for peer in peers}
    raft._Raft__match_index = {peer: 0 for peer in peers}


async def bench_write_throughput(n: int = 10000) -> None:
    """Benchmark: leader appending log entries."""
    raft = await _make_raft()
    _make_leader(raft)

    start = time.perf_counter()
    for i in range(n):
        await raft._append_entry(f"SET key{i} value{i}")
    elapsed = time.perf_counter() - start

    ops_sec = n / elapsed
    print(f"Write throughput (append):    {ops_sec:>12,.0f} ops/sec")


async def bench_append_entries_throughput(n: int = 10000) -> None:
    """Benchmark: follower processing AppendEntries RPCs."""
    raft = await _make_raft()
    raft._Raft__current_term.set(1)

    start = time.perf_counter()
    for i in range(n):
        prev_index = i
        prev_term = 1 if i > 0 else 0
        entry = raft_pb2.Log(index=i + 1, term=1, command=f"SET key{i} val{i}")
        await raft.on_append_entries(
            term=1,
            leader_id="leader-1",
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=[entry],
            leader_commit=0,
        )
    elapsed = time.perf_counter() - start

    ops_sec = n / elapsed
    print(f"AppendEntries handler:        {ops_sec:>12,.0f} ops/sec")


async def bench_client_request_latency(n: int = 1000) -> None:
    """Benchmark: full client request path with mocked replication."""
    raft = await _make_raft()
    _make_leader(raft)

    mock_client = raft._Raft__client

    # Mock append_entries to return success
    async def fake_append_entries(to, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        return (term, True)

    mock_client.append_entries = AsyncMock(side_effect=fake_append_entries)

    # Background heartbeat task that replicates and advances commit
    stop_event = asyncio.Event()

    async def heartbeat_loop():
        while not stop_event.is_set():
            await raft._publish_heartbeat()
            await asyncio.sleep(0.001)

    hb_task = asyncio.create_task(heartbeat_loop())

    latencies = []
    for i in range(n):
        t0 = time.perf_counter()
        await raft.on_client_request(f"SET key{i} value{i}")
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)  # ms

    stop_event.set()
    hb_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await hb_task

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    print(f"Client request latency:       p50={p50:.2f}ms  p95={p95:.2f}ms  p99={p99:.2f}ms")


async def bench_read_throughput(n: int = 10000) -> None:
    """Benchmark: state machine read (GET via apply) throughput.

    Since the committed codebase does not have a dedicated read-only path,
    this measures how fast the state machine can serve GET commands through
    the standard apply interface.
    """
    sm = KeyValueStateMachine()

    # Pre-populate state machine
    for i in range(100):
        await sm.apply(f"SET key{i} value{i}")

    start = time.perf_counter()
    for i in range(n):
        await sm.apply(f"GET key{i % 100}")
    elapsed = time.perf_counter() - start

    ops_sec = n / elapsed
    print(f"Read throughput (GET apply):  {ops_sec:>12,.0f} ops/sec")


async def bench_election_time(trials: int = 20) -> None:
    """Benchmark: time from candidate state to leader election."""
    times_ms = []

    for _ in range(trials):
        raft = await _make_raft()
        mock_client = raft._Raft__client
        # Mock request_vote to return (term, True) - votes granted
        mock_client.request_vote = AsyncMock(return_value=(1, True))

        raft._Raft__state = RaftState.CANDIDATE

        t0 = time.perf_counter()
        await raft._start_election()
        t1 = time.perf_counter()

        assert raft.has_leadership(), "Election did not succeed"
        times_ms.append((t1 - t0) * 1000)

    mean = statistics.mean(times_ms)
    mn = min(times_ms)
    mx = max(times_ms)
    p99 = sorted(times_ms)[int(len(times_ms) * 0.99)]
    print(f"Election time:                mean={mean:.2f}ms  min={mn:.2f}ms  max={mx:.2f}ms  p99={p99:.2f}ms")


async def bench_replicate_to_peer(n: int = 5000) -> None:
    """Benchmark: leader replicating entries to a single peer."""
    raft = await _make_raft(peers=["node-2"])
    _make_leader(raft)

    # Pre-populate the log
    for i in range(n):
        await raft._append_entry(f"SET key{i} value{i}")

    mock_client = raft._Raft__client
    mock_client.append_entries = AsyncMock(return_value=(1, True))

    # Set nextIndex for the peer to 1 so it needs to catch up
    raft._Raft__next_index["node-2"] = 1
    raft._Raft__match_index["node-2"] = 0

    start = time.perf_counter()
    count = 0
    # _replicate_to_peer sends all entries from nextIndex, and _publish_heartbeat
    # updates nextIndex/matchIndex on success. We call _replicate_to_peer directly
    # and manually advance nextIndex since _replicate_to_peer itself doesn't.
    while raft._Raft__next_index.get("node-2", 1) <= n:
        term, success = await raft._replicate_to_peer("node-2")
        if success:
            # _replicate_to_peer sends all from nextIndex, so after success
            # advance to end of log
            last_log_index = raft._Raft__last_included_index + len(raft._Raft__log)
            raft._Raft__next_index["node-2"] = last_log_index + 1
            raft._Raft__match_index["node-2"] = last_log_index
        count += 1
    elapsed = time.perf_counter() - start

    # Report based on entries replicated
    ops_sec = n / elapsed
    print(f"Replicate to peer:           {ops_sec:>12,.0f} entries/sec  ({count} RPCs)")


async def bench_state_machine_apply(n: int = 50000) -> None:
    """Benchmark: KeyValueStateMachine.apply throughput."""
    sm = KeyValueStateMachine()

    start = time.perf_counter()
    for i in range(n):
        await sm.apply(f"SET key{i} value{i}")
    elapsed = time.perf_counter() - start

    ops_sec = n / elapsed
    print(f"State machine apply:         {ops_sec:>12,.0f} ops/sec")


async def main() -> None:
    print("aioraft-ng benchmarks")
    print("=" * 50)
    print()
    await bench_write_throughput()
    await bench_append_entries_throughput()
    await bench_client_request_latency()
    await bench_read_throughput()
    await bench_election_time()
    await bench_replicate_to_peer()
    await bench_state_machine_apply()
    print()


if __name__ == "__main__":
    asyncio.run(main())
