"""Benchmarks for aioraft-ng core operations.

Run: uv run python benchmarks/bench_raft.py
"""

import asyncio
import logging
import statistics
import time
from unittest.mock import AsyncMock, MagicMock

from aioraft import KeyValueStateMachine, Raft
from aioraft.protos import raft_pb2
from aioraft.types import RaftState

# Number of trials for multi-sample benchmarks
NUM_TRIALS = 3

# Warmup ratio (10% of iterations)
WARMUP_RATIO = 0.10


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


def _print_multi_trial(label: str, values: list[float], unit: str = "ops/sec") -> None:
    """Print mean +/- stdev for a list of trial results."""
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    print(f"{label}  {mean:>12,.0f} {unit}  (stdev={stdev:,.0f})")


async def bench_write_throughput(n: int = 10000) -> None:
    """Benchmark: leader appending log entries."""
    warmup = int(n * WARMUP_RATIO)
    trial_results = []

    for _ in range(NUM_TRIALS):
        raft = await _make_raft()
        _make_leader(raft)

        # Warmup phase (untimed)
        for i in range(warmup):
            await raft._append_entry(f"SET wkey{i} wval{i}")

        # Timed phase
        start = time.perf_counter()
        for i in range(n):
            await raft._append_entry(f"SET key{i} value{i}")
        elapsed = time.perf_counter() - start

        trial_results.append(n / elapsed)

    _print_multi_trial("Write throughput (append):   ", trial_results)


async def bench_append_entries_throughput(n: int = 10000) -> None:
    """Benchmark: follower processing AppendEntries RPCs."""
    warmup = int(n * WARMUP_RATIO)
    trial_results = []

    for _ in range(NUM_TRIALS):
        raft = await _make_raft()
        raft._Raft__current_term.set(1)

        # Warmup phase (untimed)
        for i in range(warmup):
            prev_index = i
            prev_term = 1 if i > 0 else 0
            entry = raft_pb2.Log(index=i + 1, term=1, command=f"SET wkey{i} wval{i}")
            await raft.on_append_entries(
                term=1,
                leader_id="leader-1",
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=[entry],
                leader_commit=0,
            )

        # Timed phase — continue index numbering after warmup
        start = time.perf_counter()
        for i in range(n):
            idx = warmup + i
            prev_index = idx
            prev_term = 1 if idx > 0 else 0
            entry = raft_pb2.Log(index=idx + 1, term=1, command=f"SET key{i} val{i}")
            await raft.on_append_entries(
                term=1,
                leader_id="leader-1",
                prev_log_index=prev_index,
                prev_log_term=prev_term,
                entries=[entry],
                leader_commit=0,
            )
        elapsed = time.perf_counter() - start

        trial_results.append(n / elapsed)

    _print_multi_trial("AppendEntries handler:       ", trial_results)


async def bench_client_request_latency(n: int = 100) -> None:
    """Benchmark: full client request path with mocked replication.

    Instead of relying on a background heartbeat (which measures sleep jitter),
    we mock append_entries to synchronously advance matchIndex and commitIndex
    so _wait_for_commit returns immediately.
    """
    warmup = int(n * WARMUP_RATIO)
    trial_results: list[list[float]] = []

    for _ in range(NUM_TRIALS):
        raft = await _make_raft()
        _make_leader(raft)

        # Mock append_entries to auto-advance matchIndex and commitIndex
        async def fake_append_entries(
            to,
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            _raft=raft,
        ):
            # Simulate successful replication: advance matchIndex for this peer
            if entries:
                last_entry_index = entries[-1].index
            else:
                last_entry_index = prev_log_index
            _raft._Raft__next_index[to] = last_entry_index + 1
            _raft._Raft__match_index[to] = last_entry_index
            return (term, True)

        mock_client = raft._Raft__client
        mock_client.append_entries = AsyncMock(side_effect=fake_append_entries)

        # Helper: issue request and concurrently run a heartbeat so that
        # _wait_for_commit resolves without sleeping. The heartbeat task
        # calls _publish_heartbeat which invokes fake_append_entries
        # (updating matchIndex) then _update_leader_commit_index (advancing
        # commitIndex and setting __commit_event).
        async def do_request(cmd: str, _raft=raft):
            hb = asyncio.create_task(_raft._publish_heartbeat())
            result = await _raft.on_client_request(cmd)
            await hb
            return result

        # Warmup phase (untimed)
        for i in range(warmup):
            await do_request(f"SET wkey{i} wval{i}")

        # Timed phase
        latencies = []
        for i in range(n):
            t0 = time.perf_counter()
            await do_request(f"SET key{i} value{i}")
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)  # ms

        trial_results.append(latencies)

    # Aggregate across trials
    all_latencies = [lat for trial in trial_results for lat in trial]
    all_latencies.sort()
    p50 = all_latencies[int(len(all_latencies) * 0.50)]
    p95 = all_latencies[int(len(all_latencies) * 0.95)]
    p99 = all_latencies[int(len(all_latencies) * 0.99)]
    mean_lat = statistics.mean(all_latencies)
    stdev_lat = statistics.stdev(all_latencies) if len(all_latencies) > 1 else 0.0
    print(
        f"Client request latency:       "
        f"p50={p50:.2f}ms  p95={p95:.2f}ms  p99={p99:.2f}ms  "
        f"mean={mean_lat:.2f}ms  stdev={stdev_lat:.2f}ms"
    )


async def bench_read_throughput(n: int = 10000) -> None:
    """Benchmark: state machine read (GET via apply) throughput."""
    warmup = int(n * WARMUP_RATIO)
    trial_results = []

    for _ in range(NUM_TRIALS):
        sm = KeyValueStateMachine()

        # Pre-populate state machine
        for i in range(100):
            await sm.apply(f"SET key{i} value{i}")

        # Warmup phase (untimed)
        for i in range(warmup):
            await sm.apply(f"GET key{i % 100}")

        # Timed phase
        start = time.perf_counter()
        for i in range(n):
            await sm.apply(f"GET key{i % 100}")
        elapsed = time.perf_counter() - start

        trial_results.append(n / elapsed)

    _print_multi_trial("Read throughput (GET apply): ", trial_results)


async def bench_election_time(trials: int = 100) -> None:
    """Benchmark: time from candidate state to leader election."""
    warmup = int(trials * WARMUP_RATIO)
    times_ms = []

    for trial_idx in range(warmup + trials):
        raft = await _make_raft()
        mock_client = raft._Raft__client
        # Mock request_vote to return (term, True) - votes granted
        mock_client.request_vote = AsyncMock(return_value=(1, True))

        raft._Raft__state = RaftState.CANDIDATE

        t0 = time.perf_counter()
        await raft._start_election()
        t1 = time.perf_counter()

        assert raft.has_leadership(), "Election did not succeed"

        # Only record after warmup
        if trial_idx >= warmup:
            times_ms.append((t1 - t0) * 1000)

    mean = statistics.mean(times_ms)
    stdev = statistics.stdev(times_ms) if len(times_ms) > 1 else 0.0
    mn = min(times_ms)
    mx = max(times_ms)
    p99 = sorted(times_ms)[int(len(times_ms) * 0.99)]
    print(
        f"Election time:                "
        f"mean={mean:.2f}ms  stdev={stdev:.2f}ms  "
        f"min={mn:.2f}ms  max={mx:.2f}ms  p99={p99:.2f}ms"
    )


async def bench_replicate_to_peer(n: int = 5000) -> None:
    """Benchmark: leader replicating entries to a single peer.

    Uses a small batch size (nextIndex increments by BATCH_SIZE each call)
    to measure per-call replication cost rather than sending all entries
    in one RPC.
    """
    BATCH_SIZE = 50
    warmup_entries = int(n * WARMUP_RATIO)
    trial_results = []

    for _ in range(NUM_TRIALS):
        raft = await _make_raft(peers=["node-2"])
        _make_leader(raft)

        # Pre-populate the log (warmup entries + benchmark entries)
        total_entries = warmup_entries + n
        for i in range(total_entries):
            await raft._append_entry(f"SET key{i} value{i}")

        mock_client = raft._Raft__client
        mock_client.append_entries = AsyncMock(return_value=(1, True))

        # Warmup: replicate warmup_entries in batches (untimed)
        raft._Raft__next_index["node-2"] = 1
        raft._Raft__match_index["node-2"] = 0
        warmup_target = warmup_entries
        while raft._Raft__next_index.get("node-2", 1) <= warmup_target:
            await raft._replicate_to_peer("node-2")
            current_next = raft._Raft__next_index.get("node-2", 1)
            new_next = min(current_next + BATCH_SIZE, warmup_target + 1)
            raft._Raft__next_index["node-2"] = new_next
            raft._Raft__match_index["node-2"] = new_next - 1

        # Timed phase: replicate remaining n entries in BATCH_SIZE increments
        raft._Raft__next_index["node-2"] = warmup_entries + 1
        raft._Raft__match_index["node-2"] = warmup_entries
        rpc_count = 0

        start = time.perf_counter()
        while raft._Raft__next_index.get("node-2", 1) <= total_entries:
            await raft._replicate_to_peer("node-2")
            rpc_count += 1
            # Advance nextIndex by BATCH_SIZE to simulate incremental replication
            current_next = raft._Raft__next_index.get("node-2", 1)
            new_next = min(current_next + BATCH_SIZE, total_entries + 1)
            raft._Raft__next_index["node-2"] = new_next
            raft._Raft__match_index["node-2"] = new_next - 1
        elapsed = time.perf_counter() - start

        trial_results.append(n / elapsed)

    _print_multi_trial(
        "Replicate to peer:           ",
        trial_results,
        unit=f"entries/sec  (batch_size={BATCH_SIZE})",
    )


async def bench_state_machine_apply(n: int = 50000) -> None:
    """Benchmark: KeyValueStateMachine.apply throughput."""
    warmup = int(n * WARMUP_RATIO)
    trial_results = []

    for _ in range(NUM_TRIALS):
        sm = KeyValueStateMachine()

        # Warmup phase (untimed)
        for i in range(warmup):
            await sm.apply(f"SET wkey{i} wval{i}")

        # Timed phase
        start = time.perf_counter()
        for i in range(n):
            await sm.apply(f"SET key{i} value{i}")
        elapsed = time.perf_counter() - start

        trial_results.append(n / elapsed)

    _print_multi_trial("State machine apply:         ", trial_results)


async def main() -> None:
    print("aioraft-ng benchmarks")
    print("=" * 60)
    print("NOTE: All benchmarks use mocked networking — no real I/O.")
    print("      Numbers reflect pure in-memory Python logic only.")
    print("=" * 60)
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
    previous_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(main())
    finally:
        logging.disable(previous_level)
