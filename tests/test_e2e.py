"""End-to-end cluster integration tests for aioraft-ng.

These tests spin up real in-process Raft clusters with gRPC networking
and verify leader election, log replication, read leases, and membership
changes.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from aioraft import KeyValueStateMachine, MemoryStorage, Raft
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer
from aioraft.types import RaftId

log = logging.getLogger(__name__)


class RaftCluster:
    """Helper for managing an in-process Raft cluster for testing."""

    def __init__(self, size: int, base_port: int = 50100):
        self.size = size
        self.base_port = base_port
        self.nodes: list[Raft] = []
        self.servers: list[GrpcRaftServer] = []
        self.tasks: list[asyncio.Task] = []
        self._ports: list[int] = []
        self._addrs: list[str] = []

    async def start(self):
        """Start all nodes."""
        self._ports = list(range(self.base_port, self.base_port + self.size))
        self._addrs = [f"127.0.0.1:{p}" for p in self._ports]

        for i, _port in enumerate(self._ports):
            server = GrpcRaftServer()
            client = GrpcRaftClient()
            sm = KeyValueStateMachine()
            storage = MemoryStorage()
            config = [a for a in self._addrs if a != self._addrs[i]]

            node = await Raft.new(
                self._addrs[i],
                server=server,
                client=client,
                configuration=config,
                state_machine=sm,
                storage=storage,
            )
            self.nodes.append(node)
            self.servers.append(server)

        # Start servers and raft main loops
        for server, port, node in zip(self.servers, self._ports, self.nodes, strict=False):
            self.tasks.append(asyncio.create_task(server.run(host="0.0.0.0", port=port)))
            self.tasks.append(asyncio.create_task(node.main()))

    async def wait_for_leader(self, timeout: float = 10.0) -> Raft | None:
        """Wait until a leader is elected."""
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            for node in self.nodes:
                if node.has_leadership():
                    return node
            await asyncio.sleep(0.1)
        return None

    def get_leader(self) -> Raft | None:
        for node in self.nodes:
            if node.has_leadership():
                return node
        return None

    def get_followers(self) -> list[Raft]:
        return [n for n in self.nodes if not n.has_leadership()]

    def get_main_task_for_node(self, node: Raft) -> asyncio.Task | None:
        """Return the main() task for a given node.

        Tasks are stored in pairs: [server_task, main_task, server_task, main_task, ...].
        """
        try:
            idx = self.nodes.index(node)
        except ValueError:
            return None
        # Each node has 2 tasks: server (idx*2) and main (idx*2+1)
        main_task_idx = idx * 2 + 1
        if main_task_idx < len(self.tasks):
            return self.tasks[main_task_idx]
        return None

    async def add_node(self, port: int) -> Raft:
        """Create and start a new node (does NOT add it to the cluster via Raft)."""
        addr = f"127.0.0.1:{port}"
        server = GrpcRaftServer()
        client = GrpcRaftClient()
        sm = KeyValueStateMachine()
        storage = MemoryStorage()
        # New node starts with all existing addresses as peers
        config = list(self._addrs)

        node = await Raft.new(
            addr,
            server=server,
            client=client,
            configuration=config,
            state_machine=sm,
            storage=storage,
        )
        self.nodes.append(node)
        self.servers.append(server)
        self._ports.append(port)
        self._addrs.append(addr)

        self.tasks.append(asyncio.create_task(server.run(host="0.0.0.0", port=port)))
        self.tasks.append(asyncio.create_task(node.main()))
        return node

    async def stop(self):
        """Stop all tasks."""
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_leader_election_3_nodes():
    """Start a 3-node cluster and verify exactly 1 leader and 2 followers."""
    cluster = RaftCluster(size=3, base_port=50100)
    try:
        await cluster.start()
        leader = await asyncio.wait_for(cluster.wait_for_leader(timeout=10.0), timeout=15.0)
        assert leader is not None, "No leader elected within timeout"

        # Exactly 1 leader
        leaders = [n for n in cluster.nodes if n.has_leadership()]
        assert len(leaders) == 1

        # 2 followers
        followers = cluster.get_followers()
        assert len(followers) == 2

        # All nodes agree on the same term
        leader_term = leader.current_term
        for node in cluster.nodes:
            assert node.current_term == leader_term, f"Term mismatch: leader={leader_term}, node {node.id}={node.current_term}"
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_leader_reelection_after_kill():
    """Kill the leader and verify a new one is elected with a higher term."""
    cluster = RaftCluster(size=3, base_port=50200)
    try:
        await cluster.start()
        old_leader = await asyncio.wait_for(cluster.wait_for_leader(timeout=10.0), timeout=15.0)
        assert old_leader is not None, "No initial leader elected"
        old_term = old_leader.current_term
        old_leader_id = old_leader.id

        # Kill the leader's main task to simulate a crash
        main_task = cluster.get_main_task_for_node(old_leader)
        assert main_task is not None
        main_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await main_task

        # Wait for a new leader among the remaining nodes
        deadline = asyncio.get_running_loop().time() + 10.0
        new_leader = None
        while asyncio.get_running_loop().time() < deadline:
            for node in cluster.nodes:
                if node.has_leadership() and node.id != old_leader_id:
                    new_leader = node
                    break
            if new_leader is not None:
                break
            await asyncio.sleep(0.1)

        assert new_leader is not None, "No new leader elected after killing old leader"
        assert new_leader.current_term > old_term, f"New leader term {new_leader.current_term} should be > old term {old_term}"
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_client_request_committed():
    """Submit a write to the leader and verify it is committed."""
    cluster = RaftCluster(size=3, base_port=50300)
    try:
        await cluster.start()
        leader = await asyncio.wait_for(cluster.wait_for_leader(timeout=10.0), timeout=15.0)
        assert leader is not None

        success, result, hint = await asyncio.wait_for(
            leader.on_client_request("SET key1 value1"),
            timeout=10.0,
        )
        assert success, f"Client request failed: result={result!r}, hint={hint!r}"

        # Wait briefly for replication to propagate
        await asyncio.sleep(1.0)

        # Verify the leader has a commit_index > 0
        assert leader.commit_index > 0, "Leader commit_index should be > 0 after a successful write"

        # Check that all nodes have the entry in their logs (via commit_index)
        for node in cluster.nodes:
            # Access the internal log via name mangling
            node_log = node._Raft__log  # type: ignore[attr-defined]
            assert len(node_log) > 0, f"Node {node.id} has empty log after replication"
            # Verify the SET command is in at least one log entry
            commands = [entry.command for entry in node_log]
            assert any("SET key1 value1" in cmd for cmd in commands), (
                f"Node {node.id} log does not contain 'SET key1 value1': {commands}"
            )
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_read_request_with_lease():
    """Submit a write, then verify a read returns the value via leader lease."""
    cluster = RaftCluster(size=3, base_port=50400)
    try:
        await cluster.start()
        leader = await asyncio.wait_for(cluster.wait_for_leader(timeout=10.0), timeout=15.0)
        assert leader is not None

        # Write a value
        success, result, hint = await asyncio.wait_for(
            leader.on_client_request("SET mykey hello"),
            timeout=10.0,
        )
        assert success, f"Write failed: result={result!r}"

        # Wait for the entry to be applied to the state machine
        deadline = asyncio.get_running_loop().time() + 5.0
        while leader.last_applied < leader.commit_index:
            if asyncio.get_running_loop().time() > deadline:
                break
            await asyncio.sleep(0.05)

        # Also wait for at least one heartbeat ack so the lease is valid
        await asyncio.sleep(0.3)

        # Read via leader lease
        success, result, hint = await asyncio.wait_for(
            leader.on_read_request("GET mykey"),
            timeout=5.0,
        )
        assert success, f"Read request failed: result={result!r}, hint={hint!r}"
        assert result == "hello", f"Expected 'hello', got {result!r}"
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_add_server_to_cluster():
    """Add a 4th node to a 3-node cluster and verify membership grows."""
    cluster = RaftCluster(size=3, base_port=50500)
    try:
        await cluster.start()
        leader = await asyncio.wait_for(cluster.wait_for_leader(timeout=10.0), timeout=15.0)
        assert leader is not None
        assert leader.membership == 3

        # Start a 4th node
        new_port = 50503
        new_addr = f"127.0.0.1:{new_port}"
        await cluster.add_node(new_port)

        # Ask the leader to add the new server
        success, msg = await asyncio.wait_for(
            leader.add_server(RaftId(new_addr)),
            timeout=10.0,
        )
        assert success, f"add_server failed: {msg}"
        assert leader.membership == 4, f"Expected membership=4, got {leader.membership}"
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_remove_server_from_cluster():
    """Remove a follower from a 3-node cluster and verify membership shrinks."""
    cluster = RaftCluster(size=3, base_port=50600)
    try:
        await cluster.start()
        leader = await asyncio.wait_for(cluster.wait_for_leader(timeout=10.0), timeout=15.0)
        assert leader is not None
        assert leader.membership == 3

        # Pick a follower to remove
        followers = cluster.get_followers()
        assert len(followers) > 0
        follower = followers[0]
        follower_addr = follower.id

        # Remove it
        success, msg = await asyncio.wait_for(
            leader.remove_server(RaftId(follower_addr)),
            timeout=10.0,
        )
        assert success, f"remove_server failed: {msg}"
        assert leader.membership == 2, f"Expected membership=2, got {leader.membership}"
    finally:
        await cluster.stop()
