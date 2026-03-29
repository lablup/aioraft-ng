"""
aioraft-ng Example: Full-featured Raft cluster demo

Starts a 3-node in-process cluster and demonstrates:
- Leader election
- Client write requests (SET/GET/DELETE)
- Read-only queries via leader lease
- Dynamic membership (add/remove server)
- Graceful shutdown

Usage:
    uv run python examples/main.py
"""

import asyncio
from contextlib import suppress

from aioraft import KeyValueStateMachine, Raft
from aioraft.client import GrpcRaftClient
from aioraft.server import GrpcRaftServer
from aioraft.storage import MemoryStorage


async def create_node(addr: str, peers: list[str]) -> tuple[Raft, GrpcRaftServer]:
    """Create a Raft node with all features enabled."""
    server = GrpcRaftServer()
    client = GrpcRaftClient()
    state_machine = KeyValueStateMachine()
    storage = MemoryStorage()

    node = await Raft.new(
        addr,
        server=server,
        client=client,
        configuration=peers,
        state_machine=state_machine,
        storage=storage,
    )
    return node, server


async def wait_for_leader(nodes: list[Raft], timeout: float = 10.0) -> Raft | None:
    """Wait until a leader is elected."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        for node in nodes:
            if node.has_leadership():
                return node
        await asyncio.sleep(0.1)
    return None


async def demo(tasks: list[asyncio.Task]) -> None:
    print("=== aioraft-ng Demo ===\n")

    # --- 1. Start a 3-node cluster ---
    print("1. Starting 3-node cluster...")
    addrs = ["127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"]
    nodes: list[Raft] = []
    servers: list[GrpcRaftServer] = []

    for addr in addrs:
        peers = [a for a in addrs if a != addr]
        node, server = await create_node(addr, peers)
        nodes.append(node)
        servers.append(server)

    # Start gRPC servers and Raft main loops
    ports = [50051, 50052, 50053]
    for server, port, node in zip(servers, ports, nodes, strict=True):
        tasks.append(asyncio.create_task(server.run(host="0.0.0.0", port=port)))
        tasks.append(asyncio.create_task(node.main()))

    # --- 2. Wait for leader election ---
    print("2. Waiting for leader election...")
    leader = await wait_for_leader(nodes)
    if leader is None:
        print("   ERROR: No leader elected within timeout!")
        return
    print(f"   Leader elected: {leader.id} (term {leader.current_term})")
    print(f"   Cluster size: {leader.membership} nodes\n")

    # --- 3. Client write requests ---
    print("3. Submitting write requests...")
    commands = [
        "SET name aioraft-ng",
        "SET version 0.2.0",
        "SET language python",
    ]
    for cmd in commands:
        success, result, hint = await leader.on_client_request(cmd)
        status = "OK" if success else f"FAIL ({result})"
        print(f"   {cmd} -> {status}")
    print()

    # Brief pause for replication
    await asyncio.sleep(0.5)

    # --- 4. Read-only queries via leader lease ---
    print("4. Read-only queries (leader lease)...")
    queries = ["GET name", "GET version", "GET language", "GET missing_key"]
    for q in queries:
        success, result, hint = await leader.on_read_request(q)
        if success:
            value = result if result else "(nil)"
            print(f"   {q} -> {value}")
        else:
            print(f"   {q} -> ERROR: {result}")
    print()

    # --- 5. Dynamic membership ---
    print("5. Dynamic membership changes...")

    # Add a 4th node
    new_addr = "127.0.0.1:50054"
    new_peers = list(addrs)  # existing nodes are its peers
    new_node, new_server = await create_node(new_addr, new_peers)
    nodes.append(new_node)
    servers.append(new_server)
    tasks.append(asyncio.create_task(new_server.run(host="0.0.0.0", port=50054)))
    tasks.append(asyncio.create_task(new_node.main()))

    success, err = await leader.add_server(new_addr)
    if success:
        print(f"   Added {new_addr} -> cluster size: {leader.membership}")
    else:
        print(f"   Add failed: {err}")

    # Remove the 4th node
    await asyncio.sleep(0.5)
    success, err = await leader.remove_server(new_addr)
    if success:
        print(f"   Removed {new_addr} -> cluster size: {leader.membership}")
    else:
        print(f"   Remove failed: {err}")
    print()

    # --- 6. Summary ---
    print("6. Cluster state:")
    for node in nodes[:3]:  # original 3
        role = "LEADER" if node.has_leadership() else "FOLLOWER"
        print(f"   {node.id}: {role} (term {node.current_term}, committed={node.commit_index})")
    print()
    print("=== Demo complete ===")


async def main() -> None:
    tasks: list[asyncio.Task] = []
    try:
        await asyncio.wait_for(demo(tasks), timeout=30.0)
    except TimeoutError:
        print("\nDemo timed out!")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
