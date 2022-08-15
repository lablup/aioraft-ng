import asyncio
import random
import uuid
from pathlib import Path

import grpc
import tomli

from raft.aio.clients import GrpcRaftClient
from raft.protos import raft_pb2

"""
async def send_request(peer: str, id: str, command: str) -> Tuple[bool, Optional[str]]:
    async with grpc.aio.insecure_channel(peer) as channel:
        stub = raft_pb2_grpc.CommandServiceStub(channel)
        request = raft_pb2.CommandRequest(id=id, command=command)
        try:
            response = await stub.Command(request)
            print(f"({peer}) response(success={response.success}, redirect={response.redirect})")
            return response.success, response.redirect
        except grpc.aio.AioRpcError as e:
            raise e
"""


def load_config():
    path = Path(__file__).parent.parent / "config.toml"
    return tomli.loads(path.read_text())


async def main():
    config = load_config()
    configuration = config["raft"]["configuration"]

    client = GrpcRaftClient()
    leader = random.choice(configuration)

    client_id = str(uuid.uuid4())
    sequence_num = 0

    while True:
        while command := input("(redis) > "):
            success = False
            sequence_num += 1
            while not success:
                try:
                    response = await client.client_request(
                        to=leader,
                        client_id=client_id,
                        sequence_num=sequence_num,
                        command=command,
                    )
                except grpc.aio.AioRpcError:
                    leader = random.choice(configuration)
                match response.status:
                    case raft_pb2.RaftClusterStatus.OK:
                        print(f"RaftClusterStatus.OK: {response.response}")
                        success = True
                    case raft_pb2.RaftClusterStatus.NOT_LEADER:
                        print(f"RaftClusterStatus.NOT_LEADER: {response.leader_hint}")
                        leader = response.leader_hint
                    case raft_pb2.RaftClusterStatus.SESSION_EXPIRED:
                        print("RaftClusterStatus.SESSION_EXPIRED")
                        return


if __name__ == "__main__":
    asyncio.run(main())
