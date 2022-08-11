import abc
from typing import Optional

import grpc

from raft.protos import raft_pb2, raft_pb2_grpc
from raft.types import ClientQueryResponse, ClientRequestResponse, RegisterClientResponse


class AbstractRaftClient(abc.ABC):
    @abc.abstractmethod
    async def client_request(
        self, to: str, client_id: str, sequence_num: int, command: str
    ) -> ClientRequestResponse:
        """Invoked by clients to modify the replicated state.

        Arguments
        ---------
        :param str to: follower's IP address with port (e.g. "127.0.0.1:50051")
        :param str client_id: client invoking request
        :param int sequence_num: to eliminate duplicates
        :param str command: request for state machine, may affect state
        ---------

        Returns
        -------
        :param raft.types.RaftClusterStatus status: OK if state machine applied command
        :param str response: state machine output, if successful
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def register_client(self, to: str) -> RegisterClientResponse:
        """Invoked by new clients to open new session, used to eliminate duplicate requests.

        Returns
        -------
        :param str to: follower's IP address with port (e.g. "127.0.0.1:50051")
        :param raft.types.RaftClusterStatus status: OK if state machine registered client
        :param str client_id: unique identifier for client session
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def client_query(self, to: str, query: str) -> ClientQueryResponse:
        """Invoked by clients to query the replicated state (read-only commands)

        Arguments
        ---------
        :param str to: follower's IP address with port (e.g. "127.0.0.1:50051")
        :param str query: request for state machine, read-only
        ---------

        Returns
        -------
        :param raft.types.RaftClusterStatus status: OK if state machine processed query
        :param str response: state machine output, if successful
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError()


class GrpcRaftClient(AbstractRaftClient):
    def __init__(self, credentials: Optional[grpc.ChannelCredentials] = None):
        self.__credentials: Optional[grpc.ChannelCredentials] = credentials

    async def client_request(
        self, to: str, client_id: str, sequence_num: int, command: str
    ) -> ClientRequestResponse:
        request = raft_pb2.ClientRequestRequest(
            client_id=client_id, sequence_num=sequence_num, command=command
        )
        async with self.__create_channel(to) as channel:
            stub = raft_pb2_grpc.RaftClusterServiceStub(channel)
            response = await stub.ClientRequest(request)
            return ClientRequestResponse(
                status=response.status,
                response=response.response,
                leader_hint=response.leader_hint,
            )

    async def register_client(self, to: str) -> RegisterClientResponse:
        request = raft_pb2.RegisterClientRequest()
        async with self.__create_channel(to) as channel:
            stub = raft_pb2_grpc.RaftClusterServiceStub(channel)
            response = await stub.RegisterClient(request)
            return RegisterClientResponse(
                status=response.status,
                client_id=response.client_id,
                leader_hint=response.leader_hint,
            )

    async def client_query(self, to: str, query: str) -> ClientQueryResponse:
        request = raft_pb2.ClientQueryRequest(query=query)
        async with self.__create_channel(to) as channel:
            stub = raft_pb2_grpc.RaftClusterServiceStub(channel)
            response = await stub.ClientQuery(request)
            return ClientQueryResponse(
                status=response.status,
                response=response.response,
                leader_hint=response.leader_hint,
            )

    def __create_channel(self, target: str) -> grpc.aio.Channel:
        if credentials := self.__credentials:
            return grpc.aio.secure_channel(target, credentials)
        return grpc.aio.insecure_channel(target)
