import abc
from typing import Coroutine, List, Optional

import grpc

from raft.aio.protocols import AbstractRaftClusterProtocol, AbstractRaftProtocol
from raft.protos import raft_pb2, raft_pb2_grpc


class AbstractRaftServer(abc.ABC):
    @abc.abstractmethod
    def bind(
        self,
        raft_protocol: AbstractRaftProtocol,
        raft_cluster_protocol: AbstractRaftClusterProtocol,
    ):
        raise NotImplementedError()


class GrpcRaftServer(
    AbstractRaftServer,
    raft_pb2_grpc.RaftServiceServicer,
    raft_pb2_grpc.RaftClusterServiceServicer,
):
    """
    A gRPC-based implementation of `AbstractRaftServer`.
    """

    def __init__(self, credentials: Optional[grpc.ServerCredentials] = None):
        self.__raft_protocol: Optional[AbstractRaftProtocol] = None
        self.__raft_cluster_protocol: Optional[AbstractRaftClusterProtocol] = None
        self.__credentials: Optional[grpc.ServerCredentials] = credentials

    def bind(
        self,
        raft_protocol: AbstractRaftProtocol,
        raft_cluster_protocol: AbstractRaftClusterProtocol,
    ):
        self.__raft_protocol = raft_protocol
        self.__raft_cluster_protocol = raft_cluster_protocol

    async def run(
        self,
        host: str = "[::]",
        port: int = 50051,
        cleanup_coroutines: Optional[List[Coroutine]] = None,
    ):
        server = grpc.aio.server()
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        raft_pb2_grpc.add_RaftClusterServiceServicer_to_server(self, server)

        if credentials := self.__credentials:
            server.add_secure_port(f"{host}:{port}", credentials)
        else:
            server.add_insecure_port(f"{host}:{port}")

        async def server_graceful_shutdown():
            await server.stop(5)

        if cleanup_coroutines is not None:
            cleanup_coroutines.append(server_graceful_shutdown())

        await server.start()
        await server.wait_for_termination()

    """
    raft_pb2_grpc.RaftServiceServicer
    """

    async def AppendEntries(
        self,
        request: raft_pb2.AppendEntriesRequest,
        context: grpc.aio.ServicerContext,
    ) -> raft_pb2.AppendEntriesResponse:
        if (protocol := self.__raft_protocol) is None:
            return raft_pb2.AppendEntriesResponse(term=request.term, success=False)
        response = await protocol.on_append_entries(
            term=request.term,
            leader_id=request.leader_id,
            prev_log_index=request.prev_log_index,
            prev_log_term=request.prev_log_term,
            entries=request.entries,
            leader_commit=request.leader_commit,
        )
        return raft_pb2.AppendEntriesResponse(
            term=response.term, success=response.success
        )

    async def RequestVote(
        self,
        request: raft_pb2.RequestVoteRequest,
        context: grpc.aio.ServicerContext,
    ) -> raft_pb2.RequestVoteResponse:
        if (protocol := self.__raft_protocol) is None:
            return raft_pb2.RequestVoteResponse(term=request.term, vote_granted=False)
        term, vote_granted = await protocol.on_request_vote(
            term=request.term,
            candidate_id=request.candidate_id,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
        )
        return raft_pb2.RequestVoteResponse(term=term, vote_granted=vote_granted)

    """
    raft_pb2_grpc.RaftClusterServiceServicer
    """

    async def ClientRequest(
        self,
        request: raft_pb2.ClientRequestRequest,
        context: grpc.aio.ServicerContext,
    ) -> raft_pb2.ClientRequestResponse:
        if (protocol := self.__raft_cluster_protocol) is None:
            return raft_pb2.ClientRequestResponse(
                status=raft_pb2.RaftClusterStatus.NOT_LEADER, leader_hint=None  # type: ignore
            )
        response = await protocol.on_client_request(
            client_id=request.client_id,
            sequence_num=request.sequence_num,
            command=request.command,
        )
        return raft_pb2.ClientRequestResponse(
            status=response.status.value,  # type: ignore
            response=response.response,
            leader_hint=response.leader_hint,
        )

    async def RegisterClient(
        self,
        request: raft_pb2.RegisterClientRequest,
        context: grpc.aio.ServicerContext,
    ) -> raft_pb2.RegisterClientResponse:
        pass

    async def ClientQuery(
        self, request: raft_pb2.ClientQueryRequest, context: grpc.aio.ServicerContext
    ) -> raft_pb2.ClientQueryResponse:
        if (protocol := self.__raft_cluster_protocol) is None:
            return raft_pb2.ClientQueryResponse(
                status=raft_pb2.RaftClusterStatus.NOT_LEADER, leader_hint=None  # type: ignore
            )
        response = await protocol.on_client_query(query=request.query)
        return raft_pb2.ClientQueryResponse(
            status=response.status.value,   # type: ignore
            response=response.response,
            leader_hint=response.leader_hint,
        )
