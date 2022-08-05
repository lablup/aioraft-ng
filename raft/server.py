import abc
from typing import Optional

import grpc

from raft.protocol import AbstractRaftProtocol
from raft.protos import raft_pb2, raft_pb2_grpc


class AbstractRaftServer(abc.ABC):
    @abc.abstractmethod
    def bind(self, protocol: AbstractRaftProtocol):
        raise NotImplementedError()


class GrpcRaftServer(AbstractRaftServer, raft_pb2_grpc.RaftServiceServicer):
    """
    A gRPC-based implementation of `AbstractRaftServer`.
    """

    def __init__(self, credentials: Optional[grpc.ServerCredentials] = None):
        self.__protocol: Optional[AbstractRaftProtocol] = None
        self.__credentials: Optional[grpc.ServerCredentials] = credentials

    def bind(self, protocol: AbstractRaftProtocol):
        self.__protocol = protocol

    def run(self, host: str = "[::]", port: int = 50051):
        server = grpc.server()
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)

        if credentials := self.__credentials:
            server.add_secure_port(f"{host}:{port}", credentials)
        else:
            server.add_insecure_port(f"{host}:{port}")

        server.start()
        server.wait_for_termination()

    """
    raft_pb2_grpc.RaftServiceServicer
    """

    def AppendEntries(
        self,
        request: raft_pb2.AppendEntriesRequest,
        context: grpc.ServicerContext,
    ) -> raft_pb2.AppendEntriesResponse:
        if (protocol := self.__protocol) is None:
            return raft_pb2.AppendEntriesResponse(term=request.term, success=False)
        term, success = protocol.on_append_entries(
            term=request.term,
            leader_id=request.leader_id,
            prev_log_index=request.prev_log_index,
            prev_log_term=request.prev_log_term,
            entries=request.entries,
            leader_commit=request.leader_commit,
        )
        return raft_pb2.AppendEntriesResponse(term=term, success=success)

    def RequestVote(
        self,
        request: raft_pb2.RequestVoteRequest,
        context: grpc.ServicerContext,
    ) -> raft_pb2.RequestVoteResponse:
        if (protocol := self.__protocol) is None:
            return raft_pb2.RequestVoteResponse(term=request.term, vote_granted=False)
        term, vote_granted = protocol.on_request_vote(
            term=request.term,
            candidate_id=request.candidate_id,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
        )
        return raft_pb2.RequestVoteResponse(term=term, vote_granted=vote_granted)
