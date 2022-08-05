import abc
from typing import Iterable, Optional, Tuple

import grpc

from raft.protos import raft_pb2, raft_pb2_grpc
from raft.types import RaftId


class AbstractRaftClient(abc.ABC):
    @abc.abstractmethod
    def append_entries(
        self,
        *,
        to: str,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
    ) -> Tuple[int, bool]:
        """Invoked by leader to replicate log entries; also used as heartbeat.

        Arguments
        ---------
        :param str to: follower's IP address with port (e.g. "127.0.0.1:50051")
        :param int term: leader's term
        :param raft.types.RaftId leader_id: so follower can redirect clients
        :param int prev_log_index: index of log entry immediately preceding new ones
        :param int prev_log_term: term of prevLogIndex entry
        :param Iterable[raft.protos.raft_pb2.Log] entries: log entries to store
            (empty for heartbeat; may send more than one for efficiency)
        :param int leader_commit: leader's commitIndex
        ---------

        Returns
        -------
        :param int term: follower's currentTerm, for leader to update itself
        :param bool success: true if follower contained entry matching prevLogIndex and prevLogTerm
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def request_vote(
        self,
        *,
        to: str,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        """Invoked by candidates to gather votes.

        Arguments
        ---------
        :param str to: follower's IP address with port (e.g. "127.0.0.1:50051")
        :param int term: candidate's term
        :param raft.types.RaftId candidate_id: candidate requesting vote
        :param int last_log_index: index of candidate's last log entry
        :param int last_log_term: term of candidate's last log entry
        ---------

        Returns
        -------
        :param int term: follower's currentTerm, for candidate to update itself
        :param bool vote_granted: true means candidate received vote
        -------
        """
        raise NotImplementedError()


class GrpcRaftClient(AbstractRaftClient):
    """
    A gRPC-based implementation of `AbstractRaftClient`.
    """

    def __init__(self, credentials: Optional[grpc.ChannelCredentials] = None):
        self.__credentials: Optional[grpc.ChannelCredentials] = credentials

    def append_entries(
        self,
        *,
        to: str,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
    ) -> Tuple[int, bool]:
        request = raft_pb2.AppendEntriesRequest(
            term=term,
            leader_id=leader_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=leader_commit,
        )
        with self.__create_channel(to) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = stub.AppendEntries(request)
                return response.term, response.success
            except grpc.RpcError:
                pass
            return term, False

    def request_vote(
        self,
        *,
        to: str,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        request = raft_pb2.RequestVoteRequest(
            term=term,
            candidate_id=candidate_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )
        with self.__create_channel(to) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = stub.RequestVote(request)
                return response.term, response.vote_granted
            except grpc.RpcError:
                pass
            return term, False

    def __create_channel(self, target: str) -> grpc.Channel:
        if credentials := self.__credentials:
            return grpc.secure_channel(target, credentials)
        return grpc.insecure_channel(target)
