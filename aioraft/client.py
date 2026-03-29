import abc
import asyncio
import logging
from collections.abc import Iterable

import grpc

from aioraft.protos import raft_pb2, raft_pb2_grpc
from aioraft.types import RaftId

log = logging.getLogger(__name__)


class AbstractRaftClient(abc.ABC):
    @abc.abstractmethod
    async def install_snapshot(
        self,
        *,
        to: str,
        term: int,
        leader_id: RaftId,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
    ) -> tuple[int]:
        """Send InstallSnapshot RPC to a follower.

        Returns
        -------
        :param int term: follower's currentTerm
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def append_entries(
        self,
        *,
        to: str,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
    ) -> tuple[int, bool]:
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
    async def request_vote(
        self,
        *,
        to: str,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> tuple[int, bool]:
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

    async def close(self) -> None:
        """Close any resources held by the client. Default is a no-op."""


class GrpcRaftClient(AbstractRaftClient):
    """
    A gRPC-based implementation of `AbstractRaftClient` with connection pooling.
    """

    def __init__(self, credentials: grpc.ChannelCredentials | None = None):
        self.__credentials: grpc.ChannelCredentials | None = credentials
        self._channels: dict[str, grpc.aio.Channel] = {}

    def _get_channel(self, target: str) -> grpc.aio.Channel:
        """Return a cached channel for *target*, creating one if needed."""
        if target not in self._channels:
            self._channels[target] = self._create_channel(target)
        return self._channels[target]

    def _create_channel(self, target: str) -> grpc.aio.Channel:
        if credentials := self.__credentials:
            return grpc.aio.secure_channel(target, credentials)
        return grpc.aio.insecure_channel(target)

    def _invalidate_channel(self, target: str) -> None:
        """Remove a channel from the cache so the next call creates a fresh one."""
        channel = self._channels.pop(target, None)
        if channel is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(channel.close())
            except RuntimeError:
                pass

    async def close(self) -> None:
        """Close all cached channels."""
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()

    async def append_entries(
        self,
        *,
        to: str,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
        timeout: float = 5.0,
    ) -> tuple[int, bool]:
        request = raft_pb2.AppendEntriesRequest(
            term=term,
            leader_id=leader_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=leader_commit,
        )
        for attempt in range(2):
            channel = self._get_channel(to)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = await asyncio.wait_for(stub.AppendEntries(request), timeout=timeout)
                return response.term, response.success
            except grpc.aio.AioRpcError:
                if attempt == 0:
                    log.debug("Connection error to %s, retrying with fresh channel", to)
                    self._invalidate_channel(to)
                    continue
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:
                raise
            break
        return term, False

    async def request_vote(
        self,
        *,
        to: str,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
        timeout: float = 5.0,
    ) -> tuple[int, bool]:
        request = raft_pb2.RequestVoteRequest(
            term=term,
            candidate_id=candidate_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )
        for attempt in range(2):
            channel = self._get_channel(to)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = await asyncio.wait_for(stub.RequestVote(request), timeout=timeout)
                return response.term, response.vote_granted
            except grpc.aio.AioRpcError:
                if attempt == 0:
                    log.debug("Connection error to %s, retrying with fresh channel", to)
                    self._invalidate_channel(to)
                    continue
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:
                raise
            break
        return term, False

    async def install_snapshot(
        self,
        *,
        to: str,
        term: int,
        leader_id: RaftId,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
        timeout: float = 5.0,
    ) -> tuple[int]:
        request = raft_pb2.InstallSnapshotRequest(
            term=term,
            leader_id=leader_id,
            last_included_index=last_included_index,
            last_included_term=last_included_term,
            data=data,
        )
        for attempt in range(2):
            channel = self._get_channel(to)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response = await asyncio.wait_for(stub.InstallSnapshot(request), timeout=timeout)
                return (response.term,)
            except grpc.aio.AioRpcError:
                if attempt == 0:
                    log.debug("Connection error to %s, retrying with fresh channel", to)
                    self._invalidate_channel(to)
                    continue
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:
                raise
            break
        return (term,)
