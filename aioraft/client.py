import abc
import asyncio
from typing import Optional, Tuple

import grpc

from aioraft.protos import raft_pb2, raft_pb2_grpc

# from aioraft.types import RaftId


class AbstractRaftClient(abc.ABC):
    @abc.abstractmethod
    async def client_request(
        self, *, client_id: str, sequence_num: int, command: str
    ) -> Tuple[bool, str, str]:
        """Invoked by clients to modify the replicated state.

        Arguments
        ---------
        :param str client_id: client invoking request
        :param int sequence_num: to eliminate duplicates
        :param str command: request for state machine, may affect state
        ---------

        Returns
        -------
        :param bool status: OK if state machine applied command
        :param str response: state machine output, if successful
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def register_client(self) -> Tuple[bool, str, str]:
        """Invoked by new clients to open new session, used to eliminate duplicate requests.

        Returns
        -------
        :param bool status: OK if state machine registered client
        :param str client_id: unique identifier for client session
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def client_query(self, *, query: str) -> Tuple[bool, str, str]:
        """Invoked by clients to query the replicated state (read-only commands).

        Arguments
        ---------
        :param str query: request for state machine, read-only
        ---------

        Returns
        -------
        :param bool status: OK if state machine processed query
        :param str response: state machine output, if successful
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError


class GrpcRaftClient(AbstractRaftClient):
    # """
    # A gRPC-based implementation of `AbstractRaftClient`.
    # """

    def __init__(self, credentials: Optional[grpc.ChannelCredentials] = None) -> None:
        self._credentials: Optional[grpc.ChannelCredentials] = credentials

    # =========================================================

    async def client_request(
        self, *, client_id: str, sequence_num: int, command: str, timeout: float = 5.0
    ) -> Tuple[bool, str, str]:
        """Invoked by clients to modify the replicated state."""
        request = raft_pb2.ClientRequestRequest(
            client_id=client_id,
            sequence_num=sequence_num,
            command=command,
        )
        async with self.create_channel() as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            try:
                response: raft_pb2.ClientRequestResponse = await asyncio.wait_for(
                    stub.ClientRequest(request), timeout=timeout
                )
                return response.status, response.response, response.leader_hint
            except grpc.aio.AioRpcError:
                pass
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception:
                raise
            return False, None, None

    async def register_client(self) -> Tuple[bool, str, str]:
        """Invoked by new clients to open new session, used to eliminate duplicate requests."""
        raise NotImplementedError

    async def client_query(self, *, query: str) -> Tuple[bool, str, str]:
        """Invoked by clients to query the replicated state (read-only commands)."""
        raise NotImplementedError
