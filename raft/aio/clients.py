import abc
from typing import Tuple

from raft.types import RaftClusterStatus


class AbstractRaftClient(abc.ABC):
    @abc.abstractmethod
    async def client_request(
        self, client_id: str, sequence_num: int, command: str
    ) -> Tuple[RaftClusterStatus, str, str]:
        """Invoked by clients to modify the replicated state.

        Arguments
        ---------
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
    async def register_client(
        self,
    ) -> Tuple[RaftClusterStatus, str, str]:
        """Invoked by new clients to open new session, used to eliminate duplicate requests.

        Returns
        -------
        :param raft.types.RaftClusterStatus status: OK if state machine registered client
        :param str client_id: unique identifier for client session
        :param str leader_hint: address of recent leader, if known
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def client_query(self, query: str) -> Tuple[RaftClusterStatus, str, str]:
        """Invoked by clients to query the replicated state (read-only commands)

        Arguments
        ---------
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
