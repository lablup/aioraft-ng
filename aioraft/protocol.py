import abc
from typing import Iterable, Optional, Tuple

from aioraft.protos import raft_pb2
from aioraft.types import RaftId


class AbstractRaftProtocol(abc.ABC):
    @abc.abstractmethod
    async def on_append_entries(
        self,
        *,
        term: int,
        leader_id: RaftId,
        prev_log_index: int,
        prev_log_term: int,
        entries: Iterable[raft_pb2.Log],
        leader_commit: int,
    ) -> Tuple[int, bool]:
        """Receiver implementation:
        1. Reply false if term < currentTerm
        2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        3. If an existing entry conflicts with a new one (same index but different terms),
           delete the existing entry and all that follow it
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

        Arguments
        ---------
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
        :param int term: currentTerm, for leader to update itself
        :param bool success: true if follower contained entry matching prevLogIndex and prevLogTerm
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_request(self, command: str) -> Tuple[bool, str, Optional[str]]:
        """Handle a client command request.

        Returns
        -------
        :param bool success: True if the command was committed and applied
        :param str result: result of applying the command (or error message)
        :param Optional[str] leader_hint: address of current leader if this node is not the leader
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_request_vote(
        self,
        *,
        term: int,
        candidate_id: RaftId,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        """Receiver implementation:
        1. Reply false if term < currentTerm
        2. If votedFor is null or candidateId, and candidate's log is
           at lease as up-to-date as receiver's log, grant vote

        Arguments
        ---------
        :param int term: candidate's term
        :param raft.types.RaftId candidate_id: candidate requesting vote
        :param int last_log_index: index of candidate's last log entry
        :param int last_log_term: term of candidate's last log entry
        ---------

        Returns
        -------
        :param int term: currentTerm, for candidate to update itself
        :param bool vote_granted: true means candidate received vote
        -------
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_install_snapshot(
        self,
        *,
        term: int,
        leader_id: RaftId,
        last_included_index: int,
        last_included_term: int,
        data: bytes,
    ) -> Tuple[int]:
        """Receiver implementation for InstallSnapshot RPC.

        Arguments
        ---------
        :param int term: leader's term
        :param RaftId leader_id: so follower can redirect clients
        :param int last_included_index: the snapshot replaces all entries up through and including this index
        :param int last_included_term: term of last_included_index entry
        :param bytes data: serialized snapshot of state machine
        ---------

        Returns
        -------
        :param int term: currentTerm, for leader to update itself
        -------
        """
        raise NotImplementedError()
