import abc
from typing import Iterable, Tuple

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
    async def on_client_request(
        self,
        *,
        client_id: str,
        sequence_num: int,
        command: str,
    ) -> Tuple[bool, str, str]:
        """Receiver implementation:
        1. Reply NOT_LEADER if not leader, providing hint when available
        2. Append command to log, replicate and commit it
        3. Reply SESSION_EXPIRED if no record of clientId or if response for client's sequenceNum already discarded
        4. If sequenceNum already processed from client, reply OK with stored response
        5. Apply command in log order
        6. Save state machine output with sequenceNum for client, discard any prior response for client
        7. Reply OK with state machine output
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def on_register_client(self) -> Tuple[bool, str, str]:
        """Receiver implementation:
        1. Reply NOT_LEADER if not leader, providing hint when available
        2. Append register command to log, replicate and commit it
        3. Apply command in log order, allocating session for new client
        4. Reply OK with unique client identifier (the log index of this register command can be used)
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def on_client_query(self, *, query: str) -> Tuple[bool, str, str]:
        """Receiver implementation:
        1. Reply NOT_LEADER if not leader, providing hint when available
        2. Wait until last committed entry is from this leader's term
        3. Save commitIndex as local variable readIndex (used below)
        4. Send new round of heartbeats, and wait for reply from majority of servers
        5. Wait for state machine to advance at least to the readIndex log entry
        6. Process query
        7. Reply OK with state machine output
        """
        raise NotImplementedError
