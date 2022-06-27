import abc
import argparse
import asyncio
import enum
import random
import uuid
from contextlib import suppress
from datetime import datetime
from typing import Iterable


class RaftProtocol(abc.ABC):
    @abc.abstractmethod
    def on_append_entries(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_request_vote(self):
        raise NotImplementedError()


class GrpcRaftProtocol(RaftProtocol):
    def __init__(
        self,
        *,
        on_append_entries,
        on_request_vote,
    ):
        self.on_append_entries = on_append_entries
        self.on_request_vote = on_request_vote

    """
    def on_append_entries(self):
        raise NotImplementedError()

    def on_request_vote(self):
        raise NotImplementedError()
    """


class RaftPeer:
    def __init__(
        self,
        address: str,
        peers: Iterable[str],
        *,
        election_timeout: float = 10.0,
    ):
        self._id = str(uuid.uuid4())
        self._address = address
        self._election_timeout = election_timeout
        self._state: RaftState = RaftState.FOLLOWER
        self._term: int = 0
        self._peers = (self,) + tuple(peers)

    async def run(self):
        while True:
            self.reset_timeout()
            try:
                election_timeout_task = asyncio.create_task(self._wait_for_election_timeout())
                heartbeat_task = asyncio.create_task(self._listen_to_leader_heartbeat())
                done, pending = await asyncio.wait([
                    election_timeout_task,
                    heartbeat_task,
                ], return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    with suppress(asyncio.CancelledError):
                        task.cancel()
                if election_timeout_task in done:
                    raise asyncio.TimeoutError('Election Timeout!')
            except asyncio.TimeoutError as e:
                # TODO
                print(f'[{datetime.now().isoformat()}] asyncio.TimeoutError: {e}')
                await self._request_vote()

    async def _send_heartbeat(self):
        assert self.state == RaftState.LEADER.name

    async def _listen_to_leader_heartbeat(self):
        while True:
            print(f'[{datetime.now().isoformat()}] Listening to leader\'s heartbeat..')
            delay = random.random() * self._election_timeout * 1.2
            await asyncio.sleep(delay)
            self.reset_timeout()

    async def _request_vote(self):
        self._term += 1
        self._state = RaftState.CANDIDATE
        vote_count = 1  # votes for itself
        print(f'[{datetime.now().isoformat()}] {self.id} Request Vote!')
        results = await asyncio.gather([
            asyncio.create_task(self.client.request_vote(peer))
            for peer in self._peers[1:]
        ])
        # await asyncio.sleep(3.0)
        self._state = RaftState.LEADER if random.random() >= 0.5 else RaftState.FOLLOWER
        print(f'[{datetime.now().isoformat()}] {self.id} Vote Result: {self.state}')

    def reset_timeout(self):
        self._elapsed_time: float = 0.0

    def next_term(self):
        self._term += 1
        self._vote = True

    def on_vote_request(self):
        vote_granted, self._vote = self._vote, False

    @property
    def id(self) -> str:
        return self._id

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def election_timeout(self) -> float:
        return self._election_timeout

    @property
    def term(self) -> int:
        return self._term


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--address', '--addr', type=str, default='127.0.0.1:51651')
    return parser.parse_args()


async def main():
    args = parse_args()
    peer = RaftPeer(args.address, election_timeout=3.0)
    await peer.run()


if __name__ == "__main__":
    asyncio.run(main())
