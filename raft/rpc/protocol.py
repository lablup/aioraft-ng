import abc
from typing import Callable


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
        on_append_entries: Callable,
        on_request_vote: Callable,
    ):
        self.on_append_entries = on_append_entries
        self.on_request_vote = on_request_vote
