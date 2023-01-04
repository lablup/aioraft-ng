import abc


class RaftConfig:
    config: set[str]


class RaftContext:
    pass


class AbstractRaftState(abc.ABC):
    context: RaftContext


class Follower(AbstractRaftState):
    pass


class Candidate(AbstractRaftState):
    pass


class Leader(AbstractRaftState):
    pass
