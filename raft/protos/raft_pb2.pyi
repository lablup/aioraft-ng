from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class AppendEntriesRequest(_message.Message):
    __slots__ = [
        "entries",
        "leader_commit",
        "leader_id",
        "prev_log_index",
        "prev_log_term",
        "term",
    ]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    LEADER_COMMIT_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[Log]
    leader_commit: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    term: int
    def __init__(
        self,
        term: _Optional[int] = ...,
        leader_id: _Optional[str] = ...,
        prev_log_index: _Optional[int] = ...,
        prev_log_term: _Optional[int] = ...,
        entries: _Optional[_Iterable[_Union[Log, _Mapping]]] = ...,
        leader_commit: _Optional[int] = ...,
    ) -> None: ...

class AppendEntriesResponse(_message.Message):
    __slots__ = ["success", "term"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    success: bool
    term: int
    def __init__(self, term: _Optional[int] = ..., success: bool = ...) -> None: ...

class Log(_message.Message):
    __slots__ = ["command", "index", "term"]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    command: str
    index: int
    term: int
    def __init__(
        self,
        index: _Optional[int] = ...,
        term: _Optional[int] = ...,
        command: _Optional[str] = ...,
    ) -> None: ...

class RequestVoteRequest(_message.Message):
    __slots__ = ["candidate_id", "last_log_index", "last_log_term", "term"]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    candidate_id: str
    last_log_index: int
    last_log_term: int
    term: int
    def __init__(
        self,
        term: _Optional[int] = ...,
        candidate_id: _Optional[str] = ...,
        last_log_index: _Optional[int] = ...,
        last_log_term: _Optional[int] = ...,
    ) -> None: ...

class RequestVoteResponse(_message.Message):
    __slots__ = ["term", "vote_granted"]
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_GRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    vote_granted: bool
    def __init__(
        self, term: _Optional[int] = ..., vote_granted: bool = ...
    ) -> None: ...
