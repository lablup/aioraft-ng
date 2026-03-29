from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class AppendEntriesRequest(_message.Message):
    __slots__ = ("term", "leader_id", "prev_log_index", "prev_log_term", "entries", "leader_commit")
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    LEADER_COMMIT_FIELD_NUMBER: _ClassVar[int]
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: _containers.RepeatedCompositeFieldContainer[Log]
    leader_commit: int
    def __init__(
        self,
        term: int | None = ...,
        leader_id: str | None = ...,
        prev_log_index: int | None = ...,
        prev_log_term: int | None = ...,
        entries: _Iterable[Log | _Mapping] | None = ...,
        leader_commit: int | None = ...,
    ) -> None: ...

class AppendEntriesResponse(_message.Message):
    __slots__ = ("term", "success")
    TERM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    term: int
    success: bool
    def __init__(self, term: int | None = ..., success: bool = ...) -> None: ...

class RequestVoteRequest(_message.Message):
    __slots__ = ("term", "candidate_id", "last_log_index", "last_log_term")
    TERM_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    def __init__(
        self,
        term: int | None = ...,
        candidate_id: str | None = ...,
        last_log_index: int | None = ...,
        last_log_term: int | None = ...,
    ) -> None: ...

class RequestVoteResponse(_message.Message):
    __slots__ = ("term", "vote_granted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_GRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    vote_granted: bool
    def __init__(self, term: int | None = ..., vote_granted: bool = ...) -> None: ...

class Log(_message.Message):
    __slots__ = ("index", "term", "command")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    index: int
    term: int
    command: str
    def __init__(self, index: int | None = ..., term: int | None = ..., command: str | None = ...) -> None: ...

class ClientRequestMessage(_message.Message):
    __slots__ = ("command",)
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    command: str
    def __init__(self, command: str | None = ...) -> None: ...

class ClientResponseMessage(_message.Message):
    __slots__ = ("success", "result", "leader_hint", "error")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    LEADER_HINT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    success: bool
    result: str
    leader_hint: str
    error: str
    def __init__(
        self, success: bool = ..., result: str | None = ..., leader_hint: str | None = ..., error: str | None = ...
    ) -> None: ...

class InstallSnapshotRequest(_message.Message):
    __slots__ = ("term", "leader_id", "last_included_index", "last_included_term", "data")
    TERM_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_INCLUDED_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_INCLUDED_TERM_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: bytes
    def __init__(
        self,
        term: int | None = ...,
        leader_id: str | None = ...,
        last_included_index: int | None = ...,
        last_included_term: int | None = ...,
        data: bytes | None = ...,
    ) -> None: ...

class InstallSnapshotResponse(_message.Message):
    __slots__ = ("term",)
    TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    def __init__(self, term: int | None = ...) -> None: ...
