import asyncio
from contextlib import suppress
from typing import Iterable

import grpc

import raft_pb2
import raft_pb2_grpc


class RaftClient:
    pass


class AsyncGrpcRaftClient:
    async def request_append_entries(self, address: str, term: int, leader_id: str, entries: Iterable[str]):
        done, pending = await asyncio.wait({
            asyncio.create_task(self._request_append_entries(address, term, leader_id, entries)),
            asyncio.create_task(asyncio.sleep(5.0)),
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            with suppress(asyncio.CancelledError):
                task.cancel()

    async def _request_append_entries(self, address: str, term: int, leader_id: str, entries: Iterable[str]):
        async with grpc.aio.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            request = raft_pb2.AppendEntriesRequest(term=term, leader_id=leader_id, entries=entries)
            try:
                response = await stub.AppendEntries(request)
            except grpc.aio.AioRpcError:
                pass

    async def request_vote(self, address: str, term: int, candidate_id: str, last_log_index: int, last_log_term: int) -> bool:
        self._vote_granted = None
        timeout_task = asyncio.create_task(asyncio.sleep(5.0))
        done, pending = await asyncio.wait({
            asyncio.create_task(self._request_vote(address, term, candidate_id, last_log_index, last_log_term)),
            timeout_task,
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            with suppress(asyncio.CancelledError):
                task.cancel()
        if timeout_task in pending:
            return self._vote_granted or False

    async def _request_vote(self, address: str, term: int, candidate_id: str, last_log_index: int, last_log_term: int):
        async with grpc.aio.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            request = raft_pb2.RequestVoteRequest(
                term=term, candidate_id=candidate_id,
                last_log_index=last_log_index, last_log_term=last_log_term
            )
            try:
                response = await stub.RequestVote(request)
                self._vote_granted = response.vote_granted
            except grpc.aio.AioRpcError:
                pass
