# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import raft_pb2 as raft__pb2


class RaftServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.AppendEntries = channel.unary_unary(
            "/RaftService/AppendEntries",
            request_serializer=raft__pb2.AppendEntriesRequest.SerializeToString,
            response_deserializer=raft__pb2.AppendEntriesResponse.FromString,
        )
        self.RequestVote = channel.unary_unary(
            "/RaftService/RequestVote",
            request_serializer=raft__pb2.RequestVoteRequest.SerializeToString,
            response_deserializer=raft__pb2.RequestVoteResponse.FromString,
        )


class RaftServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def AppendEntries(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def RequestVote(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_RaftServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "AppendEntries": grpc.unary_unary_rpc_method_handler(
            servicer.AppendEntries,
            request_deserializer=raft__pb2.AppendEntriesRequest.FromString,
            response_serializer=raft__pb2.AppendEntriesResponse.SerializeToString,
        ),
        "RequestVote": grpc.unary_unary_rpc_method_handler(
            servicer.RequestVote,
            request_deserializer=raft__pb2.RequestVoteRequest.FromString,
            response_serializer=raft__pb2.RequestVoteResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "RaftService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class RaftService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def AppendEntries(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/RaftService/AppendEntries",
            raft__pb2.AppendEntriesRequest.SerializeToString,
            raft__pb2.AppendEntriesResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def RequestVote(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/RaftService/RequestVote",
            raft__pb2.RequestVoteRequest.SerializeToString,
            raft__pb2.RequestVoteResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class RaftClusterServiceStub(object):
    """*
    Client Interaction
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ClientRequest = channel.unary_unary(
            "/RaftClusterService/ClientRequest",
            request_serializer=raft__pb2.ClientRequestRequest.SerializeToString,
            response_deserializer=raft__pb2.ClientRequestResponse.FromString,
        )
        self.RegisterClient = channel.unary_unary(
            "/RaftClusterService/RegisterClient",
            request_serializer=raft__pb2.RegisterClientRequest.SerializeToString,
            response_deserializer=raft__pb2.RegisterClientResponse.FromString,
        )
        self.ClientQuery = channel.unary_unary(
            "/RaftClusterService/ClientQuery",
            request_serializer=raft__pb2.ClientQueryRequest.SerializeToString,
            response_deserializer=raft__pb2.ClientQueryResponse.FromString,
        )


class RaftClusterServiceServicer(object):
    """*
    Client Interaction
    """

    def ClientRequest(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def RegisterClient(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def ClientQuery(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_RaftClusterServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "ClientRequest": grpc.unary_unary_rpc_method_handler(
            servicer.ClientRequest,
            request_deserializer=raft__pb2.ClientRequestRequest.FromString,
            response_serializer=raft__pb2.ClientRequestResponse.SerializeToString,
        ),
        "RegisterClient": grpc.unary_unary_rpc_method_handler(
            servicer.RegisterClient,
            request_deserializer=raft__pb2.RegisterClientRequest.FromString,
            response_serializer=raft__pb2.RegisterClientResponse.SerializeToString,
        ),
        "ClientQuery": grpc.unary_unary_rpc_method_handler(
            servicer.ClientQuery,
            request_deserializer=raft__pb2.ClientQueryRequest.FromString,
            response_serializer=raft__pb2.ClientQueryResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "RaftClusterService", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class RaftClusterService(object):
    """*
    Client Interaction
    """

    @staticmethod
    def ClientRequest(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/RaftClusterService/ClientRequest",
            raft__pb2.ClientRequestRequest.SerializeToString,
            raft__pb2.ClientRequestResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def RegisterClient(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/RaftClusterService/RegisterClient",
            raft__pb2.RegisterClientRequest.SerializeToString,
            raft__pb2.RegisterClientResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def ClientQuery(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/RaftClusterService/ClientQuery",
            raft__pb2.ClientQueryRequest.SerializeToString,
            raft__pb2.ClientQueryResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
