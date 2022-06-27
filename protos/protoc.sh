#!/bin/sh
python -m grpc_tools.protoc -I. --python_out=../raft/rpc --grpc_python_out=../raft/rpc $1
