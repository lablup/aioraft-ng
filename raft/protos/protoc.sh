#!/bin/sh
python -m grpc_tools.protoc -I. --python_out=$2 --grpc_python_out=$2 $1
sed -i '' 's/import .*_pb2/from . import raft_pb2 as raft__pb2/' $2/raft_pb2_grpc.py
