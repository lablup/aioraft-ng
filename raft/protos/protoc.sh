#!/bin/sh
python -m grpc_tools.protoc -I. --grpc_python_out=$2 $1
sed -i '' 's/import .*_pb2/from . import raft_pb2 as raft__pb2/' $2/raft_pb2_grpc.py
protoc -I. --python_out=$2 --pyi_out=$2 $1
sed -i "" '1s/^/# type: ignore\n/' $2/raft_pb2.py $2/raft_pb2_grpc.py
python -m black $2/raft_pb2.py $2/raft_pb2_grpc.py $2/raft_pb2.pyi
python -m isort $2/raft_pb2.py $2/raft_pb2.pyi
