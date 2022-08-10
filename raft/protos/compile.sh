#!/bin/sh
python -m grpc_tools.protoc -I. --grpc_python_out=. ./*.proto
protoc -I. --python_out=. --pyi_out=. ./*.proto
sed -i '' 's/import .*_pb2/from . import raft_pb2 as raft__pb2/' ./raft_pb2_grpc.py
sed -i '' '1s/^/# type: ignore\n/' ./raft_pb2.py
sed -i '' 's/__slots__ = \[\]/__slots__: _List[str] = \[\]/' ./raft_pb2.pyi
sed -i '' '2s/^/from typing import List as _List\n/' ./raft_pb2.pyi
sed -i '' '/class RaftClusterStatus/ s/$/ # type: ignore/' ./raft_pb2.pyi

python -m black .
python -m isort .
