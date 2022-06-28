#!/bin/sh
python -m grpc_tools.protoc -I. --python_out=$2 --grpc_python_out=$2 $1
