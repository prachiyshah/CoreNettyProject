#!/bin/bash
#
# creates the python classes for our .proto
#

project_base="D:/my_data/git/cmpe275-project1/core-netty-4.0/python"
PROTOC_HOME=D:/protoc-2.5.0-win32

rm ${project_base}/src/comm_pb2.py

$PROTOC_HOME/protoc -I=${project_base}/resources --python_out=${project_base}/src ${project_base}/resources/comm.proto
