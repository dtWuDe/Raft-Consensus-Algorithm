run command to create gRPC file: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

run program on 3 terminal:

path/to/folder/raft> python test.py 127.0.0.1:4321 127.0.0.1:4322 127.0.0.1:4323

path/to/folder/raft> python test.py 127.0.0.1:4322 127.0.0.1:4323 127.0.0.1:4321

path/to/folder/raft> python test.py 127.0.0.1:4323 127.0.0.1:4321 127.0.0.1:4322