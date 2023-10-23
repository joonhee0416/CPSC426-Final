# CPSC426 Final Project: Using Amazon Dynamo's Design to Increase Availability of Sharded Key-Value Store


## Paper
See the PDF file describing the design, implementation, and performance of the project at 
https://drive.google.com/file/d/1HjCG56ixTrmhpqIMEzm6y9AgB3riuLxb/view?usp=sharing.

## Setup
 1. Install the protobuf compiler `protoc` using the instructions found at https://grpc.io/docs/protoc-installation/. 
 2. Install the Go plugins, with instructions found at https://grpc.io/docs/languages/go/quickstart/. 
 3. Run `make` to generate the binding code with protobuf. 

## Testing
To test for the functionality and correctness of the sloppy quorum, hinted handoff, and vector clock conflict resolution, run `go test -v -run=TestSloppy` in the `kv/test` directory.

## Stress Testing
To start an instance of a server, run: 

```
go run cmd/server/server.go --shardmap $shardmap-file --node $nodename
```
To run the stress tester, run: 
```
go run cmd/stress/tester.go --shardmap $shardmap-file.json
```
Further details of the stress tester can be found at `cmd/stress/tester.go`.