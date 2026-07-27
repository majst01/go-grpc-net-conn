package testproto

//go:generate sh -c "protoc ./*.proto --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. --go-connectrpc_out=paths=source_relative:."

