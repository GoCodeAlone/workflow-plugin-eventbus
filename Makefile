.PHONY: proto-gen build test vet

# proto-gen regenerates gen/eventbus.pb.go from proto/eventbus.proto.
# Requires: protoc v7.34.1 and protoc-gen-go v1.36.11
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
# WARNING: running with a different protoc/protoc-gen-go version will produce
# a different gen/eventbus.pb.go — commit only if intentionally upgrading.
proto-gen:
	protoc \
		--proto_path=proto \
		--go_out=gen \
		--go_opt=paths=source_relative \
		proto/eventbus.proto

build:
	GOWORK=off go build ./...

test:
	GOWORK=off go test ./... -v -race -count=1

vet:
	GOWORK=off go vet ./...
