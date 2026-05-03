.PHONY: proto-gen build test vet

# Regenerate Go bindings from proto/eventbus.proto.
# Requires: protoc + protoc-gen-go (go install google.golang.org/protobuf/cmd/protoc-gen-go@latest)
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
