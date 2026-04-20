.PHONY: compile test test-gosim

compile:
		protoc ./grpc/*.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --proto_path=.

test:
		go test $(shell go list ./... | grep -v /gosim)

test-gosim:
		cd gosim && go run github.com/jellevandenhooff/gosim/cmd/gosim test -v
