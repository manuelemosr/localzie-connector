BINARY := bin/localzie-connector
VERSION ?= dev
.PHONY: build install test check fmt clean
build:
	go build -trimpath -ldflags "-X main.version=$(VERSION)" -o $(BINARY) ./cmd/localzie-connector
install:
	./install.sh
test:
	go test -race ./...
check:
	test -z "$$(gofmt -l cmd internal)"
	go mod verify
	go vet ./...
fmt:
	gofmt -w cmd internal
clean:
	rm -rf bin
