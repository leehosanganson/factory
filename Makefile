.PHONY: build test fmt vet clean

build:
	mkdir -p bin
	go build -o bin/factory ./cmd/factory

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

vet:
	go vet ./...

clean:
	rm -rf bin
