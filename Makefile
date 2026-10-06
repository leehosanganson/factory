.PHONY: build test fmt vet clean help

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

help:
	@printf '%-8s %s\n' \
		build 'Compile the Factory CLI' \
		test 'Run all Go tests' \
		fmt 'Format Go source files' \
		vet 'Run go vet' \
		clean 'Remove build artifacts' \
		help 'List available Make targets'
