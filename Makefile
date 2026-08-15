.PHONY: all build test test-race vet fmt fmt-check clean docker docker-multi run

GO ?= go

all: fmt vet test

# Build the server binary.
build:
	$(GO) build -trimpath -o bin/courierbox ./cmd/courierbox

# Run all tests.
test:
	$(GO) test ./...

# Run all tests with the race detector.
test-race:
	$(GO) test -race ./...

# Run go vet.
vet:
	$(GO) vet ./...

# Format all Go files.
fmt:
	$(GO) fmt ./...

# Check formatting without modifying files.
fmt-check:
	@out=$$(gofmt -l . 2>/dev/null); if [ -n "$$out" ]; then \
		echo "gofmt would reformat:"; echo "$$out"; exit 1; fi

# Run the server locally with the example config.
run: build
	./bin/courierbox -config config.example.json

# Build a single-platform (local) Docker image for the host architecture.
docker:
	./build_benzhi_docker.sh linux/$$(go env GOARCH) courierbox:local

# Build a true multi-arch image (requires a registry with --push, or buildx).
docker-multi:
	./build_benzhi_docker.sh linux/amd64,linux/arm64 courierbox:multi

# Remove build artifacts.
clean:
	rm -rf bin/
