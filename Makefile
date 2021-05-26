.PHONY: all local test test-local integration-test-local install-deps lint fmt prebuild-local build-local

all: test

local: install-deps build-local fmt lint test-local bench-local

prebuild-local:
	@echo "+ $@"
	@mkdir -p bin
	@VERSION=$$(git describe --tags HEAD 2>/dev/null || echo "unknown"); echo $$VERSION; \
		go build -ldflags "-X main.version=$${VERSION}" -o ./bin ./cmd/modelgen
	@curl -o example/play_with_ovs/ovs.ovsschema https://raw.githubusercontent.com/openvswitch/ovs/v2.15.0/vswitchd/vswitch.ovsschema
	@go generate -v ./...

build-local: prebuild-local
	@echo "+ $@"
	@go build -v ./...

test-local:
	@echo "+ $@"
	@go test -race -coverprofile=unit.cov -short -v ./...

bench-local:
	@echo "+ $@"
	@go test -run=XXX -count=3 -bench=. ./... | tee bench.out
	@benchstat bench.out

integration-test-local: prebuild-local
	@echo "+ $@"
	@go test -race -v -coverprofile=integration.cov -run Integration ./...

test:
	@docker-compose pull
	@docker-compose run --rm test

install-deps:
	@echo "+ $@"
	@golangci-lint --version
	@go install golang.org/x/perf/cmd/benchstat@latest

lint:
	@echo "+ $@"
	@golangci-lint run

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)"
