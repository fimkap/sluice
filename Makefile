GO        ?= go
PKGS      := ./...
IMAGE     ?= sluice:dev
TF_IMAGE  ?= hashicorp/terraform:1.17.2

.PHONY: all build test race fuzz lint bench bench-docker docker up down tf-validate clean

all: lint race

build:
	$(GO) build -o bin/ ./cmd/...

test:
	$(GO) test $(PKGS)

race:
	$(GO) test -race $(PKGS)

# Short fuzzing runs; go test runs one fuzz target at a time.
fuzz:
	$(GO) test ./internal/record   -run '^$$' -fuzz FuzzDecodeBatch -fuzztime 15s
	$(GO) test ./internal/protocol -run '^$$' -fuzz FuzzDecode      -fuzztime 15s

lint:
	$(GO) vet $(PKGS)
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# All experiments in process, on this machine.
bench:
	$(GO) run ./cmd/sluice-bench experiment all

# The same experiments inside Linux (Docker), where fsync means fdatasync.
bench-docker: docker
	docker run --rm --entrypoint /sluice-bench $(IMAGE) experiment all

docker:
	docker build -t $(IMAGE) .

up:
	docker compose up -d --build

down:
	docker compose down -v

tf-validate:
	docker run --rm -v $(CURDIR)/deploy/terraform:/tf -w /tf $(TF_IMAGE) fmt -check -recursive
	docker run --rm -v $(CURDIR)/deploy/terraform:/tf -w /tf $(TF_IMAGE) init -backend=false -input=false
	docker run --rm -v $(CURDIR)/deploy/terraform:/tf -w /tf $(TF_IMAGE) validate

clean:
	rm -rf bin bench-results
