SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: fmt fmt-check vet test test-race build check

# Resolve gofmt from the module's Go baseline instead of trusting the default
# formatter on PATH. The same pinned version is selected by setup-go in CI.
GOFMT := $(shell GOWORK=off GOTOOLCHAIN=go1.26.8 go env GOROOT)/bin/gofmt

PIPEFAIL := set -o pipefail;

fmt:
	$(PIPEFAIL) find . -type f -name '*.go' -not -path './.git/*' -print0 | xargs -0 $(GOFMT) -w

fmt-check:
	@$(PIPEFAIL) output="$$(mktemp)"; trap 'rm -f "$$output"' EXIT; \
		if ! find . -type f -name '*.go' -not -path './.git/*' -print0 | xargs -0 $(GOFMT) -l >"$$output"; then exit 1; fi; \
		if [[ -s "$$output" ]]; then cat "$$output"; exit 1; fi

vet:
	GOWORK=off go vet ./...

test-race:
	GOWORK=off go test -race ./...

test: test-race

build:
	GOWORK=off go build ./...

check: fmt-check vet test-race build
