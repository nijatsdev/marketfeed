IMAGE ?= ghcr.io/nijatsdev/marketfeed
PORT  ?= 8080

.PHONY: help build run watch test lint fmt tidy check docker-build docker-run docker-down

# Deferred (=) so it evaluates at recipe time. Each separator space lives inside its
# $(if ...) so it disappears with the assignment; unset vars leave no residue at all.
APP_ENV = PORT=$(PORT)$(if $(TICK_INTERVAL_MS), TICK_INTERVAL_MS=$(TICK_INTERVAL_MS))$(if $(VOLATILITY_MULTIPLIER), VOLATILITY_MULTIPLIER=$(VOLATILITY_MULTIPLIER))$(if $(REDIS_URL), REDIS_URL=$(REDIS_URL))

.DEFAULT_GOAL := help

# ── help ───────────────────────────────────────────────────────────────────────

help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

# ── dev ────────────────────────────────────────────────────────────────────────

build: ## compile binary to bin/marketfeed
	@go build -trimpath -ldflags="-s -w" -o bin/marketfeed .

run: ## run locally; accepts PORT, TICK_INTERVAL_MS, VOLATILITY_MULTIPLIER, REDIS_URL
	@$(APP_ENV) go run .

watch: ## live reload with air; accepts the same vars as run
	@if command -v air > /dev/null; then \
		$(APP_ENV) air; \
	else \
		printf "'air' not found. Install it? [Y/n] "; read choice; \
		if [ "$$choice" != "n" ] && [ "$$choice" != "N" ]; then \
			go install github.com/air-verse/air@latest && $(APP_ENV) air; \
		else \
			echo "Exiting."; exit 1; \
		fi; \
	fi
	
# ── quality ────────────────────────────────────────────────────────────────────

test: ## run tests with race detector and shuffle; set REDIS_ADDR to include real-Redis integration tests
	@REDIS_ADDR=$(REDIS_ADDR) go test -race -shuffle=on -count=1 ./...

lint: ## run golangci-lint
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not found: https://golangci-lint.run/usage/install/"; exit 1; \
	fi

fmt: ## format code with golangci-lint fmt
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint fmt ./...; \
	else \
		gofmt -w .; \
	fi

tidy: ## verify go.mod/go.sum are tidy
	@go mod tidy
	@git diff --exit-code go.mod go.sum

check: ## Everything CI runs: tidy, lint, test
	@$(MAKE) tidy
	@$(MAKE) lint
	@$(MAKE) test

# ── docker ─────────────────────────────────────────────────────────────────────

docker-build: ## build image and tag as latest
	@docker build -t $(IMAGE):latest .

docker-run: ## run image locally on PORT=$(PORT)
	@docker run --rm --name marketfeed -p $(PORT):$(PORT) \
		-e PORT=$(PORT) \
		$(if $(TICK_INTERVAL_MS),-e TICK_INTERVAL_MS=$(TICK_INTERVAL_MS)) \
		$(if $(VOLATILITY_MULTIPLIER),-e VOLATILITY_MULTIPLIER=$(VOLATILITY_MULTIPLIER)) \
		$(if $(REDIS_URL),-e REDIS_URL=$(REDIS_URL)) \
		$(IMAGE):latest

docker-down: ## stop all running containers of this image
	@ids=$$(docker ps -q --filter ancestor=$(IMAGE)); [ -n "$$ids" ] && docker stop $$ids || true
