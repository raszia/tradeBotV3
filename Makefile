# v3TradeBot build/test helpers.
#
# Binaries are built into ./bin. Each service reads bootstrap config from
# $CONFIG_PATH (V3_CONFIG_PATH) or environment variables. See configs/config.example.toml.

GO        ?= go
BIN_DIR   ?= bin
BINARIES  := collector trade-engine order-executor reconciler balance-sync health-monitor dashboard retention-worker migrate

.PHONY: all build $(BINARIES) test vet fmt tidy clean

all: build

build: $(BINARIES)

$(BINARIES):
	$(GO) build -o $(BIN_DIR)/$@ ./cmd/$@

# Run the full test suite. DB/Redis integration tests are skipped unless their
# env vars are set (e.g. V3_TEST_MYSQL_DSN).
test:
	$(GO) test ./...

# Apply database migrations (the only normal-ops schema writer).
migrate-up: migrate
	$(GO) run ./cmd/migrate

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR)
