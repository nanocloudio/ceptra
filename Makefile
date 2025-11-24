.PHONY: help fmt fmt-check clippy lint check build build-release test clean spec-lint spec-sync-check spec-sync-write wire-lint

SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c

CARGO ?= cargo
CLIPPY_ARGS ?= -D warnings
CARGO_FEATURES ?= --all-features
CARGO_TARGETS ?= --all-targets

help:
	@echo "ceptra make targets"
	@echo "  make build          # debug build of ceptra binary + lib ($(CARGO_FEATURES))"
	@echo "  make build-release  # optimized build of ceptra binary + lib ($(CARGO_FEATURES))"
	@echo "  make test           # full test suite (--all) $(CARGO_FEATURES) (tests/ only)"
	@echo "  make check          # cargo check $(CARGO_TARGETS) $(CARGO_FEATURES)"
	@echo "  make fmt/fmt-check  # rustfmt (check mode available)"
	@echo "  make clippy|lint    # clippy with warnings as errors"
	@echo "  make spec-lint      # run spec + telemetry catalog lint"
	@echo "  make spec-sync-check/spec-sync-write # validate or update spec sync"
	@echo "  make wire-lint      # validate wire definitions"
	@echo "  make clean          # cargo clean"

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy $(CARGO_TARGETS) $(CARGO_FEATURES) -- $(CLIPPY_ARGS)

lint: fmt-check clippy

check:
	$(CARGO) check $(CARGO_TARGETS) $(CARGO_FEATURES)

build:
	$(CARGO) build $(CARGO_TARGETS) $(CARGO_FEATURES)

build-release:
	$(CARGO) build $(CARGO_TARGETS) $(CARGO_FEATURES) --release

test:
	$(CARGO) test --all $(CARGO_FEATURES)

clean:
	$(CARGO) clean

spec-lint:
	if [[ ! -x ../clustor/target/release/spec_lint ]]; then \
		(cd ../clustor && $(CARGO) build --release --bin spec_lint); \
	fi
	../clustor/target/release/spec_lint --manifest ../clustor/manifests/consensus_core_manifest.json
	$(CARGO) run --manifest-path tools/telemetry_guard/Cargo.toml -- --catalog ../clustor/telemetry/catalog.json --catalog telemetry/catalog.json

spec-sync-check:
	$(CARGO) run --manifest-path tools/spec_sync/Cargo.toml -- --check

spec-sync-write:
	$(CARGO) run --manifest-path tools/spec_sync/Cargo.toml -- --write

wire-lint:
	$(CARGO) run --manifest-path tools/wire_lint/Cargo.toml
