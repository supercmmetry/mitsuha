SHELL := /bin/bash
LABEL := mitsuha_runtime

build-test-assets:
	@$(MAKE) -C mitsuha-runtime-test build-assets

build:
	@echo $(LABEL): Building all crates
	@cargo build --release
	@echo $(LABEL): Built all crates

test: build-test-assets
	@echo $(LABEL): Running tests
	@cargo test -- --nocapture
	@echo $(LABEL): Completed tests successfully

clean-test-assets:
	@$(MAKE) -C mitsuha-runtime-test clean-assets

clean: clean-test-assets
	@rm -rf target
