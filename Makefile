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
	@cargo test -- --test-threads 1
	@echo $(LABEL): Completed tests successfully

test-light: build-test-assets
	@echo $(LABEL): Running lightweight tests
	@cargo test --workspace --exclude mitsuha-qflow -- --test-threads 1
	@echo $(LABEL): Completed lightweight tests successfully

clean-test-assets:
	@$(MAKE) -C mitsuha-runtime-test clean-assets

clean: clean-test-assets
	@rm -rf target

cache:
	@echo mitsuha: Creating build cache for mitsuha
	@mkdir -p "$(CACHE_DIR)/mitsuha"
	@cp -rp target "$(CACHE_DIR)/mitsuha/target"
	@echo mitsuha: Successfully created build cache for mitsuha

load-cache-internal:
	@echo mitsuha: Loading build cache for mitsuha
	@cp -rp "$(CACHE_DIR)/mitsuha/target" target
	@echo mitsuha: Successfully loaded build cache for mitsuha

load-cache:
	@[ -a "$(CACHE_DIR)/mitsuha/target" ] && $(MAKE) load-cache-internal || \
	echo "mitsuha: No cache was found for mitsuha"