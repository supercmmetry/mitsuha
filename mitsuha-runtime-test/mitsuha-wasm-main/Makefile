SHELL := /bin/bash
LABEL := mitsuha_runtime/mitsuha_runtime_test/mitsuha_wasm_main

build:
	@echo $(LABEL): Building
	@cargo build --release --target wasm32-unknown-unknown
	@echo $(LABEL): Successfully built

clean:
	@echo $(LABEL): Cleaning
	@cargo clean
	@echo $(LABEL): Successfully cleaned

lint:
	@echo $(LABEL): Linting
	@cargo fmt
	@echo $(LABEL): Successfully linted

cache:
	@echo $(LABEL): Creating build cache
	@mkdir -p "$(CACHE_DIR)/$(LABEL)"
	@cp -rp target "$(CACHE_DIR)/$(LABEL)/target"
	@echo $(LABEL): Successfully created build cache

load-cache-internal:
	@echo $(LABEL): Loading build cache
	@cp -rp "$(CACHE_DIR)/$(LABEL)/target" target
	@echo $(LABEL): Successfully loaded build cache

load-cache:
	@[ -a "$(CACHE_DIR)/$(LABEL)/target" ] && $(MAKE) load-cache-internal || \
	echo "$(LABEL): No cache was found" 
