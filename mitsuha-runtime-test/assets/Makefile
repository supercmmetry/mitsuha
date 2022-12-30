SHELL := /bin/bash
LABEL := mitsuha_runtime/mitsuha_runtime_test/assets

build-wasm:
	@$(MAKE) -C wasm build

build: build-wasm
	@echo $(LABEL): Successfully built

clean-wasm:
	@$(MAKE) -C wasm clean

clean: clean-wasm
	@echo $(LABEL): Successfully cleaned
