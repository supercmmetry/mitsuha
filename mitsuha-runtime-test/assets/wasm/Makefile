SHELL := /bin/bash
LABEL := mitsuha_runtime/mitsuha_runtime_test/assets/wasm

build-wasm-echo:
	@$(MAKE) -C "../../mitsuha_wasm_echo" build
	@cp "../../mitsuha_wasm_echo/target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm" mitsuha.test.echo.v0.1.0.wasm

build-wasm-main:
	@$(MAKE) -C "../../mitsuha_wasm_main" build
	@cp "../../mitsuha_wasm_main/target/wasm32-unknown-unknown/release/mitsuha_wasm_main.wasm" mitsuha.test.main.v0.1.0.wasm

build-wasm-loop:
	@$(MAKE) -C "../../mitsuha_wasm_loop" build
	@cp "../../mitsuha_wasm_loop/target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm" mitsuha.test.loop.v0.1.0.wasm

build: build-wasm-echo build-wasm-main build-wasm-loop
	@echo $(LABEL): Successfully built

clean:
	@echo $(LABEL): Cleaning
	@rm -rf *.wasm
	@echo $(LABEL): Successfully cleaned