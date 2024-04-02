SHELL := /bin/bash
LABEL := mitsuha_runtime/mitsuha_runtime_test/assets/wasm

build-wasm-echo:
	@$(MAKE) -C "../../mitsuha-wasm-echo" build
	@cp "../../target/wasm32-unknown-unknown/release/mitsuha_wasm_echo.wasm" mitsuha.test.echo.v0.1.0.wasm

build-wasm-main:
	@$(MAKE) -C "../../mitsuha-wasm-main" build
	@cp "../../target/wasm32-unknown-unknown/release/mitsuha_wasm_main.wasm" mitsuha.test.main.v0.1.0.wasm

build-wasm-loop:
	@$(MAKE) -C "../../mitsuha-wasm-loop" build
	@cp "../../target/wasm32-unknown-unknown/release/mitsuha_wasm_loop.wasm" mitsuha.test.loop.v0.1.0.wasm

build-wasi-echo:
	@$(MAKE) -C "../../mitsuha-wasi-echo" build
	@cp "../../target/wasm32-wasi/release/mitsuha_wasi_echo.wasm" mitsuha.test.wasi.echo.v0.1.0.wasm


build: build-wasm-echo build-wasm-main build-wasm-loop build-wasi-echo
	@echo $(LABEL): Successfully built

clean:
	@echo $(LABEL): Cleaning
	@rm -rf *.wasm
	@echo $(LABEL): Successfully cleaned