SHELL := /bin/bash
LABEL := mitsuha_runtime/mitsuha_runtime_test

build-assets:
	@$(MAKE) -C assets build

build: build-assets
	@echo $(LABEL): Building docker image
	@docker build --progress plain -t ghcr.io/supercmmetry/mitsuha-runtime:test-latest -f ../images/test.Dockerfile ..
	@echo $(LABEL): Built docker image

test: build
	@echo $(LABEL): Running tests
	@docker-compose -f docker-compose/development.yml --project-directory . up --force-recreate --exit-code-from mitsuha_runtime
	@echo $(LABEL): Completed tests successfully

clean-assets:
	@$(MAKE) -C assets clean

clean: clean-assets
	@$(MAKE) -C mitsuha_wasm_echo clean
	@$(MAKE) -C mitsuha_wasm_loop clean
	@$(MAKE) -C mitsuha_wasm_main clean
	@$(MAKE) -C mitsuha_wasi_echo clean
	@echo $(LABEL): Successfully cleaned
