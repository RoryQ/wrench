.PHONY: test
test:
	@make _spanner-up
	S_SPANNER_PORT=$$(docker port spanner-tests 9010 | head -1 | cut -d: -f2); \
	SPANNER_EMULATOR_HOST=localhost:$$S_SPANNER_PORT \
	SPANNER_PROJECT_ID=$(S_PROJECT) \
	SPANNER_INSTANCE_ID=$(S_INSTANCE) \
	go test -race -v -count=1 ./...
	-@make _spanner-down

.PHONY: README.md
README.md: export WRENCH_LOCK_IDENTIFIER=58a4394a-19f9-4dbf-880d-20b6cf169d46
README.md: export SPANNER_PROJECT_ID=
README.md: export SPANNER_INSTANCE_ID=
README.md:
	go run tools/usage/main.go

.PHONY: lint
lint: tooldep.golangci-lint
	golangci-lint run

GOLANGCI_LINT_VERSION ?= 1.64.8
.PHONY: tooldep.golangci-lint
tooldep.golangci-lint:
	-@[[ "$(shell golangci-lint version)" =~ "$(GOLANGCI_LINT_VERSION)" ]] || \
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v$(GOLANGCI_LINT_VERSION)

.PHONY: _spanner-up
_spanner-up:
	-@make _spanner-down >/dev/null 2>&1 # clear previous
	@docker run --rm --detach -p 9010 -p 9020 \
		--env SPANNER_PROJECT_ID=$(S_PROJECT) \
		--env SPANNER_INSTANCE_ID=$(S_INSTANCE) \
		--env HTTP_PROXY="" \
		--env http_proxy="" \
        --env HTTPS_PROXY="" \
        --env https_proxy="" \
		--name spanner-tests \
		roryq/spanner-emulator:latest >/dev/null 2>&1
	@sleep 5

.PHONY: _spanner-down
_spanner-down:
	-@docker stop spanner-tests >/dev/null 2>&1

S_PROJECT = test-project
S_INSTANCE = my-instance
