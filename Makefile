RUN ?= .*

.PHONY: test
test: _spanner-up
	go test -race -v -count=1 ./... -run=$(RUN)
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

GOLANGCI_LINT_VERSION ?= 1.56.2
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
		--name spanner-tests \
		roryq/spanner-emulator:latest >/dev/null 2>&1
	@sleep 2

.PHONY: _spanner-down
_spanner-down:
	-@docker stop spanner-tests >/dev/null 2>&1

S_SPANNER_PORT = $(shell docker port spanner-tests 9010 | grep '0.0.0.0' | sed 's/0.0.0.0/localhost/')
S_PROJECT = test-project
S_INSTANCE = my-instance

test: export SPANNER_EMULATOR_HOST=$(S_SPANNER_PORT)
test: export SPANNER_PROJECT_ID=$(S_PROJECT)
test: export SPANNER_INSTANCE_ID=$(S_INSTANCE)

