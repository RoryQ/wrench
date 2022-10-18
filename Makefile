.PHONY: test
test: _spanner-up
	go test -race -count=1 ./...
	-@make _spanner-down

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

S_SPANNER_PORT = $(shell docker port spanner-tests 9010 | sed 's/0.0.0.0/localhost/')
S_PROJECT = test-project
S_INSTANCE = my-instance

test: export SPANNER_EMULATOR_HOST=$(S_SPANNER_PORT)
test: export SPANNER_PROJECT_ID=$(S_PROJECT)
test: export SPANNER_INSTANCE_ID=$(S_INSTANCE)

