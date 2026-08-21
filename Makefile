.PHONY: test test-integration-nats test-integration-postgres test-integration

test:
	go test ./...

test-integration-nats:
	sh ./scripts/test-integration-nats.sh

test-integration-postgres:
	sh ./scripts/test-integration-postgres.sh

test-integration: test-integration-nats test-integration-postgres
