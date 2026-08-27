.PHONY: test test-integration-nats test-integration-postgres test-integration-e2e test-integration

test:
	go test ./...

test-integration-nats:
	sh ./scripts/test-integration-nats.sh

test-integration-postgres:
	sh ./scripts/test-integration-postgres.sh

test-integration-e2e:
	sh ./scripts/test-integration-e2e.sh

test-integration: test-integration-nats test-integration-postgres
