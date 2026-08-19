.PHONY: test test-integration-nats

test:
	go test ./...

test-integration-nats:
	sh ./scripts/test-integration-nats.sh
