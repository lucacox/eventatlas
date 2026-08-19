#!/bin/sh

set -eu

EVENTATLAS_NATS_IMAGE="${EVENTATLAS_NATS_IMAGE:-nats:2.14.4-alpine}"
EVENTATLAS_NATS_CONTAINER="eventatlas-nats-integration-$$"

cleanup() {
	docker stop "$EVENTATLAS_NATS_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run --rm --detach \
	--name "$EVENTATLAS_NATS_CONTAINER" \
	--publish 127.0.0.1::4222 \
	"$EVENTATLAS_NATS_IMAGE" \
	-js >/dev/null

EVENTATLAS_NATS_ADDRESS="$(docker port "$EVENTATLAS_NATS_CONTAINER" 4222/tcp)"
EVENTATLAS_NATS_URL="nats://$EVENTATLAS_NATS_ADDRESS" \
	go test -tags=integration ./internal/discovery/nats ./internal/api -count=1
