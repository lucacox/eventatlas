#!/bin/sh

set -eu

EVENTATLAS_NATS_IMAGE="${EVENTATLAS_NATS_IMAGE:-nats:2.14.4-alpine}"
EVENTATLAS_POSTGRES_IMAGE="${EVENTATLAS_POSTGRES_IMAGE:-postgres:18-alpine}"
EVENTATLAS_NATS_CONTAINER="eventatlas-nats-e2e-$$"
EVENTATLAS_POSTGRES_CONTAINER="eventatlas-postgres-e2e-$$"

cleanup() {
	docker stop "$EVENTATLAS_NATS_CONTAINER" "$EVENTATLAS_POSTGRES_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run --rm --detach \
	--name "$EVENTATLAS_NATS_CONTAINER" \
	--publish 127.0.0.1::4222 \
	"$EVENTATLAS_NATS_IMAGE" \
	-js >/dev/null

docker run --rm --detach \
	--name "$EVENTATLAS_POSTGRES_CONTAINER" \
	--env POSTGRES_USER=eventatlas \
	--env POSTGRES_PASSWORD=eventatlas \
	--env POSTGRES_DB=eventatlas \
	--publish 127.0.0.1::5432 \
	"$EVENTATLAS_POSTGRES_IMAGE" >/dev/null

attempt=0
until docker exec "$EVENTATLAS_POSTGRES_CONTAINER" pg_isready --username eventatlas --dbname eventatlas >/dev/null 2>&1; do
	attempt=$((attempt + 1))
	if [ "$attempt" -ge 60 ]; then
		printf 'PostgreSQL did not become ready in time\n' >&2
		exit 1
	fi
	sleep 1
done

EVENTATLAS_NATS_ADDRESS="$(docker port "$EVENTATLAS_NATS_CONTAINER" 4222/tcp)"
EVENTATLAS_POSTGRES_ADDRESS="$(docker port "$EVENTATLAS_POSTGRES_CONTAINER" 5432/tcp)"
EVENTATLAS_NATS_URL="nats://$EVENTATLAS_NATS_ADDRESS" \
EVENTATLAS_DATABASE_URL="postgres://eventatlas:eventatlas@$EVENTATLAS_POSTGRES_ADDRESS/eventatlas?sslmode=disable" \
go test -p=1 -tags=integration ./cmd/eventatlas -run TestOTLPToTopologyPipelinePersistsAcrossRuntimeReconstruction -count=1
