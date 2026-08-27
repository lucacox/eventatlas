---
post_title: EventAtlas Backend
author1: Luca Cossaro
post_slug: eventatlas-backend
featured_image: ""
categories:
  - development
tags:
  - eventatlas
  - go
  - event-driven-architecture
ai_note: AI-assisted and reviewed by Luca Cossaro.
summary: Backend, discovery, topology engine, storage, and API for EventAtlas.
post_date: 2026-08-16
---

# EventAtlas

**Discover and visualize your event-driven topology.**

[![Backend CI](https://github.com/lucacox/eventatlas/actions/workflows/ci.yml/badge.svg?branch=development)](https://github.com/lucacox/eventatlas/actions/workflows/ci.yml)
[![Backend Integration](https://github.com/lucacox/eventatlas/actions/workflows/integration.yml/badge.svg?branch=development)](https://github.com/lucacox/eventatlas/actions/workflows/integration.yml)

EventAtlas discovers messaging infrastructure, normalizes it into a
vendor-neutral topology, and exposes that topology to clients through an API.
NATS and JetStream are the first integration, but the core is designed to
support other messaging systems without adopting provider-specific concepts.

## Status

EventAtlas has its first read-only backend vertical slice. The executable
discovers JetStream streams, subjects, consumers, and consumer selectors at
startup and periodically thereafter. It normalizes them into a vendor-neutral
topology snapshot and exposes it through `GET /api/v1/topology`. The latest
reconciled snapshot can be stored durably in PostgreSQL and restored after a
restart; without a database URL, the backend uses the in-memory adapter. The
API publishes an OpenAPI document and interactive documentation through Huma.

The OpenTelemetry phase has started with the provider-neutral observation
fact, aggregation port, in-memory and PostgreSQL adapters, and merged topology
projector. OTLP transport, runtime wiring, and API exposure are not implemented
yet.

There is no usable release at this stage.

## Responsibilities

This repository owns the EventAtlas backend:

- the vendor-neutral domain model and topology invariants;
- messaging provider discovery and normalization;
- topology reconciliation and observation ingestion;
- durable persistence;
- the REST API;
- the backend executable and process lifecycle.

The web application, deployment assets, and cross-project documentation live
in separate repositories.

## Architecture

The initial backend follows these constraints:

- one Go executable and operating system process;
- explicit internal boundaries using ports and adapters;
- a provider-neutral core with NATS and JetStream as the first adapter;
- separate declared and observed topology evidence;
- OpenTelemetry as an observation source, not a broker provider;
- PostgreSQL as the initial durable persistence layer;
- REST and JSON for the initial public API.

Provider SDKs, database types, HTTP details, and telemetry transports must
remain outside the domain model.

## Current Structure

```text
.
├── cmd/
│   └── eventatlas/
│       └── main.go  # Backend entrypoint
├── internal/
│   ├── api/          # Huma HTTP adapter and public response model
│   ├── application/  # Topology refresh and query use cases
│   ├── discovery/
│   │   ├── nats/     # NATS and JetStream discovery adapter
│   │   └── provider.go
│   ├── observation/   # Provider-neutral runtime facts and aggregates
│   ├── storage/
│   │   ├── memory/   # Volatile topology store adapter
│   │   └── postgres/ # Durable store and embedded migrations
│   └── topology/     # Vendor-neutral topology domain model
├── go.mod
├── go.sum
├── LICENSE
└── README.md
```

Additional packages will be introduced incrementally as the first vertical
slice is implemented. The repository favors `internal` packages for
application and adapter code that is not intended for external consumers.

## Getting Started

### Prerequisites

- Go 1.27.0, matching the version declared in `go.mod`;
- Git.

A JetStream-enabled NATS server is required for discovery. PostgreSQL is
optional for local demonstrations and required for durable storage. Unit tests
need neither service. The integration suite requires a running Docker daemon
and starts isolated NATS and PostgreSQL containers.

### Run the Backend

```bash
git clone https://github.com/lucacox/eventatlas.git
cd eventatlas
go mod download
go run ./cmd/eventatlas
```

With the defaults, EventAtlas discovers `nats://127.0.0.1:4222`, refreshes the
topology every minute, keeps declared topology and runtime observations in
memory, and listens on `:8080`. OTLP ingestion remains disabled until an OTLP
listener address is configured. Set `EVENTATLAS_DATABASE_URL` to use one
PostgreSQL pool for both stores and run their automatic migrations. The
available HTTP resources are:

| Resource | URL |
| --- | --- |
| Current merged topology view | `http://localhost:8080/api/v1/topology` |
| Interactive API documentation | `http://localhost:8080/docs` |
| OpenAPI 3.1 JSON | `http://localhost:8080/openapi.json` |
| OTLP/HTTP traces, when enabled | `http://localhost:4318/v1/traces` |

Runtime configuration is read from environment variables:

| Variable | Default |
| --- | --- |
| `EVENTATLAS_HTTP_ADDRESS` | `:8080` |
| `EVENTATLAS_NATS_URL` | `nats://127.0.0.1:4222` |
| `EVENTATLAS_DATABASE_URL` | empty; uses in-memory storage |
| `EVENTATLAS_NATS_SOURCE_ID` | `provider:nats:default` |
| `EVENTATLAS_NATS_BROKER_NAME` | `NATS` |
| `EVENTATLAS_NATS_DISCOVERY_SCOPE` | `account:default` |
| `EVENTATLAS_ENVIRONMENT` | `development` |
| `EVENTATLAS_OTLP_HTTP_ADDRESS` | empty; OTLP ingestion disabled |
| `EVENTATLAS_OTLP_SOURCE_ID` | `observation:otel:default` |
| `EVENTATLAS_OTLP_MAX_REQUEST_BYTES` | `67108864` (64 MiB) |
| `EVENTATLAS_OTLP_MAX_FUTURE_SKEW` | `5m` |
| `EVENTATLAS_OBSERVATION_RETENTION` | `24h` |
| `EVENTATLAS_DISCOVERY_TIMEOUT` | `10s` |
| `EVENTATLAS_DATABASE_TIMEOUT` | `10s` |
| `EVENTATLAS_REFRESH_INTERVAL` | `1m` |
| `EVENTATLAS_SHUTDOWN_TIMEOUT` | `10s` |

Periodic attempts do not overlap. A failed refresh is logged and leaves the
latest successful snapshot available to API clients; the next scheduled
attempt still runs. With PostgreSQL configured, startup can serve the last
persisted snapshot when the initial NATS discovery attempt fails.

When `EVENTATLAS_OTLP_HTTP_ADDRESS` is set, EventAtlas starts a separate
OTLP/HTTP listener that accepts binary Protobuf trace requests, with optional
gzip compression, on `POST /v1/traces`. The observation slice recognizes NATS
messaging `send` and `process` spans. Send spans produce observed `publishes`
relationships and process spans produce observed `consumes` relationships. The
configured byte limit applies to both the on-wire request and its decompressed
representation.

The topology API projects the latest declared snapshot together with active
observations on every read. An observation remains active for
`EVENTATLAS_OBSERVATION_RETENTION`; expiry removes only its observed evidence
from the view and never deletes provider-declared topology.

## Development

Run the standard Go checks before submitting changes:

```bash
go fmt ./...
go vet ./...
go test ./...
go build ./...
```

Run the provider and HTTP API integration tests against an isolated real
broker:

```bash
make test-integration-nats
```

Run the PostgreSQL migration, round-trip, transactional replacement, and
persisted-startup fallback tests:

```bash
make test-integration-postgres
```

Run the end-to-end OTLP-to-topology scenario with isolated NATS and PostgreSQL
containers:

```bash
make test-integration-e2e
```

Run both integration suites:

```bash
make test-integration
```

The tests exercise direct provider discovery and the complete NATS-to-Huma
JSON flow, including discovery of a stream created after the initial snapshot
without restarting the API. The test container uses a random local port, is
stopped automatically, and does not create a persistent volume. To target an
already running broker instead, run the tagged tests with
`EVENTATLAS_NATS_URL` set explicitly.

Keep dependencies minimal and run `go mod tidy` after adding or removing
imports.

## Contributing

EventAtlas is at an early stage, so focused changes that preserve the documented
architecture are preferred.

1. Create a dedicated branch from the current development baseline.
1. Keep each change limited to one concern and include tests for new behavior.
1. Run the standard checks documented in the [Development](#development)
  section.
1. Update this README when setup, configuration, commands, or behavior change.
1. Update
  [eventatlas-docs](https://github.com/lucacox/eventatlas-docs) when a change
  affects cross-project architecture or an accepted decision.
1. Open a pull request that explains the problem, the chosen approach, and how
  the change was verified.

Before implementing a change that contradicts an accepted ADR or introduces a
new cross-project architectural constraint, discuss and document the decision
in `eventatlas-docs`.

Go contributions should remain idiomatic, keep dependencies minimal, preserve
the provider-neutral domain boundary, and avoid coupling domain logic to
provider SDKs or infrastructure adapters.

## Initial Delivery Plan

The initial delivery sequence is:

1. implement core topology types and invariants — complete;
1. define the provider discovery contract — complete;
1. discover JetStream streams, subjects, consumers, and filters — complete;
1. normalize discovery into a topology snapshot — complete;
1. expose `GET /api/v1/topology` using an in-memory implementation — complete;
1. periodically reconcile the in-memory topology — complete;
1. introduce PostgreSQL persistence after the flow is validated — complete;
1. add OpenTelemetry observations after declared topology works end to end — in progress.

## Documentation

Cross-project architecture and decisions are maintained in
[eventatlas-docs](https://github.com/lucacox/eventatlas-docs).

- [Domain model](https://github.com/lucacox/eventatlas-docs/blob/main/architecture/domain-model.md)
- [Observation architecture](https://github.com/lucacox/eventatlas-docs/blob/main/architecture/observations.md)
- [Architecture decision records](https://github.com/lucacox/eventatlas-docs/tree/main/adrs)

## Related Repositories

| Repository | Responsibility |
| --- | --- |
| [eventatlas-web](https://github.com/lucacox/eventatlas-web) | Web application and graph visualization |
| [eventatlas-deploy](https://github.com/lucacox/eventatlas-deploy) | Local environments, containers, and deployment assets |
| [eventatlas-docs](https://github.com/lucacox/eventatlas-docs) | Cross-project architecture and documentation |

## License

See the [LICENSE](LICENSE) file for license information.
