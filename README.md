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

EventAtlas discovers messaging infrastructure, normalizes it into a
vendor-neutral topology, and exposes that topology to clients through an API.
NATS and JetStream are the first integration, but the core is designed to
support other messaging systems without adopting provider-specific concepts.

## Status

EventAtlas is in its initial domain-model phase. The Go module, executable
entrypoint, core topology entities, evidence, edges, immutable discovery
snapshots, and provider discovery contract exist. The first NATS adapter can
discover JetStream streams, subjects, consumers, and consumer filters. It
normalizes `captured_by`, `has_consumer`, and `filters` relationships.
Persistence and API endpoints are not implemented yet.

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
│   ├── discovery/
│   │   ├── nats/        # NATS and JetStream discovery adapter
│   │   └── provider.go  # Provider discovery port
│   └── topology/        # Vendor-neutral topology domain model
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

- Go 1.26.6, matching the version declared in `go.mod`;
- Git.

A running NATS server and PostgreSQL are not required for unit tests or the
current executable. The NATS integration test requires a running Docker daemon
and starts an isolated `nats:2.14.4-alpine` container with JetStream enabled.

### Run the Backend

```bash
git clone https://github.com/lucacox/eventatlas.git
cd eventatlas
go mod download
go run ./cmd/eventatlas
```

The command currently prints a startup message and exits without starting a
server because the executable has not been implemented yet.

## Development

Run the standard Go checks before submitting changes:

```bash
go fmt ./...
go vet ./...
go test ./...
go build ./...
```

Run the NATS integration test against an isolated real broker:

```bash
make test-integration-nats
```

The test container uses a random local port, is stopped automatically, and
does not create a persistent volume. To target an already running broker
instead, run the tagged test with `EVENTATLAS_NATS_URL` set explicitly.

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

The first backend vertical slice will:

1. implement core topology types and invariants;
1. define the provider discovery contract;
1. discover JetStream streams, subjects, consumers, and filters;
1. normalize discovery into a topology snapshot;
1. expose `GET /api/v1/topology` using an in-memory implementation;
1. introduce PostgreSQL persistence after the flow is validated;
1. add OpenTelemetry observations after declared topology works end to end.

## Documentation

Cross-project architecture and decisions are maintained in
[eventatlas-docs](https://github.com/lucacox/eventatlas-docs).

- [Domain model](https://github.com/lucacox/eventatlas-docs/blob/main/architecture/domain-model.md)
- [Architecture decision records](https://github.com/lucacox/eventatlas-docs/tree/main/adrs)

## Related Repositories

| Repository | Responsibility |
| --- | --- |
| [eventatlas-web](https://github.com/lucacox/eventatlas-web) | Web application and graph visualization |
| [eventatlas-deploy](https://github.com/lucacox/eventatlas-deploy) | Local environments, containers, and deployment assets |
| [eventatlas-docs](https://github.com/lucacox/eventatlas-docs) | Cross-project architecture and documentation |

## License

See the [LICENSE](LICENSE) file for license information.
