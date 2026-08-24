CREATE TABLE topology_observations (
    source_id TEXT NOT NULL,
    discovery_scope TEXT NOT NULL,
    relationship_kind TEXT NOT NULL CHECK (relationship_kind IN ('publishes', 'consumes')),
    service_environment TEXT NOT NULL CHECK (btrim(service_environment) <> ''),
    service_namespace TEXT NOT NULL DEFAULT '',
    service_name TEXT NOT NULL CHECK (btrim(service_name) <> ''),
    messaging_system TEXT NOT NULL CHECK (btrim(messaging_system) <> ''),
    destination_physical_name TEXT NOT NULL CHECK (btrim(destination_physical_name) <> ''),
    destination_logical_name TEXT NOT NULL DEFAULT '',
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    observation_count BIGINT NOT NULL CHECK (observation_count > 0),
    metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        source_id,
        discovery_scope,
        relationship_kind,
        service_environment,
        service_namespace,
        service_name,
        messaging_system,
        destination_physical_name,
        destination_logical_name
    ),
    CHECK (last_seen >= first_seen)
);

CREATE INDEX topology_observations_active_scope_idx
    ON topology_observations (discovery_scope, last_seen DESC);
