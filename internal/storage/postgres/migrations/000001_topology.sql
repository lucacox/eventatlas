CREATE TABLE topology_snapshots (
    source_id TEXT NOT NULL,
    discovery_scope TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    completeness TEXT NOT NULL CHECK (completeness IN ('full', 'partial')),
    partial_errors JSONB NOT NULL CHECK (jsonb_typeof(partial_errors) = 'array'),
    cursor TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, discovery_scope),
    CHECK (
        (completeness = 'full' AND jsonb_array_length(partial_errors) = 0)
        OR (completeness = 'partial' AND jsonb_array_length(partial_errors) > 0)
    )
);

CREATE TABLE topology_nodes (
    source_id TEXT NOT NULL,
    discovery_scope TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_position INTEGER NOT NULL CHECK (node_position >= 0),
    kind TEXT NOT NULL CHECK (kind IN ('service', 'broker', 'destination', 'resource', 'consumer')),
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    broker_id TEXT,
    provider TEXT,
    environment TEXT,
    destination_kind TEXT,
    logical_name TEXT,
    resource_kind TEXT,
    consumer_kind TEXT,
    durability TEXT,
    attributes JSONB NOT NULL CHECK (jsonb_typeof(attributes) = 'object'),
    PRIMARY KEY (source_id, discovery_scope, node_id),
    UNIQUE (source_id, discovery_scope, node_position),
    FOREIGN KEY (source_id, discovery_scope)
        REFERENCES topology_snapshots (source_id, discovery_scope)
        ON DELETE CASCADE,
    CHECK (
        (kind = 'broker' AND provider IS NOT NULL AND environment IS NOT NULL AND broker_id IS NULL)
        OR (kind = 'service' AND environment IS NOT NULL AND broker_id IS NULL)
        OR (kind = 'destination' AND broker_id IS NOT NULL AND destination_kind IN ('subject', 'topic', 'queue', 'exchange'))
        OR (kind = 'resource' AND broker_id IS NOT NULL AND resource_kind IS NOT NULL)
        OR (kind = 'consumer' AND broker_id IS NOT NULL AND consumer_kind IS NOT NULL AND durability IN ('durable', 'ephemeral', 'unknown'))
    )
);

CREATE TABLE topology_edges (
    source_id TEXT NOT NULL,
    discovery_scope TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    edge_kind TEXT NOT NULL CHECK (edge_kind IN ('publishes', 'consumes', 'captured_by', 'has_consumer', 'executed_by', 'routes_to', 'belongs_to')),
    target_node_id TEXT NOT NULL,
    edge_position INTEGER NOT NULL CHECK (edge_position >= 0),
    PRIMARY KEY (source_id, discovery_scope, source_node_id, edge_kind, target_node_id),
    UNIQUE (source_id, discovery_scope, edge_position),
    FOREIGN KEY (source_id, discovery_scope, source_node_id)
        REFERENCES topology_nodes (source_id, discovery_scope, node_id)
        ON DELETE CASCADE,
    FOREIGN KEY (source_id, discovery_scope, target_node_id)
        REFERENCES topology_nodes (source_id, discovery_scope, node_id)
        ON DELETE CASCADE
);

CREATE TABLE topology_edge_evidence (
    source_id TEXT NOT NULL,
    discovery_scope TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    target_node_id TEXT NOT NULL,
    evidence_position INTEGER NOT NULL CHECK (evidence_position >= 0),
    evidence_source_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('declared', 'observed')),
    source_system TEXT NOT NULL CHECK (btrim(source_system) <> ''),
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
    PRIMARY KEY (
        source_id,
        discovery_scope,
        source_node_id,
        edge_kind,
        target_node_id,
        evidence_position
    ),
    FOREIGN KEY (source_id, discovery_scope, source_node_id, edge_kind, target_node_id)
        REFERENCES topology_edges (source_id, discovery_scope, source_node_id, edge_kind, target_node_id)
        ON DELETE CASCADE,
    CHECK (last_seen >= first_seen)
);
