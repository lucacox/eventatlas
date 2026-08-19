package nats

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/lucacox/eventatlas/internal/topology"
)

type identities struct {
	sourceID topology.SourceID
	scope    topology.DiscoveryScope
}

func newIdentities(sourceID topology.SourceID, scope topology.DiscoveryScope) identities {
	return identities{sourceID: sourceID, scope: scope}
}

func (identity identities) brokerID() topology.NodeID {
	return identity.nodeID("broker")
}

func (identity identities) destinationID(subject string) topology.NodeID {
	return identity.nodeID("destination", "subject", subject)
}

func (identity identities) streamID(name string) topology.NodeID {
	return identity.nodeID("resource", "nats.jetstream.stream", name)
}

func (identity identities) consumerID(stream, name string) topology.NodeID {
	return identity.nodeID("consumer", "nats.jetstream.consumer", stream, name)
}

func generateSnapshotID() (topology.SnapshotID, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return topology.SnapshotID{}, err
	}
	value := "snapshot:nats:" + hex.EncodeToString(random[:])
	snapshotID, err := topology.NewSnapshotID(value)
	if err != nil {
		return topology.SnapshotID{}, err
	}
	return snapshotID, nil
}

func (identity identities) nodeID(kind string, nativeParts ...string) topology.NodeID {
	parts := []string{identity.sourceID.String(), identity.scope.String()}
	parts = append(parts, nativeParts...)
	value := stableOpaqueValue(kind, parts...)
	nodeID, err := topology.NewNodeID(value)
	if err != nil {
		panic("generated an invalid node ID: " + err.Error())
	}
	return nodeID
}

func stableOpaqueValue(kind string, parts ...string) string {
	hash := sha256.New()
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(part))
	}
	return kind + ":nats:" + hex.EncodeToString(hash.Sum(nil))
}
