package nats

import (
	"strings"
	"testing"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestIdentitiesAreStableAndScoped(t *testing.T) {
	t.Parallel()

	sourceID := mustSourceID(t, "provider:nats:production-main")
	scope := mustDiscoveryScope(t, "account:ORDERS")
	identity := newIdentities(sourceID, scope)
	tests := []struct {
		name   string
		prefix string
		first  string
		second string
	}{
		{name: "broker", prefix: "broker:nats:", first: identity.brokerID().String(), second: newIdentities(sourceID, scope).brokerID().String()},
		{name: "destination", prefix: "destination:nats:", first: identity.destinationID("orders.created").String(), second: newIdentities(sourceID, scope).destinationID("orders.created").String()},
		{name: "stream", prefix: "resource:nats:", first: identity.streamID("ORDERS").String(), second: newIdentities(sourceID, scope).streamID("ORDERS").String()},
		{name: "consumer", prefix: "consumer:nats:", first: identity.consumerID("ORDERS", "billing").String(), second: newIdentities(sourceID, scope).consumerID("ORDERS", "billing").String()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.first != test.second {
				t.Errorf("stable identity mismatch: %q != %q", test.first, test.second)
			}
			if !strings.HasPrefix(test.first, test.prefix) {
				t.Errorf("identity = %q, want prefix %q", test.first, test.prefix)
			}
			if got := len(strings.TrimPrefix(test.first, test.prefix)); got != 64 {
				t.Errorf("identity digest length = %d, want 64", got)
			}
		})
	}

	otherScope := newIdentities(sourceID, mustDiscoveryScope(t, "account:PAYMENTS"))
	if identity.destinationID("orders.created") == otherScope.destinationID("orders.created") {
		t.Error("destination identity is equal across discovery scopes")
	}
	otherSource := newIdentities(mustSourceID(t, "provider:nats:secondary"), scope)
	if identity.streamID("ORDERS") == otherSource.streamID("ORDERS") {
		t.Error("stream identity is equal across provider sources")
	}
	if identity.destinationID("orders.created") == identity.destinationID("orders.updated") {
		t.Error("different subjects have equal destination identities")
	}
	if identity.consumerID("ORDERS", "billing") == identity.consumerID("PAYMENTS", "billing") {
		t.Error("same consumer name in different streams has equal identity")
	}
}

func TestGenerateSnapshotID(t *testing.T) {
	t.Parallel()

	first, err := generateSnapshotID()
	if err != nil {
		t.Fatalf("generateSnapshotID() error = %v", err)
	}
	second, err := generateSnapshotID()
	if err != nil {
		t.Fatalf("generateSnapshotID() error = %v", err)
	}
	if first == second {
		t.Errorf("separate snapshot IDs are equal: %q", first)
	}
	if !strings.HasPrefix(first.String(), "snapshot:nats:") || len(strings.TrimPrefix(first.String(), "snapshot:nats:")) != 32 {
		t.Errorf("snapshot ID = %q, want snapshot:nats followed by 128-bit hex", first)
	}
}

func TestStableOpaqueValueSeparatesParts(t *testing.T) {
	t.Parallel()

	if stableOpaqueValue("test", "a", "bc") == stableOpaqueValue("test", "ab", "c") {
		t.Error("length-prefixed identity parts produced a concatenation collision")
	}
}

func mustSourceID(t *testing.T, value string) topology.SourceID {
	t.Helper()
	sourceID, err := topology.NewSourceID(value)
	if err != nil {
		t.Fatalf("NewSourceID(%q) error = %v", value, err)
	}
	return sourceID
}

func mustDiscoveryScope(t *testing.T, value string) topology.DiscoveryScope {
	t.Helper()
	scope, err := topology.NewDiscoveryScope(value)
	if err != nil {
		t.Fatalf("NewDiscoveryScope(%q) error = %v", value, err)
	}
	return scope
}
