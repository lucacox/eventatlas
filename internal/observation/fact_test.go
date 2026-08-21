package observation

import (
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestFactNormalizesIdentityAndClonesMetadata(t *testing.T) {
	t.Parallel()

	service, err := NewServiceIdentity(" development ", " commerce ", " checkout-api ")
	if err != nil {
		t.Fatalf("NewServiceIdentity() error = %v", err)
	}
	destination, err := NewDestinationHint(" NATS ", " orders.123 ", " orders.* ")
	if err != nil {
		t.Fatalf("NewDestinationHint() error = %v", err)
	}
	sourceID, _ := topology.NewSourceID("observation:otel:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	location := time.FixedZone("test", 2*60*60)
	observedAt := time.Date(2026, time.August, 21, 14, 0, 0, 0, location)
	metadata := map[string]string{"semconv.version": "1.39.0"}

	fact, err := NewFact(FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       observedAt,
		RelationshipKind: topology.EdgeKindPublishes,
		Service:          service,
		Destination:      destination,
		Metadata:         metadata,
	})
	if err != nil {
		t.Fatalf("NewFact() error = %v", err)
	}
	metadata["semconv.version"] = "changed"

	if got := fact.Service(); got.Environment() != "development" || got.Namespace() != "commerce" || got.Name() != "checkout-api" {
		t.Errorf("Service() = %#v, want normalized identity", got)
	}
	if got := fact.Destination(); got.MessagingSystem() != "nats" || got.PhysicalName() != "orders.123" || got.LogicalName() != "orders.*" {
		t.Errorf("Destination() = %#v, want normalized hint", got)
	}
	if got := fact.ObservedAt(); !got.Equal(observedAt) || got.Location() != time.UTC {
		t.Errorf("ObservedAt() = %v, want equal UTC time", got)
	}
	if got := fact.Metadata()["semconv.version"]; got != "1.39.0" {
		t.Errorf("Metadata() value = %q, want original", got)
	}
	returned := fact.Metadata()
	returned["semconv.version"] = "mutated"
	if got := fact.Metadata()["semconv.version"]; got != "1.39.0" {
		t.Errorf("Metadata() exposed internal map: got %q", got)
	}
}

func TestFactKeyExcludesTimeAndMetadata(t *testing.T) {
	t.Parallel()

	first := observationTestFact(t, time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC), "orders.1", "orders.*", map[string]string{"version": "one"})
	second := observationTestFact(t, time.Date(2026, time.August, 21, 11, 0, 0, 0, time.UTC), "orders.1", "orders.*", map[string]string{"version": "two"})
	different := observationTestFact(t, second.ObservedAt(), "billing.1", "billing.*", nil)

	if first.Key() != second.Key() {
		t.Error("equivalent facts produced different aggregate keys")
	}
	if first.Key() == different.Key() {
		t.Error("different destination hints produced the same aggregate key")
	}
}

func TestFactRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	service, _ := NewServiceIdentity("development", "", "checkout")
	destination, _ := NewDestinationHint("nats", "orders.*", "")
	sourceID, _ := topology.NewSourceID("observation:otel:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	valid := FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       time.Now(),
		RelationshipKind: topology.EdgeKindPublishes,
		Service:          service,
		Destination:      destination,
	}

	tests := []struct {
		name   string
		change func(*FactParams)
		want   error
	}{
		{name: "source", change: func(params *FactParams) { params.SourceID = topology.SourceID{} }, want: ErrFactSourceIDInvalid},
		{name: "scope", change: func(params *FactParams) { params.Scope = topology.DiscoveryScope{} }, want: ErrFactScopeInvalid},
		{name: "time", change: func(params *FactParams) { params.ObservedAt = time.Time{} }, want: ErrFactObservedAtZero},
		{name: "relationship", change: func(params *FactParams) { params.RelationshipKind = topology.EdgeKindConsumes }, want: ErrFactRelationshipUnsupported},
		{name: "service", change: func(params *FactParams) { params.Service = ServiceIdentity{} }, want: ErrFactServiceIdentityInvalid},
		{name: "destination", change: func(params *FactParams) { params.Destination = DestinationHint{} }, want: ErrFactDestinationHintInvalid},
		{name: "metadata key", change: func(params *FactParams) { params.Metadata = map[string]string{" ": "value"} }, want: ErrFactMetadataKeyEmpty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := valid
			test.change(&params)
			if _, err := NewFact(params); !errors.Is(err, test.want) {
				t.Errorf("NewFact() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestObservationIdentityConstructorsRejectBlankRequiredValues(t *testing.T) {
	t.Parallel()

	if _, err := NewServiceIdentity(" ", "", "service"); !errors.Is(err, ErrServiceEnvironmentEmpty) {
		t.Errorf("NewServiceIdentity(blank environment) error = %v", err)
	}
	if _, err := NewServiceIdentity("development", "", " "); !errors.Is(err, ErrServiceNameEmpty) {
		t.Errorf("NewServiceIdentity(blank name) error = %v", err)
	}
	if _, err := NewDestinationHint(" ", "orders.*", ""); !errors.Is(err, ErrDestinationMessagingSystemEmpty) {
		t.Errorf("NewDestinationHint(blank system) error = %v", err)
	}
	if _, err := NewDestinationHint("nats", " ", "orders.*"); !errors.Is(err, ErrDestinationPhysicalNameEmpty) {
		t.Errorf("NewDestinationHint(blank physical name) error = %v", err)
	}
}

func TestAggregateMergesLateAndNewerFacts(t *testing.T) {
	t.Parallel()

	middle := time.Date(2026, time.August, 21, 11, 0, 0, 0, time.UTC)
	aggregate, err := NewAggregate(observationTestFact(t, middle, "orders.1", "orders.*", map[string]string{"delivery": "middle"}))
	if err != nil {
		t.Fatalf("NewAggregate() error = %v", err)
	}
	aggregate, err = aggregate.Add(observationTestFact(t, middle.Add(-time.Hour), "orders.1", "orders.*", map[string]string{"delivery": "late"}))
	if err != nil {
		t.Fatalf("Add(late) error = %v", err)
	}
	aggregate, err = aggregate.Add(observationTestFact(t, middle.Add(time.Hour), "orders.1", "orders.*", map[string]string{"delivery": "latest"}))
	if err != nil {
		t.Fatalf("Add(latest) error = %v", err)
	}

	if got, want := aggregate.FirstSeen(), middle.Add(-time.Hour); !got.Equal(want) {
		t.Errorf("FirstSeen() = %v, want %v", got, want)
	}
	if got, want := aggregate.LastSeen(), middle.Add(time.Hour); !got.Equal(want) {
		t.Errorf("LastSeen() = %v, want %v", got, want)
	}
	if got := aggregate.ObservationCount(); got != 3 {
		t.Errorf("ObservationCount() = %d, want 3", got)
	}
	if got := aggregate.Metadata()["delivery"]; got != "latest" {
		t.Errorf("Metadata() delivery = %q, want latest", got)
	}

	different := observationTestFact(t, middle, "billing.1", "billing.*", nil)
	if _, err := aggregate.Add(different); !errors.Is(err, ErrAggregateKeyMismatch) {
		t.Errorf("Add(different key) error = %v, want %v", err, ErrAggregateKeyMismatch)
	}
	if _, err := NewAggregate(Fact{}); !errors.Is(err, ErrFactInvalid) {
		t.Errorf("NewAggregate(zero fact) error = %v, want %v", err, ErrFactInvalid)
	}
}

func observationTestFact(
	t *testing.T,
	observedAt time.Time,
	physicalName string,
	logicalName string,
	metadata map[string]string,
) Fact {
	t.Helper()
	service, _ := NewServiceIdentity("development", "commerce", "checkout-api")
	destination, _ := NewDestinationHint("nats", physicalName, logicalName)
	sourceID, _ := topology.NewSourceID("observation:otel:test")
	scope, _ := topology.NewDiscoveryScope("account:test")
	fact, err := NewFact(FactParams{
		SourceID:         sourceID,
		Scope:            scope,
		ObservedAt:       observedAt,
		RelationshipKind: topology.EdgeKindPublishes,
		Service:          service,
		Destination:      destination,
		Metadata:         metadata,
	})
	if err != nil {
		t.Fatalf("NewFact() error = %v", err)
	}
	return fact
}
