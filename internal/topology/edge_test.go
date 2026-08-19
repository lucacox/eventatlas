package topology

import (
	"errors"
	"testing"
	"time"
)

func TestNewEdgeAllowsDefinedEndpointPairs(t *testing.T) {
	t.Parallel()

	nodes := testEdgeNodes(t)
	evidence := []Evidence{testEdgeEvidence(t, "provider:nats:production-main")}

	tests := []struct {
		name   string
		source NodeKind
		target NodeKind
		kind   EdgeKind
	}{
		{name: "service publishes destination", source: NodeKindService, target: NodeKindDestination, kind: EdgeKindPublishes},
		{name: "service consumes destination", source: NodeKindService, target: NodeKindDestination, kind: EdgeKindConsumes},
		{name: "destination captured by resource", source: NodeKindDestination, target: NodeKindResource, kind: EdgeKindCapturedBy},
		{name: "resource has consumer", source: NodeKindResource, target: NodeKindConsumer, kind: EdgeKindHasConsumer},
		{name: "destination has consumer", source: NodeKindDestination, target: NodeKindConsumer, kind: EdgeKindHasConsumer},
		{name: "consumer filters destination", source: NodeKindConsumer, target: NodeKindDestination, kind: EdgeKindFilters},
		{name: "consumer executed by service", source: NodeKindConsumer, target: NodeKindService, kind: EdgeKindExecutedBy},
		{name: "destination routes to destination", source: NodeKindDestination, target: NodeKindDestination, kind: EdgeKindRoutesTo},
		{name: "destination routes to resource", source: NodeKindDestination, target: NodeKindResource, kind: EdgeKindRoutesTo},
		{name: "resource routes to destination", source: NodeKindResource, target: NodeKindDestination, kind: EdgeKindRoutesTo},
		{name: "resource routes to resource", source: NodeKindResource, target: NodeKindResource, kind: EdgeKindRoutesTo},
		{name: "destination belongs to broker", source: NodeKindDestination, target: NodeKindBroker, kind: EdgeKindBelongsTo},
		{name: "resource belongs to broker", source: NodeKindResource, target: NodeKindBroker, kind: EdgeKindBelongsTo},
		{name: "consumer belongs to broker", source: NodeKindConsumer, target: NodeKindBroker, kind: EdgeKindBelongsTo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			edge, err := NewEdge(nodes[test.source], nodes[test.target], test.kind, evidence)
			if err != nil {
				t.Fatalf("NewEdge() error = %v", err)
			}

			if edge.SourceID() != nodes[test.source].ID() {
				t.Errorf("Edge source ID = %q, want %q", edge.SourceID(), nodes[test.source].ID())
			}
			if edge.TargetID() != nodes[test.target].ID() {
				t.Errorf("Edge target ID = %q, want %q", edge.TargetID(), nodes[test.target].ID())
			}
			if edge.Kind() != test.kind {
				t.Errorf("Edge kind = %q, want %q", edge.Kind(), test.kind)
			}
			if len(edge.Evidence()) != 1 {
				t.Errorf("Edge evidence length = %d, want 1", len(edge.Evidence()))
			}
		})
	}
}

func TestNewEdgeRejectsInvalidEndpointPairs(t *testing.T) {
	t.Parallel()

	nodes := testEdgeNodes(t)
	evidence := []Evidence{testEdgeEvidence(t, "provider:nats:production-main")}

	tests := []struct {
		name   string
		source NodeKind
		target NodeKind
		kind   EdgeKind
	}{
		{name: "publishes is directed", source: NodeKindDestination, target: NodeKindService, kind: EdgeKindPublishes},
		{name: "consumes starts at service", source: NodeKindConsumer, target: NodeKindDestination, kind: EdgeKindConsumes},
		{name: "captured by targets resource", source: NodeKindDestination, target: NodeKindConsumer, kind: EdgeKindCapturedBy},
		{name: "has consumer targets consumer", source: NodeKindResource, target: NodeKindService, kind: EdgeKindHasConsumer},
		{name: "filters starts at consumer", source: NodeKindService, target: NodeKindDestination, kind: EdgeKindFilters},
		{name: "executed by targets service", source: NodeKindConsumer, target: NodeKindDestination, kind: EdgeKindExecutedBy},
		{name: "routes to excludes service", source: NodeKindService, target: NodeKindDestination, kind: EdgeKindRoutesTo},
		{name: "service is not broker owned", source: NodeKindService, target: NodeKindBroker, kind: EdgeKindBelongsTo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := NewEdge(nodes[test.source], nodes[test.target], test.kind, evidence); !errors.Is(err, ErrEdgeEndpointsInvalid) {
				t.Errorf("NewEdge() error = %v, want %v", err, ErrEdgeEndpointsInvalid)
			}
		})
	}
}

func TestNewEdgeRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	nodes := testEdgeNodes(t)
	evidence := []Evidence{testEdgeEvidence(t, "provider:nats:production-main")}
	var nilNode *Node
	zeroNode := &Node{}

	tests := []struct {
		name     string
		source   TopologyNode
		target   TopologyNode
		kind     EdgeKind
		evidence []Evidence
		wantErr  error
	}{
		{name: "nil source", target: nodes[NodeKindDestination], kind: EdgeKindPublishes, evidence: evidence, wantErr: ErrEdgeSourceInvalid},
		{name: "typed nil source", source: nilNode, target: nodes[NodeKindDestination], kind: EdgeKindPublishes, evidence: evidence, wantErr: ErrEdgeSourceInvalid},
		{name: "zero source", source: zeroNode, target: nodes[NodeKindDestination], kind: EdgeKindPublishes, evidence: evidence, wantErr: ErrEdgeSourceInvalid},
		{name: "nil target", source: nodes[NodeKindService], kind: EdgeKindPublishes, evidence: evidence, wantErr: ErrEdgeTargetInvalid},
		{name: "typed nil target", source: nodes[NodeKindService], target: nilNode, kind: EdgeKindPublishes, evidence: evidence, wantErr: ErrEdgeTargetInvalid},
		{name: "zero target", source: nodes[NodeKindService], target: zeroNode, kind: EdgeKindPublishes, evidence: evidence, wantErr: ErrEdgeTargetInvalid},
		{name: "invalid kind", source: nodes[NodeKindService], target: nodes[NodeKindDestination], kind: EdgeKind("sends"), evidence: evidence, wantErr: ErrEdgeKindInvalid},
		{name: "no evidence", source: nodes[NodeKindService], target: nodes[NodeKindDestination], kind: EdgeKindPublishes, wantErr: ErrEdgeEvidenceEmpty},
		{name: "zero evidence", source: nodes[NodeKindService], target: nodes[NodeKindDestination], kind: EdgeKindPublishes, evidence: []Evidence{{}}, wantErr: ErrEdgeEvidenceInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := NewEdge(test.source, test.target, test.kind, test.evidence); !errors.Is(err, test.wantErr) {
				t.Errorf("NewEdge() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestEdgeClonesEvidence(t *testing.T) {
	t.Parallel()

	nodes := testEdgeNodes(t)
	firstEvidence := testEdgeEvidence(t, "provider:nats:production-main")
	secondEvidence := testEdgeEvidence(t, "observation:otel:production")
	firstEvidence.metadata["nats.account"] = "ORDERS"
	input := []Evidence{firstEvidence}

	edge, err := NewEdge(
		nodes[NodeKindService],
		nodes[NodeKindDestination],
		EdgeKindPublishes,
		input,
	)
	if err != nil {
		t.Fatalf("NewEdge() error = %v", err)
	}

	input[0] = secondEvidence
	if got := edge.Evidence()[0].SourceID(); got != firstEvidence.SourceID() {
		t.Errorf("Edge evidence source after input mutation = %q, want %q", got, firstEvidence.SourceID())
	}
	firstEvidence.metadata["nats.account"] = "changed"
	if got := edge.Evidence()[0].Metadata()["nats.account"]; got != "ORDERS" {
		t.Errorf("Edge evidence metadata after input mutation = %q, want %q", got, "ORDERS")
	}

	returned := edge.Evidence()
	returned[0] = secondEvidence
	if got := edge.Evidence()[0].SourceID(); got != firstEvidence.SourceID() {
		t.Errorf("Edge evidence source after returned slice mutation = %q, want %q", got, firstEvidence.SourceID())
	}
	returned = edge.Evidence()
	returned[0].metadata["nats.account"] = "changed"
	if got := edge.Evidence()[0].Metadata()["nats.account"]; got != "ORDERS" {
		t.Errorf("Edge evidence metadata after returned metadata mutation = %q, want %q", got, "ORDERS")
	}
}

func testEdgeNodes(t *testing.T) map[NodeKind]TopologyNode {
	t.Helper()

	nodes := make(map[NodeKind]TopologyNode)
	for _, kind := range []NodeKind{
		NodeKindService,
		NodeKindBroker,
		NodeKindDestination,
		NodeKindResource,
		NodeKindConsumer,
	} {
		node, err := NewNode("node:"+string(kind), kind, string(kind), nil)
		if err != nil {
			t.Fatalf("NewNode(%q) error = %v", kind, err)
		}
		nodes[kind] = &node
	}
	return nodes
}

func testEdgeEvidence(t *testing.T, source string) Evidence {
	t.Helper()

	sourceID, err := NewSourceID(source)
	if err != nil {
		t.Fatalf("NewSourceID(%q) error = %v", source, err)
	}
	sourceSystem, err := NewSourceSystem("nats")
	if err != nil {
		t.Fatalf("NewSourceSystem() error = %v", err)
	}
	seenAt := time.Date(2026, time.August, 19, 10, 0, 0, 0, time.UTC)
	evidence, err := NewEvidence(
		sourceID,
		EvidenceModeDeclared,
		sourceSystem,
		seenAt,
		seenAt,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEvidence() error = %v", err)
	}
	return evidence
}
