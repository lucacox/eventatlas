package api

import (
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

type getTopologyOutput struct {
	Body TopologyResponse
}

// TopologyResponse is the provider-neutral representation exposed to clients.
type TopologyResponse struct {
	Snapshot SnapshotResponse `json:"snapshot"`
	Nodes    []NodeResponse   `json:"nodes"`
	Edges    []EdgeResponse   `json:"edges"`
}

type SnapshotResponse struct {
	ID            string            `json:"id"`
	SourceID      string            `json:"sourceId"`
	Scope         string            `json:"scope"`
	CapturedAt    time.Time         `json:"capturedAt"`
	Completeness  string            `json:"completeness"`
	PartialErrors []string          `json:"partialErrors"`
	Cursor        string            `json:"cursor,omitempty"`
	Metadata      map[string]string `json:"metadata"`
}

type NodeResponse struct {
	ID              string            `json:"id"`
	Kind            string            `json:"kind"`
	Name            string            `json:"name"`
	Attributes      map[string]string `json:"attributes"`
	BrokerID        string            `json:"brokerId,omitempty"`
	Provider        string            `json:"provider,omitempty"`
	Environment     string            `json:"environment,omitempty"`
	DestinationKind string            `json:"destinationKind,omitempty"`
	LogicalName     string            `json:"logicalName,omitempty"`
	ResourceKind    string            `json:"resourceKind,omitempty"`
	ConsumerKind    string            `json:"consumerKind,omitempty"`
	Durability      string            `json:"durability,omitempty"`
}

type EdgeResponse struct {
	SourceID string             `json:"sourceId"`
	TargetID string             `json:"targetId"`
	Kind     string             `json:"kind"`
	Evidence []EvidenceResponse `json:"evidence"`
}

type EvidenceResponse struct {
	SourceID     string            `json:"sourceId"`
	Mode         string            `json:"mode"`
	SourceSystem string            `json:"sourceSystem"`
	FirstSeen    time.Time         `json:"firstSeen"`
	LastSeen     time.Time         `json:"lastSeen"`
	Metadata     map[string]string `json:"metadata"`
}

func newTopologyResponse(snapshot *topology.TopologySnapshot) TopologyResponse {
	response := TopologyResponse{
		Snapshot: SnapshotResponse{
			ID:            snapshot.ID().String(),
			SourceID:      snapshot.SourceID().String(),
			Scope:         snapshot.Scope().String(),
			CapturedAt:    snapshot.CapturedAt(),
			Completeness:  string(snapshot.Completeness()),
			PartialErrors: snapshot.PartialErrors(),
			Cursor:        snapshot.Cursor(),
			Metadata:      snapshot.Metadata(),
		},
		Nodes: make([]NodeResponse, 0, len(snapshot.Nodes())),
		Edges: make([]EdgeResponse, 0, len(snapshot.Edges())),
	}

	for _, node := range snapshot.Nodes() {
		response.Nodes = append(response.Nodes, newNodeResponse(node))
	}
	for _, edge := range snapshot.Edges() {
		response.Edges = append(response.Edges, newEdgeResponse(edge))
	}
	return response
}

func newNodeResponse(node topology.TopologyNode) NodeResponse {
	response := NodeResponse{
		ID:         node.ID().String(),
		Kind:       string(node.Kind()),
		Name:       node.Name(),
		Attributes: node.Attributes(),
	}

	switch value := node.(type) {
	case *topology.Service:
		response.Environment = value.Environment()
	case *topology.Broker:
		response.Provider = value.Provider()
		response.Environment = value.Environment()
	case *topology.Destination:
		response.BrokerID = value.BrokerID().String()
		response.DestinationKind = string(value.DestinationKind())
		response.LogicalName = value.LogicalName()
	case *topology.MessagingResource:
		response.BrokerID = value.BrokerID().String()
		response.ResourceKind = string(value.ResourceKind())
	case *topology.Consumer:
		response.BrokerID = value.BrokerID().String()
		response.ConsumerKind = string(value.ConsumerKind())
		response.Durability = string(value.Durability())
	}
	return response
}

func newEdgeResponse(edge topology.Edge) EdgeResponse {
	evidence := edge.Evidence()
	response := EdgeResponse{
		SourceID: edge.SourceID().String(),
		TargetID: edge.TargetID().String(),
		Kind:     string(edge.Kind()),
		Evidence: make([]EvidenceResponse, 0, len(evidence)),
	}
	for _, item := range evidence {
		response.Evidence = append(response.Evidence, EvidenceResponse{
			SourceID:     item.SourceID().String(),
			Mode:         string(item.Mode()),
			SourceSystem: item.SourceSystem().String(),
			FirstSeen:    item.FirstSeen(),
			LastSeen:     item.LastSeen(),
			Metadata:     item.Metadata(),
		})
	}
	return response
}
