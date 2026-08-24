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
	View  ViewResponse   `json:"view"`
	Nodes []NodeResponse `json:"nodes"`
	Edges []EdgeResponse `json:"edges"`
}

type ViewResponse struct {
	GeneratedAt time.Time               `json:"generatedAt"`
	Scope       string                  `json:"scope"`
	Sources     []ViewSourceResponse    `json:"sources"`
	Diagnostics ViewDiagnosticsResponse `json:"diagnostics"`
}

type ViewSourceResponse struct {
	SourceID string    `json:"sourceId"`
	Mode     string    `json:"mode"`
	LatestAt time.Time `json:"latestAt"`
}

type ViewDiagnosticsResponse struct {
	UnresolvedObservations uint64 `json:"unresolvedObservations"`
	AmbiguousObservations  uint64 `json:"ambiguousObservations"`
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

func newTopologyResponse(view *topology.TopologyView) TopologyResponse {
	sources := view.Sources()
	diagnostics := view.Diagnostics()
	response := TopologyResponse{
		View: ViewResponse{
			GeneratedAt: view.GeneratedAt(),
			Scope:       view.Scope().String(),
			Sources:     make([]ViewSourceResponse, 0, len(sources)),
			Diagnostics: ViewDiagnosticsResponse{
				UnresolvedObservations: diagnostics.UnresolvedObservations(),
				AmbiguousObservations:  diagnostics.AmbiguousObservations(),
			},
		},
		Nodes: make([]NodeResponse, 0, len(view.Nodes())),
		Edges: make([]EdgeResponse, 0, len(view.Edges())),
	}

	for _, source := range sources {
		response.View.Sources = append(response.View.Sources, ViewSourceResponse{
			SourceID: source.SourceID().String(),
			Mode:     string(source.Mode()),
			LatestAt: source.LatestAt(),
		})
	}
	for _, node := range view.Nodes() {
		response.Nodes = append(response.Nodes, newNodeResponse(node))
	}
	for _, edge := range view.Edges() {
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
