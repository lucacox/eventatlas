package topology

import (
	"errors"
	"strings"
)

type Destination struct {
	Node
	destinationKind DestinationKind
	brokerID        NodeID
	logicalName     string
}

var (
	ErrDestinationBrokerIDInvalid = errors.New("destination broker ID is invalid")
	ErrDestinationKindInvalid     = errors.New("destination kind is invalid")
)

func NewDestination(id, name string, kind DestinationKind, brokerID NodeID, logicalName string, attributes map[string]string) (*Destination, error) {
	node, err := NewNode(id, NodeKindDestination, name, attributes)
	if err != nil {
		return nil, err
	}
	if !kind.IsValid() {
		return nil, ErrDestinationKindInvalid
	}
	if brokerID.String() == "" {
		return nil, ErrDestinationBrokerIDInvalid
	}
	return &Destination{
		Node:            node,
		destinationKind: kind,
		brokerID:        brokerID,
		logicalName:     strings.TrimSpace(logicalName),
	}, nil
}

func (d *Destination) DestinationKind() DestinationKind {
	return d.destinationKind
}

func (d *Destination) BrokerID() NodeID {
	return d.brokerID
}

func (d *Destination) LogicalName() string {
	return d.logicalName
}
