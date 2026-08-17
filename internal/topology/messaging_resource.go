package topology

import "errors"

var (
	ErrMessagingResourceBrokerIDInvalid = errors.New("messaging resource broker ID is invalid")
	ErrMessagingResourceKindInvalid     = errors.New("messaging resource kind is invalid")
)

type MessagingResource struct {
	Node
	brokerID     NodeID
	resourceKind ResourceKind
}

func NewMessagingResource(id, name string, kind ResourceKind, brokerID NodeID, attributes map[string]string) (*MessagingResource, error) {
	node, err := NewNode(id, NodeKindResource, name, attributes)
	if err != nil {
		return nil, err
	}
	if !kind.IsValid() {
		return nil, ErrMessagingResourceKindInvalid
	}
	if brokerID.String() == "" {
		return nil, ErrMessagingResourceBrokerIDInvalid
	}
	return &MessagingResource{
		Node:         node,
		brokerID:     brokerID,
		resourceKind: kind,
	}, nil
}

func (resource *MessagingResource) BrokerID() NodeID {
	return resource.brokerID
}

func (resource *MessagingResource) ResourceKind() ResourceKind {
	return resource.resourceKind
}
