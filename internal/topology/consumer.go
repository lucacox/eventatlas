package topology

import "errors"

type Durability string

const (
	DurabilityDurable   Durability = "durable"
	DurabilityEphemeral Durability = "ephemeral"
	DurabilityUnknown   Durability = "unknown"
)

func (durability Durability) IsValid() bool {
	switch durability {
	case DurabilityDurable, DurabilityEphemeral, DurabilityUnknown:
		return true
	default:
		return false
	}
}

var (
	ErrConsumerKindInvalid       = errors.New("consumer kind is invalid")
	ErrConsumerDurabilityInvalid = errors.New("consumer durability is invalid")
	ErrConsumerBrokerIDInvalid   = errors.New("consumer broker ID is invalid")
)

type Consumer struct {
	Node
	consumerKind ConsumerKind
	durability   Durability
	brokerID     NodeID
}

func NewConsumer(id, name string, kind ConsumerKind, durability Durability, brokerID NodeID, attributes map[string]string) (*Consumer, error) {
	node, err := NewNode(id, NodeKindConsumer, name, attributes)
	if err != nil {
		return nil, err
	}
	if !kind.IsValid() {
		return nil, ErrConsumerKindInvalid
	}
	if !durability.IsValid() {
		return nil, ErrConsumerDurabilityInvalid
	}
	if brokerID.String() == "" {
		return nil, ErrConsumerBrokerIDInvalid
	}
	return &Consumer{
		Node:         node,
		consumerKind: kind,
		durability:   durability,
		brokerID:     brokerID,
	}, nil
}

func (c *Consumer) ConsumerKind() ConsumerKind {
	return c.consumerKind
}

func (c *Consumer) Durability() Durability {
	return c.durability
}

func (c *Consumer) BrokerID() NodeID {
	return c.brokerID
}
