package topology

import (
	"errors"
	"strings"
)

type Broker struct {
	Node
	provider    string
	environment string
}

var ErrBrokerProviderEmpty = errors.New("broker provider cannot be blank")
var ErrBrokerEnvironmentEmpty = errors.New("broker environment cannot be blank")

func NewBroker(id string, name, provider, environment string, attributes map[string]string) (*Broker, error) {
	node, err := NewNode(id, NodeKindBroker, name, attributes)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(provider) == "" {
		return nil, ErrBrokerProviderEmpty
	}
	if strings.TrimSpace(environment) == "" {
		return nil, ErrBrokerEnvironmentEmpty
	}
	return &Broker{
		Node:        node,
		provider:    strings.TrimSpace(provider),
		environment: strings.TrimSpace(environment),
	}, nil
}

func (b *Broker) Provider() string {
	return b.provider
}

func (b *Broker) Environment() string {
	return b.environment
}
