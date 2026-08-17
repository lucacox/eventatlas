package topology

import (
	"strings"
)

type Service struct {
	Node
	environment string
}

func NewService(id string, name, environment string, attributes map[string]string) (*Service, error) {
	node, err := NewNode(id, name, attributes)
	if err != nil {
		return nil, err
	}
	return &Service{
		Node:        node,
		environment: strings.TrimSpace(environment),
	}, nil
}

func (s *Service) Environment() string {
	return s.environment
}
