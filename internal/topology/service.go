package topology

import (
	"errors"
	"strings"
)

var ErrServiceEnvironmentEmpty = errors.New("service environment cannot be blank")

type Service struct {
	Node
	environment string
}

func NewService(id string, name, environment string, attributes map[string]string) (*Service, error) {
	node, err := NewNode(id, NodeKindService, name, attributes)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(environment) == "" {
		return nil, ErrServiceEnvironmentEmpty
	}
	return &Service{
		Node:        node,
		environment: strings.TrimSpace(environment),
	}, nil
}

func (s *Service) Environment() string {
	return s.environment
}
