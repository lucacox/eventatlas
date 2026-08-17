package topology

import (
	"errors"
	"maps"
	"strings"
)

var ErrNodeNameEmpty = errors.New("node name cannot be blank")
var ErrNodeAttributeNameEmpty = errors.New("attribute name cannot be blank")

type AttributeNode interface {
	Attributes() map[string]string
	Attribute(name string) (string, bool)
	SetAttribute(name, value string) error
	DeleteAttribute(name string)
}

type TopologyNode interface {
	ID() NodeID
	Name() string
	AttributeNode
}

type Node struct {
	id         NodeID
	name       string
	attributes map[string]string
}

func NewNode(id, name string, attributes map[string]string) (Node, error) {
	nodeID, err := NewNodeID(id)
	if err != nil {
		return Node{}, err
	}
	if strings.TrimSpace(name) == "" {
		return Node{}, ErrNodeNameEmpty
	}
	attr := maps.Clone(attributes)
	if attr == nil {
		attr = make(map[string]string)
	}
	return Node{
		id:         nodeID,
		name:       strings.TrimSpace(name),
		attributes: attr,
	}, nil
}

func (n *Node) ID() NodeID {
	return n.id
}

func (n *Node) Name() string {
	return n.name
}

func (n *Node) Attribute(name string) (string, bool) {
	val, ok := n.attributes[name]
	return val, ok
}

func (n *Node) SetAttribute(name, value string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return ErrNodeAttributeNameEmpty
	}
	n.attributes[name] = value
	return nil
}

func (n *Node) DeleteAttribute(name string) {
	delete(n.attributes, name)
}

func (n *Node) Attributes() map[string]string {
	return maps.Clone(n.attributes)
}
