package topology

import (
	"errors"
	"strings"
)

var ErrEmptyNodeID = errors.New("node ID must not be empty")

// NodeID identifies a topology node without imposing provider-specific semantics.
type NodeID struct {
	value string
}

// NewNodeID rejects blank values but otherwise preserves the opaque identifier.
func NewNodeID(value string) (NodeID, error) {
	if strings.TrimSpace(value) == "" {
		return NodeID{}, ErrEmptyNodeID
	}

	return NodeID{value: value}, nil
}

// String returns the opaque identifier without normalization.
func (id NodeID) String() string {
	return id.value
}
