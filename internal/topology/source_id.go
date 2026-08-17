package topology

import (
	"errors"
	"strings"
)

var ErrEmptySourceID = errors.New("source ID must not be empty")

// SourceID identifies a provider or observation source instance.
type SourceID struct {
	value string
}

// NewSourceID rejects blank values but otherwise preserves the opaque identifier.
func NewSourceID(value string) (SourceID, error) {
	if strings.TrimSpace(value) == "" {
		return SourceID{}, ErrEmptySourceID
	}

	return SourceID{value: value}, nil
}

// String returns the opaque identifier without normalization.
func (id SourceID) String() string {
	return id.value
}
