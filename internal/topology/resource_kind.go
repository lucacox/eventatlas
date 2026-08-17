package topology

import "strings"

type ResourceKind string

func (kind ResourceKind) IsValid() bool {
	return strings.Contains(string(kind), ".")
}
