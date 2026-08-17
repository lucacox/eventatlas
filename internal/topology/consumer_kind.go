package topology

import "strings"

type ConsumerKind string

func (kind ConsumerKind) IsValid() bool {
	return strings.Contains(string(kind), ".")
}
