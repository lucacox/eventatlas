package topology

type ResourceKind string

func (kind ResourceKind) IsValid() bool {
	return isValidNamespacedKind(string(kind))
}
