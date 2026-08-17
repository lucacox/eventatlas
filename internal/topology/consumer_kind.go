package topology

type ConsumerKind string

func (kind ConsumerKind) IsValid() bool {
	return isValidNamespacedKind(string(kind))
}
