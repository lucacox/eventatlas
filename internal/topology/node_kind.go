package topology

// NodeKind identifies the domain role of a topology node.
type NodeKind string

const (
	NodeKindService     NodeKind = "service"
	NodeKindBroker      NodeKind = "broker"
	NodeKindDestination NodeKind = "destination"
	NodeKindResource    NodeKind = "resource"
	NodeKindConsumer    NodeKind = "consumer"
)

// IsValid reports whether the kind belongs to the core topology model.
func (kind NodeKind) IsValid() bool {
	switch kind {
	case NodeKindService,
		NodeKindBroker,
		NodeKindDestination,
		NodeKindResource,
		NodeKindConsumer:
		return true
	default:
		return false
	}
}
