package topology

// EdgeKind identifies the semantic relationship between two topology nodes.
type EdgeKind string

const (
	EdgeKindPublishes   EdgeKind = "publishes"
	EdgeKindConsumes    EdgeKind = "consumes"
	EdgeKindCapturedBy  EdgeKind = "captured_by"
	EdgeKindHasConsumer EdgeKind = "has_consumer"
	EdgeKindExecutedBy  EdgeKind = "executed_by"
	EdgeKindRoutesTo    EdgeKind = "routes_to"
	EdgeKindBelongsTo   EdgeKind = "belongs_to"
)

// IsValid reports whether the kind belongs to the core topology model.
func (kind EdgeKind) IsValid() bool {
	switch kind {
	case EdgeKindPublishes,
		EdgeKindConsumes,
		EdgeKindCapturedBy,
		EdgeKindHasConsumer,
		EdgeKindExecutedBy,
		EdgeKindRoutesTo,
		EdgeKindBelongsTo:
		return true
	default:
		return false
	}
}
