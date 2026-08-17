package topology

type DestinationKind string

const (
	DestinationKindSubject  DestinationKind = "subject"
	DestinationKindTopic    DestinationKind = "topic"
	DestinationKindQueue    DestinationKind = "queue"
	DestinationKindExchange DestinationKind = "exchange"
)

func (kind DestinationKind) IsValid() bool {
	switch kind {
	case DestinationKindSubject,
		DestinationKindTopic,
		DestinationKindQueue,
		DestinationKindExchange:
		return true
	default:
		return false
	}
}
