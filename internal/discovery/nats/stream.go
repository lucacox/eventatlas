package nats

import "context"

// Stream is the provider-native data needed to normalize one JetStream stream.
// SDK types are translated into this record at the client boundary.
type Stream struct {
	Name        string
	Description string
	Subjects    []string
	Retention   string
	Storage     string
	Replicas    int
	Consumers   []Consumer
}

// Consumer is the provider-native data needed to normalize one JetStream consumer.
type Consumer struct {
	Name           string
	Description    string
	Durable        bool
	FilterSubjects []string
	DeliverSubject string
}

// Client is the minimum JetStream discovery capability required by Provider.
type Client interface {
	ListStreams(ctx context.Context) ([]Stream, error)
}
