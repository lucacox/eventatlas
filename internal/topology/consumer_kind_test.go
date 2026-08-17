package topology

import "testing"

func TestConsumerKindIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		kind  ConsumerKind
		valid bool
	}{
		{name: "NATS consumer", kind: ConsumerKind("nats.jetstream.consumer"), valid: true},
		{name: "Kafka consumer group", kind: ConsumerKind("kafka.consumer_group"), valid: true},
		{name: "RabbitMQ consumer", kind: ConsumerKind("rabbitmq.consumer"), valid: true},
		{name: "zero value", kind: ConsumerKind(""), valid: false},
		{name: "missing namespace", kind: ConsumerKind("consumer"), valid: false},
		{name: "empty namespace", kind: ConsumerKind(".consumer"), valid: false},
		{name: "empty kind", kind: ConsumerKind("nats."), valid: false},
		{name: "empty middle segment", kind: ConsumerKind("nats..consumer"), valid: false},
		{name: "uppercase", kind: ConsumerKind("nats.Consumer"), valid: false},
		{name: "leading underscore", kind: ConsumerKind("nats._consumer"), valid: false},
		{name: "hyphen", kind: ConsumerKind("kafka.consumer-group"), valid: false},
		{name: "whitespace", kind: ConsumerKind(" kafka.consumer_group"), valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.kind.IsValid(); got != test.valid {
				t.Errorf("ConsumerKind(%q).IsValid() = %t, wants %t", test.kind, got, test.valid)
			}
		})
	}
}
