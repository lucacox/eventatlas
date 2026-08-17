package topology

import (
	"errors"
	"testing"
)

func TestDurabilityIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		durability Durability
		valid      bool
	}{
		{name: "durable", durability: DurabilityDurable, valid: true},
		{name: "ephemeral", durability: DurabilityEphemeral, valid: true},
		{name: "unknown", durability: DurabilityUnknown, valid: true},
		{name: "zero value", durability: Durability(""), valid: false},
		{name: "unsupported", durability: Durability("persistent"), valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.durability.IsValid(); got != test.valid {
				t.Errorf("Durability(%q).IsValid() = %t, want %t", test.durability, got, test.valid)
			}
		})
	}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:messaging-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	tests := []struct {
		name         string
		id           string
		consumerName string
		kind         ConsumerKind
		durability   Durability
		brokerID     NodeID
		wantName     string
		wantErr      error
	}{
		{
			name:         "durable NATS consumer",
			id:           "consumer:production:nats-main:billing",
			consumerName: " billing ",
			kind:         ConsumerKind("nats.jetstream.consumer"),
			durability:   DurabilityDurable,
			brokerID:     brokerID,
			wantName:     "billing",
		},
		{
			name:         "ephemeral RabbitMQ consumer",
			id:           "consumer:production:rabbitmq-main:worker",
			consumerName: "worker",
			kind:         ConsumerKind("rabbitmq.consumer"),
			durability:   DurabilityEphemeral,
			brokerID:     brokerID,
			wantName:     "worker",
		},
		{
			name:         "Kafka consumer group with unknown durability",
			id:           "consumer:production:kafka-main:billing",
			consumerName: "billing",
			kind:         ConsumerKind("kafka.consumer_group"),
			durability:   DurabilityUnknown,
			brokerID:     brokerID,
			wantName:     "billing",
		},
		{
			name:         "blank ID",
			id:           " ",
			consumerName: "billing",
			kind:         ConsumerKind("nats.jetstream.consumer"),
			durability:   DurabilityDurable,
			brokerID:     brokerID,
			wantErr:      ErrEmptyNodeID,
		},
		{
			name:         "blank name",
			id:           "consumer:production:nats-main:billing",
			consumerName: "\t",
			kind:         ConsumerKind("nats.jetstream.consumer"),
			durability:   DurabilityDurable,
			brokerID:     brokerID,
			wantErr:      ErrNodeNameEmpty,
		},
		{
			name:         "zero consumer kind",
			id:           "consumer:production:nats-main:billing",
			consumerName: "billing",
			durability:   DurabilityDurable,
			brokerID:     brokerID,
			wantErr:      ErrConsumerKindInvalid,
		},
		{
			name:         "non-namespaced consumer kind",
			id:           "consumer:production:nats-main:billing",
			consumerName: "billing",
			kind:         ConsumerKind("consumer"),
			durability:   DurabilityDurable,
			brokerID:     brokerID,
			wantErr:      ErrConsumerKindInvalid,
		},
		{
			name:         "zero durability",
			id:           "consumer:production:nats-main:billing",
			consumerName: "billing",
			kind:         ConsumerKind("nats.jetstream.consumer"),
			brokerID:     brokerID,
			wantErr:      ErrConsumerDurabilityInvalid,
		},
		{
			name:         "unsupported durability",
			id:           "consumer:production:nats-main:billing",
			consumerName: "billing",
			kind:         ConsumerKind("nats.jetstream.consumer"),
			durability:   Durability("persistent"),
			brokerID:     brokerID,
			wantErr:      ErrConsumerDurabilityInvalid,
		},
		{
			name:         "zero broker ID",
			id:           "consumer:production:nats-main:billing",
			consumerName: "billing",
			kind:         ConsumerKind("nats.jetstream.consumer"),
			durability:   DurabilityDurable,
			wantErr:      ErrConsumerBrokerIDInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			consumer, err := NewConsumer(
				test.id,
				test.consumerName,
				test.kind,
				test.durability,
				test.brokerID,
				nil,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("NewConsumer() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConsumer() error = %v", err)
			}

			if consumer.ID().String() != test.id {
				t.Errorf("Consumer ID = %q, want %q", consumer.ID(), test.id)
			}
			if consumer.Kind() != NodeKindConsumer {
				t.Errorf("Consumer node kind = %q, want %q", consumer.Kind(), NodeKindConsumer)
			}
			if consumer.Name() != test.wantName {
				t.Errorf("Consumer name = %q, want %q", consumer.Name(), test.wantName)
			}
			if consumer.ConsumerKind() != test.kind {
				t.Errorf("Consumer kind = %q, want %q", consumer.ConsumerKind(), test.kind)
			}
			if consumer.Durability() != test.durability {
				t.Errorf("Consumer durability = %q, want %q", consumer.Durability(), test.durability)
			}
			if consumer.BrokerID() != test.brokerID {
				t.Errorf("Consumer broker ID = %q, want %q", consumer.BrokerID(), test.brokerID)
			}
		})
	}
}

func TestNewConsumerClonesAttributes(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:nats-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	attributes := map[string]string{"nats.filter_subject": "orders.*"}
	consumer, err := NewConsumer(
		"consumer:production:nats-main:billing",
		"billing",
		ConsumerKind("nats.jetstream.consumer"),
		DurabilityDurable,
		brokerID,
		attributes,
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	attributes["nats.filter_subject"] = "changed"
	if filter, _ := consumer.Attribute("nats.filter_subject"); filter != "orders.*" {
		t.Errorf("Consumer filter = %q, want %q", filter, "orders.*")
	}

	returnedAttributes := consumer.Attributes()
	returnedAttributes["nats.filter_subject"] = "changed"
	if filter, _ := consumer.Attribute("nats.filter_subject"); filter != "orders.*" {
		t.Errorf("Consumer filter after returned map mutation = %q, want %q", filter, "orders.*")
	}
}
