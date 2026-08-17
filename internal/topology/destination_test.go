package topology

import (
	"errors"
	"testing"
)

func TestNewDestination(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:messaging-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	tests := []struct {
		name            string
		id              string
		destinationName string
		kind            DestinationKind
		brokerID        NodeID
		logicalName     string
		wantErr         error
	}{
		{
			name:            "NATS subject",
			id:              "destination:production:orders.created",
			destinationName: " orders.created ",
			kind:            DestinationKindSubject,
			brokerID:        brokerID,
			logicalName:     " orders.* ",
		},
		{
			name:            "Kafka topic",
			id:              "destination:production:orders",
			destinationName: "orders",
			kind:            DestinationKindTopic,
			brokerID:        brokerID,
		},
		{
			name:            "RabbitMQ queue",
			id:              "destination:production:billing.orders",
			destinationName: "billing.orders",
			kind:            DestinationKindQueue,
			brokerID:        brokerID,
		},
		{
			name:            "RabbitMQ exchange",
			id:              "destination:production:orders-exchange",
			destinationName: "orders-exchange",
			kind:            DestinationKindExchange,
			brokerID:        brokerID,
		},
		{
			name:            "blank ID",
			id:              " ",
			destinationName: "orders.created",
			kind:            DestinationKindSubject,
			brokerID:        brokerID,
			wantErr:         ErrEmptyNodeID,
		},
		{
			name:            "blank name",
			id:              "destination:production:orders.created",
			destinationName: "\t",
			kind:            DestinationKindSubject,
			brokerID:        brokerID,
			wantErr:         ErrNodeNameEmpty,
		},
		{
			name:            "zero destination kind",
			id:              "destination:production:orders.created",
			destinationName: "orders.created",
			brokerID:        brokerID,
			wantErr:         ErrDestinationKindInvalid,
		},
		{
			name:            "unknown destination kind",
			id:              "destination:production:orders.created",
			destinationName: "orders.created",
			kind:            DestinationKind("stream"),
			brokerID:        brokerID,
			wantErr:         ErrDestinationKindInvalid,
		},
		{
			name:            "zero broker ID",
			id:              "destination:production:orders.created",
			destinationName: "orders.created",
			kind:            DestinationKindSubject,
			wantErr:         ErrDestinationBrokerIDInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			destination, err := NewDestination(
				test.id,
				test.destinationName,
				test.kind,
				test.brokerID,
				test.logicalName,
				nil,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("NewDestination() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewDestination() error = %v", err)
			}

			if destination.ID().String() != test.id {
				t.Errorf("Destination ID = %q, want %q", destination.ID(), test.id)
			}
			if destination.Kind() != NodeKindDestination {
				t.Errorf("Destination node kind = %q, want %q", destination.Kind(), NodeKindDestination)
			}
			if destination.DestinationKind() != test.kind {
				t.Errorf("Destination kind = %q, want %q", destination.DestinationKind(), test.kind)
			}
			if destination.BrokerID() != test.brokerID {
				t.Errorf("Destination broker ID = %q, want %q", destination.BrokerID(), test.brokerID)
			}
		})
	}
}

func TestNewDestinationNormalizesNames(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:nats-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	destination, err := NewDestination(
		"destination:production:orders.created",
		" orders.created ",
		DestinationKindSubject,
		brokerID,
		" orders.* ",
		nil,
	)
	if err != nil {
		t.Fatalf("NewDestination() error = %v", err)
	}

	if destination.Name() != "orders.created" {
		t.Errorf("Destination name = %q, want %q", destination.Name(), "orders.created")
	}
	if destination.LogicalName() != "orders.*" {
		t.Errorf("Destination logical name = %q, want %q", destination.LogicalName(), "orders.*")
	}
}

func TestNewDestinationClonesAttributes(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:nats-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	attributes := map[string]string{"nats.pattern": "orders.*"}
	destination, err := NewDestination(
		"destination:production:orders.created",
		"orders.created",
		DestinationKindSubject,
		brokerID,
		"orders.*",
		attributes,
	)
	if err != nil {
		t.Fatalf("NewDestination() error = %v", err)
	}

	attributes["nats.pattern"] = "changed"
	if pattern, _ := destination.Attribute("nats.pattern"); pattern != "orders.*" {
		t.Errorf("Destination pattern = %q, want %q", pattern, "orders.*")
	}

	returnedAttributes := destination.Attributes()
	returnedAttributes["nats.pattern"] = "changed"
	if pattern, _ := destination.Attribute("nats.pattern"); pattern != "orders.*" {
		t.Errorf("Destination pattern after returned map mutation = %q, want %q", pattern, "orders.*")
	}
}
