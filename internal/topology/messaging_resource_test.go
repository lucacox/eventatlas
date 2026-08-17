package topology

import (
	"errors"
	"testing"
)

func TestNewMessagingResource(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:nats-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	tests := []struct {
		name         string
		id           string
		resourceName string
		kind         ResourceKind
		brokerID     NodeID
		wantName     string
		wantErr      error
	}{
		{
			name:         "NATS JetStream stream",
			id:           "resource:production:nats-main:ORDERS",
			resourceName: " ORDERS ",
			kind:         ResourceKind("nats.jetstream.stream"),
			brokerID:     brokerID,
			wantName:     "ORDERS",
		},
		{
			name:         "provider resource with underscore",
			id:           "resource:production:azure-main:orders",
			resourceName: "orders",
			kind:         ResourceKind("azure.service_bus"),
			brokerID:     brokerID,
			wantName:     "orders",
		},
		{
			name:         "blank ID",
			id:           " ",
			resourceName: "ORDERS",
			kind:         ResourceKind("nats.jetstream.stream"),
			brokerID:     brokerID,
			wantErr:      ErrEmptyNodeID,
		},
		{
			name:         "blank name",
			id:           "resource:production:nats-main:ORDERS",
			resourceName: "\t",
			kind:         ResourceKind("nats.jetstream.stream"),
			brokerID:     brokerID,
			wantErr:      ErrNodeNameEmpty,
		},
		{
			name:         "zero resource kind",
			id:           "resource:production:nats-main:ORDERS",
			resourceName: "ORDERS",
			brokerID:     brokerID,
			wantErr:      ErrMessagingResourceKindInvalid,
		},
		{
			name:         "non-namespaced resource kind",
			id:           "resource:production:nats-main:ORDERS",
			resourceName: "ORDERS",
			kind:         ResourceKind("stream"),
			brokerID:     brokerID,
			wantErr:      ErrMessagingResourceKindInvalid,
		},
		{
			name:         "zero broker ID",
			id:           "resource:production:nats-main:ORDERS",
			resourceName: "ORDERS",
			kind:         ResourceKind("nats.jetstream.stream"),
			wantErr:      ErrMessagingResourceBrokerIDInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			resource, err := NewMessagingResource(
				test.id,
				test.resourceName,
				test.kind,
				test.brokerID,
				nil,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("NewMessagingResource() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewMessagingResource() error = %v", err)
			}

			if resource.ID().String() != test.id {
				t.Errorf("MessagingResource ID = %q, want %q", resource.ID(), test.id)
			}
			if resource.Kind() != NodeKindResource {
				t.Errorf("MessagingResource node kind = %q, want %q", resource.Kind(), NodeKindResource)
			}
			if resource.Name() != test.wantName {
				t.Errorf("MessagingResource name = %q, want %q", resource.Name(), test.wantName)
			}
			if resource.ResourceKind() != test.kind {
				t.Errorf("MessagingResource kind = %q, want %q", resource.ResourceKind(), test.kind)
			}
			if resource.BrokerID() != test.brokerID {
				t.Errorf("MessagingResource broker ID = %q, want %q", resource.BrokerID(), test.brokerID)
			}
		})
	}
}

func TestNewMessagingResourceClonesAttributes(t *testing.T) {
	t.Parallel()

	brokerID, err := NewNodeID("broker:production:nats-main")
	if err != nil {
		t.Fatalf("NewNodeID() error = %v", err)
	}

	attributes := map[string]string{"nats.jetstream.retention": "limits"}
	resource, err := NewMessagingResource(
		"resource:production:nats-main:ORDERS",
		"ORDERS",
		ResourceKind("nats.jetstream.stream"),
		brokerID,
		attributes,
	)
	if err != nil {
		t.Fatalf("NewMessagingResource() error = %v", err)
	}

	attributes["nats.jetstream.retention"] = "changed"
	if retention, _ := resource.Attribute("nats.jetstream.retention"); retention != "limits" {
		t.Errorf("MessagingResource retention = %q, want %q", retention, "limits")
	}

	returnedAttributes := resource.Attributes()
	returnedAttributes["nats.jetstream.retention"] = "changed"
	if retention, _ := resource.Attribute("nats.jetstream.retention"); retention != "limits" {
		t.Errorf("MessagingResource retention after returned map mutation = %q, want %q", retention, "limits")
	}
}
