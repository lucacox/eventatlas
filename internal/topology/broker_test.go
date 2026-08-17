package topology

import (
	"errors"
	"testing"
)

func TestNewBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		id          string
		brokerName  string
		provider    string
		environment string
		wantErr     error
	}{
		{
			name:        "valid broker",
			id:          "broker:production:nats-main",
			brokerName:  " NATS Main ",
			provider:    " nats ",
			environment: " production ",
		},
		{
			name:        "blank ID",
			id:          " ",
			brokerName:  "NATS Main",
			provider:    "nats",
			environment: "production",
			wantErr:     ErrEmptyNodeID,
		},
		{
			name:        "blank name",
			id:          "broker:production:nats-main",
			brokerName:  " ",
			provider:    "nats",
			environment: "production",
			wantErr:     ErrNodeNameEmpty,
		},
		{
			name:        "blank provider",
			id:          "broker:production:nats-main",
			brokerName:  "NATS Main",
			provider:    "\t",
			environment: "production",
			wantErr:     ErrBrokerProviderEmpty,
		},
		{
			name:        "blank environment",
			id:          "broker:production:nats-main",
			brokerName:  "NATS Main",
			provider:    "nats",
			environment: "\n",
			wantErr:     ErrBrokerEnvironmentEmpty,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			broker, err := NewBroker(
				test.id,
				test.brokerName,
				test.provider,
				test.environment,
				nil,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("NewBroker() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewBroker() error = %v", err)
			}

			if broker.ID().String() != test.id {
				t.Errorf("Broker ID = %q, want %q", broker.ID(), test.id)
			}
			if broker.Kind() != NodeKindBroker {
				t.Errorf("Broker kind = %q, want %q", broker.Kind(), NodeKindBroker)
			}
			if broker.Name() != "NATS Main" {
				t.Errorf("Broker name = %q, want %q", broker.Name(), "NATS Main")
			}
			if broker.Provider() != "nats" {
				t.Errorf("Broker provider = %q, want %q", broker.Provider(), "nats")
			}
			if broker.Environment() != "production" {
				t.Errorf("Broker environment = %q, want %q", broker.Environment(), "production")
			}
		})
	}
}

func TestNewBrokerClonesAttributes(t *testing.T) {
	t.Parallel()

	attributes := map[string]string{"region": "eu-west-1"}
	broker, err := NewBroker(
		"broker:production:nats-main",
		"NATS Main",
		"nats",
		"production",
		attributes,
	)
	if err != nil {
		t.Fatalf("NewBroker() error = %v", err)
	}

	attributes["region"] = "changed"
	if region, _ := broker.Attribute("region"); region != "eu-west-1" {
		t.Errorf("Broker region = %q, want %q", region, "eu-west-1")
	}

	returnedAttributes := broker.Attributes()
	returnedAttributes["region"] = "changed"
	if region, _ := broker.Attribute("region"); region != "eu-west-1" {
		t.Errorf("Broker region after returned map mutation = %q, want %q", region, "eu-west-1")
	}
}
