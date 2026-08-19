package nats

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamClientListsStreams(t *testing.T) {
	t.Parallel()

	info := &jetstream.StreamInfo{Config: jetstream.StreamConfig{
		Name:        "ORDERS",
		Description: "Order events",
		Subjects:    []string{"orders.*", "orders.audit"},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.MemoryStorage,
		Replicas:    3,
	}}
	consumerInfos := []*jetstream.ConsumerInfo{
		{
			Name: "billing",
			Config: jetstream.ConsumerConfig{
				Durable:       "billing",
				Description:   "Billing worker",
				FilterSubject: "orders.*",
			},
		},
		{
			Name: "audit",
			Config: jetstream.ConsumerConfig{
				FilterSubjects: []string{"orders.created", "orders.updated"},
				DeliverSubject: "deliver.audit",
			},
		},
	}
	manager := &fakeStreamManager{
		lister: newFakeStreamInfoLister([]*jetstream.StreamInfo{info}, nil),
		streams: map[string]jetstream.Stream{
			"ORDERS": &fakeJetStream{consumerLister: newFakeConsumerInfoLister(consumerInfos, nil)},
		},
	}
	client, err := NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}

	streams, err := client.ListStreams(context.Background())
	if err != nil {
		t.Fatalf("ListStreams() error = %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("ListStreams() length = %d, want 1", len(streams))
	}
	got := streams[0]
	if got.Name != "ORDERS" || got.Description != "Order events" {
		t.Errorf("stream identity = (%q, %q), want (ORDERS, Order events)", got.Name, got.Description)
	}
	if got.Retention != "workqueue" || got.Storage != "memory" || got.Replicas != 3 {
		t.Errorf("stream properties = (%q, %q, %d), want (workqueue, memory, 3)", got.Retention, got.Storage, got.Replicas)
	}
	if len(got.Subjects) != 2 || got.Subjects[0] != "orders.*" || got.Subjects[1] != "orders.audit" {
		t.Errorf("stream subjects = %v, want [orders.* orders.audit]", got.Subjects)
	}
	if len(got.Consumers) != 2 {
		t.Fatalf("stream consumer count = %d, want 2", len(got.Consumers))
	}
	if consumer := got.Consumers[0]; consumer.Name != "billing" || !consumer.Durable || consumer.Description != "Billing worker" || len(consumer.FilterSubjects) != 1 || consumer.FilterSubjects[0] != "orders.*" {
		t.Errorf("billing consumer = %+v, want durable billing consumer with orders.* filter", consumer)
	}
	if consumer := got.Consumers[1]; consumer.Name != "audit" || consumer.Durable || consumer.DeliverSubject != "deliver.audit" || len(consumer.FilterSubjects) != 2 {
		t.Errorf("audit consumer = %+v, want ephemeral push consumer with two filters", consumer)
	}

	info.Config.Subjects[0] = "changed"
	consumerInfos[1].Config.FilterSubjects[0] = "changed"
	if streams[0].Subjects[0] != "orders.*" {
		t.Errorf("stream subjects changed through SDK alias: %v", streams[0].Subjects)
	}
	if streams[0].Consumers[1].FilterSubjects[0] != "orders.created" {
		t.Errorf("consumer filters changed through SDK alias: %v", streams[0].Consumers[1].FilterSubjects)
	}
}

func TestJetStreamClientPropagatesListerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("JetStream unavailable")
	manager := &fakeStreamManager{lister: newFakeStreamInfoLister(nil, wantErr)}
	client, err := NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}

	streams, err := client.ListStreams(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("ListStreams() error = %v, want wrapped %v", err, wantErr)
	}
	if streams != nil {
		t.Errorf("ListStreams() streams = %v, want nil", streams)
	}
}

func TestJetStreamClientRejectsNilStreamInfo(t *testing.T) {
	t.Parallel()

	manager := &fakeStreamManager{lister: newFakeStreamInfoLister([]*jetstream.StreamInfo{nil}, nil)}
	client, err := NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	if _, err := client.ListStreams(context.Background()); !errors.Is(err, ErrStreamInfoNil) {
		t.Errorf("ListStreams() error = %v, want %v", err, ErrStreamInfoNil)
	}
}

func TestNewJetStreamClientRejectsNilManager(t *testing.T) {
	t.Parallel()

	var typedNil *fakeStreamManager
	for _, manager := range []StreamManager{nil, typedNil} {
		if _, err := NewJetStreamClient(manager); !errors.Is(err, ErrJetStreamManagerNil) {
			t.Errorf("NewJetStreamClient() error = %v, want %v", err, ErrJetStreamManagerNil)
		}
	}
}

func TestJetStreamClientRejectsNilLister(t *testing.T) {
	t.Parallel()

	client, err := NewJetStreamClient(&fakeStreamManager{})
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	if _, err := client.ListStreams(context.Background()); !errors.Is(err, ErrStreamListerNil) {
		t.Errorf("ListStreams() error = %v, want %v", err, ErrStreamListerNil)
	}
}

func TestJetStreamClientPropagatesStreamLookupError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream disappeared")
	manager := &fakeStreamManager{
		lister:    newFakeStreamInfoLister([]*jetstream.StreamInfo{{Config: jetstream.StreamConfig{Name: "ORDERS"}}}, nil),
		streamErr: wantErr,
	}
	client, err := NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	if _, err := client.ListStreams(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("ListStreams() error = %v, want wrapped %v", err, wantErr)
	}
}

func TestJetStreamClientValidatesConsumerIteration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stream  jetstream.Stream
		wantErr error
	}{
		{name: "nil stream handle", wantErr: ErrStreamHandleNil},
		{name: "nil consumer lister", stream: &fakeJetStream{}, wantErr: ErrConsumerListerNil},
		{name: "nil consumer info", stream: &fakeJetStream{consumerLister: newFakeConsumerInfoLister([]*jetstream.ConsumerInfo{nil}, nil)}, wantErr: ErrConsumerInfoNil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manager := &fakeStreamManager{
				lister:  newFakeStreamInfoLister([]*jetstream.StreamInfo{{Config: jetstream.StreamConfig{Name: "ORDERS"}}}, nil),
				streams: map[string]jetstream.Stream{"ORDERS": test.stream},
			}
			client, err := NewJetStreamClient(manager)
			if err != nil {
				t.Fatalf("NewJetStreamClient() error = %v", err)
			}
			if _, err := client.ListStreams(context.Background()); !errors.Is(err, test.wantErr) {
				t.Errorf("ListStreams() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestJetStreamClientPropagatesConsumerListerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("consumer page failed")
	manager := &fakeStreamManager{
		lister: newFakeStreamInfoLister([]*jetstream.StreamInfo{{Config: jetstream.StreamConfig{Name: "ORDERS"}}}, nil),
		streams: map[string]jetstream.Stream{
			"ORDERS": &fakeJetStream{consumerLister: newFakeConsumerInfoLister(nil, wantErr)},
		},
	}
	client, err := NewJetStreamClient(manager)
	if err != nil {
		t.Fatalf("NewJetStreamClient() error = %v", err)
	}
	if _, err := client.ListStreams(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("ListStreams() error = %v, want wrapped %v", err, wantErr)
	}
}

type fakeStreamManager struct {
	lister    jetstream.StreamInfoLister
	streams   map[string]jetstream.Stream
	streamErr error
}

func (manager *fakeStreamManager) ListStreams(context.Context, ...jetstream.StreamListOpt) jetstream.StreamInfoLister {
	return manager.lister
}

func (manager *fakeStreamManager) Stream(context.Context, string) (jetstream.Stream, error) {
	if manager.streamErr != nil {
		return nil, manager.streamErr
	}
	for _, stream := range manager.streams {
		return stream, nil
	}
	return &fakeJetStream{consumerLister: newFakeConsumerInfoLister(nil, nil)}, nil
}

type fakeJetStream struct {
	jetstream.Stream
	consumerLister jetstream.ConsumerInfoLister
}

func (stream *fakeJetStream) ListConsumers(context.Context) jetstream.ConsumerInfoLister {
	return stream.consumerLister
}

type fakeStreamInfoLister struct {
	info <-chan *jetstream.StreamInfo
	err  error
}

func newFakeStreamInfoLister(infos []*jetstream.StreamInfo, err error) *fakeStreamInfoLister {
	channel := make(chan *jetstream.StreamInfo, len(infos))
	for _, info := range infos {
		channel <- info
	}
	close(channel)
	return &fakeStreamInfoLister{info: channel, err: err}
}

func (lister *fakeStreamInfoLister) Info() <-chan *jetstream.StreamInfo {
	return lister.info
}

func (lister *fakeStreamInfoLister) Err() error {
	return lister.err
}

type fakeConsumerInfoLister struct {
	info <-chan *jetstream.ConsumerInfo
	err  error
}

func newFakeConsumerInfoLister(infos []*jetstream.ConsumerInfo, err error) *fakeConsumerInfoLister {
	channel := make(chan *jetstream.ConsumerInfo, len(infos))
	for _, info := range infos {
		channel <- info
	}
	close(channel)
	return &fakeConsumerInfoLister{info: channel, err: err}
}

func (lister *fakeConsumerInfoLister) Info() <-chan *jetstream.ConsumerInfo {
	return lister.info
}

func (lister *fakeConsumerInfoLister) Err() error {
	return lister.err
}
