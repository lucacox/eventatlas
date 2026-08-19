package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrJetStreamManagerNil = errors.New("JetStream manager cannot be nil")
	ErrStreamListerNil     = errors.New("JetStream returned nil stream lister")
	ErrStreamInfoNil       = errors.New("JetStream returned nil stream info")
	ErrStreamHandleNil     = errors.New("JetStream returned nil stream handle")
	ErrConsumerListerNil   = errors.New("JetStream returned nil consumer lister")
	ErrConsumerInfoNil     = errors.New("JetStream returned nil consumer info")
)

// StreamManager is the subset of the nats.go JetStream API used for discovery.
type StreamManager interface {
	ListStreams(ctx context.Context, opts ...jetstream.StreamListOpt) jetstream.StreamInfoLister
	Stream(ctx context.Context, stream string) (jetstream.Stream, error)
}

// JetStreamClient adapts the official nats.go management API to Client.
type JetStreamClient struct {
	manager StreamManager
}

func NewJetStreamClient(manager StreamManager) (*JetStreamClient, error) {
	if isNilInterface(manager) {
		return nil, ErrJetStreamManagerNil
	}
	return &JetStreamClient{manager: manager}, nil
}

func (client *JetStreamClient) ListStreams(ctx context.Context) ([]Stream, error) {
	lister := client.manager.ListStreams(ctx)
	if isNilInterface(lister) {
		return nil, ErrStreamListerNil
	}
	streams := make([]Stream, 0)
	var nilInfoFound bool
	for info := range lister.Info() {
		if info == nil {
			nilInfoFound = true
			continue
		}
		streams = append(streams, Stream{
			Name:        info.Config.Name,
			Description: info.Config.Description,
			Subjects:    append([]string(nil), info.Config.Subjects...),
			Retention:   strings.ToLower(info.Config.Retention.String()),
			Storage:     strings.ToLower(info.Config.Storage.String()),
			Replicas:    info.Config.Replicas,
		})
	}
	if err := lister.Err(); err != nil {
		return nil, fmt.Errorf("list JetStream streams: %w", err)
	}
	if nilInfoFound {
		return nil, ErrStreamInfoNil
	}

	for index := range streams {
		stream, err := client.manager.Stream(ctx, streams[index].Name)
		if err != nil {
			return nil, fmt.Errorf("open JetStream stream %q: %w", streams[index].Name, err)
		}
		if isNilInterface(stream) {
			return nil, fmt.Errorf("%w: %s", ErrStreamHandleNil, streams[index].Name)
		}
		lister := stream.ListConsumers(ctx)
		if isNilInterface(lister) {
			return nil, fmt.Errorf("%w: %s", ErrConsumerListerNil, streams[index].Name)
		}
		consumers := make([]Consumer, 0)
		var nilConsumerFound bool
		for info := range lister.Info() {
			if info == nil {
				nilConsumerFound = true
				continue
			}
			filters := make([]string, 0, 1+len(info.Config.FilterSubjects))
			if info.Config.FilterSubject != "" {
				filters = append(filters, info.Config.FilterSubject)
			}
			filters = append(filters, info.Config.FilterSubjects...)
			consumers = append(consumers, Consumer{
				Name:           info.Name,
				Description:    info.Config.Description,
				Durable:        info.Config.Durable != "",
				FilterSubjects: filters,
				DeliverSubject: info.Config.DeliverSubject,
			})
		}
		if err := lister.Err(); err != nil {
			return nil, fmt.Errorf("list consumers for JetStream stream %q: %w", streams[index].Name, err)
		}
		if nilConsumerFound {
			return nil, fmt.Errorf("%w in stream %s", ErrConsumerInfoNil, streams[index].Name)
		}
		streams[index].Consumers = consumers
	}
	return streams, nil
}

var _ Client = (*JetStreamClient)(nil)
