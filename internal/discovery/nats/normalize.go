package nats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

const (
	consumerFilterModeMetadataKey     = "nats.jetstream.filter_mode"
	consumerFilterSubjectsMetadataKey = "nats.jetstream.filter_subjects"
	consumerFilterModeAll             = "all"
	consumerFilterModeSubjects        = "subjects"
)

var (
	ErrStreamNameEmpty     = errors.New("JetStream stream name cannot be blank")
	ErrStreamDuplicate     = errors.New("JetStream stream name is duplicated")
	ErrStreamSubjectEmpty  = errors.New("JetStream stream subject cannot be blank")
	ErrConsumerNameEmpty   = errors.New("JetStream consumer name cannot be blank")
	ErrConsumerDuplicate   = errors.New("JetStream consumer name is duplicated within its stream")
	ErrConsumerFilterEmpty = errors.New("JetStream consumer filter subject cannot be blank")
)

func (provider *Provider) normalize(scope topology.DiscoveryScope, capturedAt time.Time, input []Stream) (*topology.TopologySnapshot, error) {
	identity := newIdentities(provider.sourceID, scope)
	broker, err := topology.NewBroker(
		identity.brokerID().String(),
		provider.brokerName,
		"nats",
		provider.environment,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("normalize NATS broker: %w", err)
	}

	streams := append([]Stream(nil), input...)
	for index := range streams {
		streams[index].Subjects = append([]string(nil), streams[index].Subjects...)
		slices.Sort(streams[index].Subjects)
		streams[index].Subjects = slices.Compact(streams[index].Subjects)
		streams[index].Consumers = append([]Consumer(nil), streams[index].Consumers...)
		for consumerIndex := range streams[index].Consumers {
			consumer := &streams[index].Consumers[consumerIndex]
			consumer.FilterSubjects = append([]string(nil), consumer.FilterSubjects...)
			slices.Sort(consumer.FilterSubjects)
			consumer.FilterSubjects = slices.Compact(consumer.FilterSubjects)
		}
		slices.SortFunc(streams[index].Consumers, func(left, right Consumer) int {
			return strings.Compare(left.Name, right.Name)
		})
	}
	slices.SortFunc(streams, func(left, right Stream) int {
		return strings.Compare(left.Name, right.Name)
	})

	nodes := []topology.TopologyNode{broker}
	edges := make([]topology.Edge, 0)
	destinations := make(map[string]*topology.Destination)
	seenStreams := make(map[string]struct{}, len(streams))
	consumerCount := 0
	declaredEvidence, err := topology.NewEvidence(
		provider.sourceID,
		topology.EvidenceModeDeclared,
		provider.sourceSystem,
		capturedAt,
		capturedAt,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create NATS declared evidence: %w", err)
	}

	for _, stream := range streams {
		if strings.TrimSpace(stream.Name) == "" {
			return nil, ErrStreamNameEmpty
		}
		if _, exists := seenStreams[stream.Name]; exists {
			return nil, fmt.Errorf("%w: %s", ErrStreamDuplicate, stream.Name)
		}
		seenStreams[stream.Name] = struct{}{}

		resource, err := topology.NewMessagingResource(
			identity.streamID(stream.Name).String(),
			stream.Name,
			topology.ResourceKind("nats.jetstream.stream"),
			broker.ID(),
			streamAttributes(stream),
		)
		if err != nil {
			return nil, fmt.Errorf("normalize JetStream stream %q: %w", stream.Name, err)
		}
		nodes = append(nodes, resource)

		for _, subject := range stream.Subjects {
			if strings.TrimSpace(subject) == "" {
				return nil, fmt.Errorf("%w in stream %s", ErrStreamSubjectEmpty, stream.Name)
			}
			destination, exists := destinations[subject]
			if !exists {
				destination, err = topology.NewDestination(
					identity.destinationID(subject).String(),
					subject,
					topology.DestinationKindSubject,
					broker.ID(),
					"",
					destinationAttributes(subject),
				)
				if err != nil {
					return nil, fmt.Errorf("normalize NATS subject %q: %w", subject, err)
				}
				destinations[subject] = destination
				nodes = append(nodes, destination)
			}

			edge, err := topology.NewEdge(destination, resource, topology.EdgeKindCapturedBy, []topology.Evidence{declaredEvidence})
			if err != nil {
				return nil, fmt.Errorf("normalize stream %q subject %q edge: %w", stream.Name, subject, err)
			}
			edges = append(edges, edge)
		}

		seenConsumers := make(map[string]struct{}, len(stream.Consumers))
		for _, nativeConsumer := range stream.Consumers {
			if strings.TrimSpace(nativeConsumer.Name) == "" {
				return nil, fmt.Errorf("%w in stream %s", ErrConsumerNameEmpty, stream.Name)
			}
			if _, exists := seenConsumers[nativeConsumer.Name]; exists {
				return nil, fmt.Errorf("%w: %s/%s", ErrConsumerDuplicate, stream.Name, nativeConsumer.Name)
			}
			seenConsumers[nativeConsumer.Name] = struct{}{}
			consumerCount++

			durability := topology.DurabilityEphemeral
			if nativeConsumer.Durable {
				durability = topology.DurabilityDurable
			}
			consumer, err := topology.NewConsumer(
				identity.consumerID(stream.Name, nativeConsumer.Name).String(),
				nativeConsumer.Name,
				topology.ConsumerKind("nats.jetstream.consumer"),
				durability,
				broker.ID(),
				consumerAttributes(stream.Name, nativeConsumer),
			)
			if err != nil {
				return nil, fmt.Errorf("normalize JetStream consumer %q/%q: %w", stream.Name, nativeConsumer.Name, err)
			}
			nodes = append(nodes, consumer)

			for _, filter := range nativeConsumer.FilterSubjects {
				if strings.TrimSpace(filter) == "" {
					return nil, fmt.Errorf("%w for consumer %s/%s", ErrConsumerFilterEmpty, stream.Name, nativeConsumer.Name)
				}
			}
			bindingMetadata, err := consumerBindingMetadata(nativeConsumer.FilterSubjects)
			if err != nil {
				return nil, fmt.Errorf("encode consumer filters for %s/%s: %w", stream.Name, nativeConsumer.Name, err)
			}
			bindingEvidence, err := topology.NewEvidence(
				provider.sourceID,
				topology.EvidenceModeDeclared,
				provider.sourceSystem,
				capturedAt,
				capturedAt,
				bindingMetadata,
			)
			if err != nil {
				return nil, fmt.Errorf("create consumer binding evidence for %s/%s: %w", stream.Name, nativeConsumer.Name, err)
			}
			hasConsumer, err := topology.NewEdge(resource, consumer, topology.EdgeKindHasConsumer, []topology.Evidence{bindingEvidence})
			if err != nil {
				return nil, fmt.Errorf("normalize stream %q consumer %q edge: %w", stream.Name, nativeConsumer.Name, err)
			}
			edges = append(edges, hasConsumer)
		}
	}

	metadata := map[string]string{
		"nats.jetstream.stream_count":   strconv.Itoa(len(streams)),
		"nats.jetstream.consumer_count": strconv.Itoa(consumerCount),
	}
	snapshotID, err := provider.newSnapshotID()
	if err != nil {
		return nil, fmt.Errorf("generate NATS snapshot ID: %w", err)
	}
	return topology.NewTopologySnapshot(topology.TopologySnapshotParams{
		ID:           snapshotID,
		SourceID:     provider.sourceID,
		Scope:        scope,
		CapturedAt:   capturedAt,
		Nodes:        nodes,
		Edges:        edges,
		Completeness: topology.SnapshotCompletenessFull,
		Metadata:     metadata,
	})
}

func consumerBindingMetadata(filterSubjects []string) (map[string]string, error) {
	mode := consumerFilterModeAll
	if len(filterSubjects) > 0 {
		mode = consumerFilterModeSubjects
	}
	var encodedSubjects bytes.Buffer
	encoder := json.NewEncoder(&encodedSubjects)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(append([]string{}, filterSubjects...)); err != nil {
		return nil, err
	}
	return map[string]string{
		consumerFilterModeMetadataKey:     mode,
		consumerFilterSubjectsMetadataKey: strings.TrimSpace(encodedSubjects.String()),
	}, nil
}

func consumerAttributes(stream string, consumer Consumer) map[string]string {
	deliveryMode := "pull"
	if consumer.DeliverSubject != "" {
		deliveryMode = "push"
	}
	attributes := map[string]string{
		"nats.jetstream.stream":        stream,
		"nats.jetstream.delivery_mode": deliveryMode,
	}
	if strings.TrimSpace(consumer.Description) != "" {
		attributes["nats.jetstream.description"] = consumer.Description
	}
	if consumer.DeliverSubject != "" {
		attributes["nats.jetstream.deliver_subject"] = consumer.DeliverSubject
	}
	return attributes
}

func streamAttributes(stream Stream) map[string]string {
	attributes := map[string]string{
		"nats.jetstream.retention": strings.ToLower(stream.Retention),
		"nats.jetstream.storage":   strings.ToLower(stream.Storage),
		"nats.jetstream.replicas":  strconv.Itoa(stream.Replicas),
	}
	if strings.TrimSpace(stream.Description) != "" {
		attributes["nats.jetstream.description"] = stream.Description
	}
	return attributes
}

func destinationAttributes(subject string) map[string]string {
	attributes := make(map[string]string)
	if strings.ContainsAny(subject, "*>") {
		attributes["nats.subject.is_pattern"] = "true"
	}
	return attributes
}
