// Package otel translates OpenTelemetry data into provider-neutral runtime
// observations. Protobuf and semantic-convention details stop at this package.
package otel

import (
	"context"
	"errors"
	"maps"
	"math"
	"strings"
	"time"

	"github.com/lucacox/eventatlas/internal/observation"
	"github.com/lucacox/eventatlas/internal/topology"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	attributeServiceName           = "service.name"
	attributeServiceNamespace      = "service.namespace"
	attributeEnvironment           = "deployment.environment.name"
	attributeMessagingOperation    = "messaging.operation.type"
	attributeMessagingSystem       = "messaging.system"
	attributeDestinationName       = "messaging.destination.name"
	attributeDestinationTemplate   = "messaging.destination.template"
	unknownServiceName             = "unknown_service"
	operationTypeSend              = "send"
	MetadataEnvironmentSource      = "opentelemetry.environment_source"
	MetadataInstrumentationName    = "opentelemetry.instrumentation_scope.name"
	MetadataInstrumentationVersion = "opentelemetry.instrumentation_scope.version"
	MetadataResourceSchemaURL      = "opentelemetry.resource_schema_url"
	MetadataScopeSchemaURL         = "opentelemetry.scope_schema_url"
)

const (
	EnvironmentSourceResource           = "resource"
	EnvironmentSourceConfiguredFallback = "configured_fallback"
)

var (
	ErrSourceIDInvalid             = errors.New("otel observation source ID is invalid")
	ErrScopeInvalid                = errors.New("otel observation scope is invalid")
	ErrEnvironmentEmpty            = errors.New("otel observation environment cannot be blank")
	ErrMessagingSystemsEmpty       = errors.New("otel supported messaging systems cannot be empty")
	ErrMessagingSystemEmpty        = errors.New("otel supported messaging system cannot be blank")
	ErrFutureSkewNegative          = errors.New("otel maximum future skew cannot be negative")
	ErrAttributeValueLengthInvalid = errors.New("otel maximum attribute value length must be positive")
	ErrContextNil                  = errors.New("otel normalization context cannot be nil")
	ErrRequestNil                  = errors.New("otel trace export request cannot be nil")
)

// RejectionReason is a bounded, low-cardinality explanation for a malformed
// publishing span. It is suitable for metrics and OTLP partial-success text.
type RejectionReason string

const (
	RejectionInvalidSpan                RejectionReason = "invalid_span"
	RejectionDuplicateAttribute         RejectionReason = "duplicate_attribute"
	RejectionInvalidAttributeType       RejectionReason = "invalid_attribute_type"
	RejectionAttributeValueTooLong      RejectionReason = "attribute_value_too_long"
	RejectionServiceNameMissing         RejectionReason = "service_name_missing"
	RejectionServiceNameUnknown         RejectionReason = "service_name_unknown"
	RejectionEnvironmentInvalid         RejectionReason = "environment_invalid"
	RejectionEnvironmentMismatch        RejectionReason = "environment_mismatch"
	RejectionMessagingSystemMissing     RejectionReason = "messaging_system_missing"
	RejectionMessagingSystemUnsupported RejectionReason = "messaging_system_unsupported"
	RejectionDestinationMissing         RejectionReason = "destination_missing"
	RejectionDestinationTemporary       RejectionReason = "destination_temporary"
	RejectionTimestampMissing           RejectionReason = "timestamp_missing"
	RejectionTimestampInvalid           RejectionReason = "timestamp_invalid"
	RejectionTimestampFuture            RejectionReason = "timestamp_future"
	RejectionFactInvalid                RejectionReason = "fact_invalid"
)

// Config contains the stable identity and validation policy applied to each
// trace export request.
type Config struct {
	SourceID                  topology.SourceID
	Scope                     topology.DiscoveryScope
	Environment               string
	SupportedMessagingSystems []string
	MaxFutureSkew             time.Duration
	MaxAttributeValueLength   int
}

// Normalizer is a pure OTLP adapter: it performs no I/O and retains no data
// between requests.
type Normalizer struct {
	sourceID                topology.SourceID
	scope                   topology.DiscoveryScope
	environment             string
	supportedSystems        map[string]struct{}
	maxFutureSkew           time.Duration
	maxAttributeValueLength int
	now                     func() time.Time
}

// Result separates useful observations from irrelevant and malformed spans.
// Its accessors return copies so callers cannot mutate normalization output.
type Result struct {
	facts            []observation.Fact
	totalSpans       int64
	ignoredSpans     int64
	rejectedSpans    int64
	rejectionReasons map[RejectionReason]int64
}

func (result Result) Facts() []observation.Fact {
	return append([]observation.Fact(nil), result.facts...)
}

func (result Result) TotalSpans() int64    { return result.totalSpans }
func (result Result) AcceptedSpans() int64 { return int64(len(result.facts)) }
func (result Result) IgnoredSpans() int64  { return result.ignoredSpans }
func (result Result) RejectedSpans() int64 { return result.rejectedSpans }

func (result Result) RejectionReasons() map[RejectionReason]int64 {
	return maps.Clone(result.rejectionReasons)
}

// NewNormalizer validates configuration once, before traffic can reach the
// receiver. Messaging-system names are normalized like DestinationHint names.
func NewNormalizer(config Config) (*Normalizer, error) {
	if config.SourceID.String() == "" {
		return nil, ErrSourceIDInvalid
	}
	if config.Scope.String() == "" {
		return nil, ErrScopeInvalid
	}

	environment := strings.TrimSpace(config.Environment)
	if environment == "" {
		return nil, ErrEnvironmentEmpty
	}
	if len(config.SupportedMessagingSystems) == 0 {
		return nil, ErrMessagingSystemsEmpty
	}

	supportedSystems := make(map[string]struct{}, len(config.SupportedMessagingSystems))
	for _, value := range config.SupportedMessagingSystems {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return nil, ErrMessagingSystemEmpty
		}
		supportedSystems[value] = struct{}{}
	}
	if config.MaxFutureSkew < 0 {
		return nil, ErrFutureSkewNegative
	}
	if config.MaxAttributeValueLength <= 0 {
		return nil, ErrAttributeValueLengthInvalid
	}

	return &Normalizer{
		sourceID:                config.SourceID,
		scope:                   config.Scope,
		environment:             environment,
		supportedSystems:        supportedSystems,
		maxFutureSkew:           config.MaxFutureSkew,
		maxAttributeValueLength: config.MaxAttributeValueLength,
		now:                     time.Now,
	}, nil
}

// Normalize maps all eligible publishing spans in one OTLP request. If the
// context is cancelled, partial work is discarded and the cancellation error
// is returned so the caller cannot accidentally persist an incomplete batch.
func (normalizer *Normalizer) Normalize(
	ctx context.Context,
	request *collectortracepb.ExportTraceServiceRequest,
) (Result, error) {
	if ctx == nil {
		return Result{}, ErrContextNil
	}
	if request == nil {
		return Result{}, ErrRequestNil
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	result := Result{rejectionReasons: make(map[RejectionReason]int64)}
	now := normalizer.now().UTC()
	for _, resourceSpans := range request.GetResourceSpans() {
		if resourceSpans == nil {
			continue
		}

		resourceAttributes := collectAttributes(resourceSpans.GetResource().GetAttributes())
		for _, scopeSpans := range resourceSpans.GetScopeSpans() {
			if scopeSpans == nil {
				continue
			}
			for _, span := range scopeSpans.GetSpans() {
				if err := ctx.Err(); err != nil {
					return Result{}, err
				}

				result.totalSpans++
				fact, disposition, reason := normalizer.normalizeSpan(
					resourceAttributes,
					resourceSpans.GetSchemaUrl(),
					scopeSpans,
					span,
					now,
				)
				switch disposition {
				case dispositionAccepted:
					result.facts = append(result.facts, fact)
				case dispositionIgnored:
					result.ignoredSpans++
				case dispositionRejected:
					result.rejectedSpans++
					result.rejectionReasons[reason]++
				}
			}
		}
	}

	return result, nil
}

type disposition uint8

const (
	dispositionAccepted disposition = iota
	dispositionIgnored
	dispositionRejected
)

func (normalizer *Normalizer) normalizeSpan(
	resourceAttributes attributes,
	resourceSchemaURL string,
	scopeSpans *tracepb.ScopeSpans,
	span *tracepb.Span,
	now time.Time,
) (observation.Fact, disposition, RejectionReason) {
	if span == nil {
		return observation.Fact{}, dispositionRejected, RejectionInvalidSpan
	}

	spanAttributes := collectAttributes(span.GetAttributes())
	operation, state := spanAttributes.stringValue(attributeMessagingOperation, normalizer.maxAttributeValueLength)
	switch state {
	case attributeMissing:
		return observation.Fact{}, dispositionIgnored, ""
	case attributeDuplicate:
		return observation.Fact{}, dispositionRejected, RejectionDuplicateAttribute
	case attributeWrongType:
		return observation.Fact{}, dispositionRejected, RejectionInvalidAttributeType
	case attributeTooLong:
		return observation.Fact{}, dispositionRejected, RejectionAttributeValueTooLong
	}
	if operation != operationTypeSend {
		return observation.Fact{}, dispositionIgnored, ""
	}

	serviceName, reason := normalizer.requiredString(resourceAttributes, attributeServiceName, RejectionServiceNameMissing)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}
	if serviceName == unknownServiceName || strings.HasPrefix(serviceName, unknownServiceName+":") {
		return observation.Fact{}, dispositionRejected, RejectionServiceNameUnknown
	}

	serviceNamespace, reason := normalizer.optionalString(resourceAttributes, attributeServiceNamespace)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}

	environment, environmentSource, reason := normalizer.resolveEnvironment(resourceAttributes)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}

	messagingSystem, reason := normalizer.requiredString(spanAttributes, attributeMessagingSystem, RejectionMessagingSystemMissing)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}
	messagingSystem = strings.ToLower(messagingSystem)
	if _, supported := normalizer.supportedSystems[messagingSystem]; !supported {
		return observation.Fact{}, dispositionRejected, RejectionMessagingSystemUnsupported
	}

	destinationName, reason := normalizer.requiredString(spanAttributes, attributeDestinationName, RejectionDestinationMissing)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}
	if isTemporaryDestination(messagingSystem, destinationName) {
		return observation.Fact{}, dispositionRejected, RejectionDestinationTemporary
	}

	destinationTemplate, reason := normalizer.optionalString(spanAttributes, attributeDestinationTemplate)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}

	observedAt, reason := normalizer.observedAt(span, now)
	if reason != "" {
		return observation.Fact{}, dispositionRejected, reason
	}

	service, err := observation.NewServiceIdentity(environment, serviceNamespace, serviceName)
	if err != nil {
		return observation.Fact{}, dispositionRejected, RejectionFactInvalid
	}
	destination, err := observation.NewDestinationHint(messagingSystem, destinationName, destinationTemplate)
	if err != nil {
		return observation.Fact{}, dispositionRejected, RejectionFactInvalid
	}

	fact, err := observation.NewFact(observation.FactParams{
		SourceID:         normalizer.sourceID,
		Scope:            normalizer.scope,
		ObservedAt:       observedAt,
		RelationshipKind: topology.EdgeKindPublishes,
		Service:          service,
		Destination:      destination,
		Metadata: normalizer.metadata(
			environmentSource,
			resourceSchemaURL,
			scopeSpans.GetSchemaUrl(),
			scopeSpans.GetScope(),
		),
	})
	if err != nil {
		return observation.Fact{}, dispositionRejected, RejectionFactInvalid
	}

	return fact, dispositionAccepted, ""
}

func (normalizer *Normalizer) requiredString(
	attributes attributes,
	key string,
	missingReason RejectionReason,
) (string, RejectionReason) {
	value, state := attributes.stringValue(key, normalizer.maxAttributeValueLength)
	switch state {
	case attributeMissing:
		return "", missingReason
	case attributeDuplicate:
		return "", RejectionDuplicateAttribute
	case attributeWrongType:
		return "", RejectionInvalidAttributeType
	case attributeTooLong:
		return "", RejectionAttributeValueTooLong
	}
	if value == "" {
		return "", missingReason
	}
	return value, ""
}

func (normalizer *Normalizer) optionalString(attributes attributes, key string) (string, RejectionReason) {
	value, state := attributes.stringValue(key, normalizer.maxAttributeValueLength)
	switch state {
	case attributeMissing:
		return "", ""
	case attributeDuplicate:
		return "", RejectionDuplicateAttribute
	case attributeWrongType:
		return "", RejectionInvalidAttributeType
	case attributeTooLong:
		return "", RejectionAttributeValueTooLong
	default:
		return value, ""
	}
}

func (normalizer *Normalizer) resolveEnvironment(attributes attributes) (string, string, RejectionReason) {
	value, state := attributes.stringValue(attributeEnvironment, normalizer.maxAttributeValueLength)
	switch state {
	case attributeMissing:
		return normalizer.environment, EnvironmentSourceConfiguredFallback, ""
	case attributeDuplicate:
		return "", "", RejectionDuplicateAttribute
	case attributeWrongType:
		return "", "", RejectionInvalidAttributeType
	case attributeTooLong:
		return "", "", RejectionAttributeValueTooLong
	}
	if value == "" {
		return "", "", RejectionEnvironmentInvalid
	}
	if value != normalizer.environment {
		return "", "", RejectionEnvironmentMismatch
	}
	return value, EnvironmentSourceResource, ""
}

func (normalizer *Normalizer) observedAt(span *tracepb.Span, now time.Time) (time.Time, RejectionReason) {
	start := span.GetStartTimeUnixNano()
	end := span.GetEndTimeUnixNano()
	if start != 0 && end != 0 && end < start {
		return time.Time{}, RejectionTimestampInvalid
	}

	observedNanos := end
	if observedNanos == 0 {
		observedNanos = start
	}
	if observedNanos == 0 {
		return time.Time{}, RejectionTimestampMissing
	}
	if observedNanos > math.MaxInt64 {
		return time.Time{}, RejectionTimestampInvalid
	}

	observedAt := time.Unix(0, int64(observedNanos)).UTC()
	if observedAt.After(now.Add(normalizer.maxFutureSkew)) {
		return time.Time{}, RejectionTimestampFuture
	}
	return observedAt, ""
}

func (normalizer *Normalizer) metadata(
	environmentSource string,
	resourceSchemaURL string,
	scopeSchemaURL string,
	scope *commonpb.InstrumentationScope,
) map[string]string {
	metadata := map[string]string{MetadataEnvironmentSource: environmentSource}
	normalizer.addMetadata(metadata, MetadataResourceSchemaURL, resourceSchemaURL)
	normalizer.addMetadata(metadata, MetadataScopeSchemaURL, scopeSchemaURL)
	if scope != nil {
		normalizer.addMetadata(metadata, MetadataInstrumentationName, scope.GetName())
		normalizer.addMetadata(metadata, MetadataInstrumentationVersion, scope.GetVersion())
	}
	return metadata
}

func (normalizer *Normalizer) addMetadata(metadata map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > normalizer.maxAttributeValueLength {
		return
	}
	metadata[key] = value
}

func isTemporaryDestination(messagingSystem, destinationName string) bool {
	return messagingSystem == "nats" && strings.HasPrefix(destinationName, "_INBOX.")
}

type attributes struct {
	values     map[string]*commonpb.AnyValue
	duplicates map[string]struct{}
}

type attributeState uint8

const (
	attributeValid attributeState = iota
	attributeMissing
	attributeDuplicate
	attributeWrongType
	attributeTooLong
)

func collectAttributes(values []*commonpb.KeyValue) attributes {
	result := attributes{
		values:     make(map[string]*commonpb.AnyValue, len(values)),
		duplicates: make(map[string]struct{}),
	}
	for _, keyValue := range values {
		if keyValue == nil {
			continue
		}
		key := keyValue.GetKey()
		if _, exists := result.values[key]; exists {
			result.duplicates[key] = struct{}{}
			continue
		}
		result.values[key] = keyValue.GetValue()
	}
	return result
}

func (attributes attributes) stringValue(key string, maxLength int) (string, attributeState) {
	if _, duplicate := attributes.duplicates[key]; duplicate {
		return "", attributeDuplicate
	}
	value, exists := attributes.values[key]
	if !exists {
		return "", attributeMissing
	}
	stringValue, ok := value.GetValue().(*commonpb.AnyValue_StringValue)
	if !ok {
		return "", attributeWrongType
	}
	if len(stringValue.StringValue) > maxLength {
		return "", attributeTooLong
	}
	return strings.TrimSpace(stringValue.StringValue), attributeValid
}

// String keeps error messages and future diagnostics independent of raw span
// attributes, which may contain high-cardinality or sensitive values.
func (reason RejectionReason) String() string {
	return string(reason)
}
