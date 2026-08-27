// Package observation contains provider-neutral runtime topology facts.
package observation

import (
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrServiceEnvironmentEmpty         = errors.New("observation service environment cannot be blank")
	ErrServiceNameEmpty                = errors.New("observation service name cannot be blank")
	ErrDestinationMessagingSystemEmpty = errors.New("observation destination messaging system cannot be blank")
	ErrDestinationPhysicalNameEmpty    = errors.New("observation destination physical name cannot be blank")
	ErrFactSourceIDInvalid             = errors.New("observation fact source ID is invalid")
	ErrFactScopeInvalid                = errors.New("observation fact scope is invalid")
	ErrFactObservedAtZero              = errors.New("observation fact time must not be zero")
	ErrFactRelationshipUnsupported     = errors.New("observation fact relationship is unsupported")
	ErrFactServiceIdentityInvalid      = errors.New("observation fact service identity is invalid")
	ErrFactDestinationHintInvalid      = errors.New("observation fact destination hint is invalid")
	ErrFactMetadataKeyEmpty            = errors.New("observation fact metadata key cannot be blank")
)

// ServiceIdentity is the logical identity of an observed application service.
// Process, instance, and version attributes deliberately do not participate.
type ServiceIdentity struct {
	environment string
	namespace   string
	name        string
}

// NewServiceIdentity builds the stable service key used by observations.
func NewServiceIdentity(environment, namespace, name string) (ServiceIdentity, error) {
	environment = strings.TrimSpace(environment)
	if environment == "" {
		return ServiceIdentity{}, ErrServiceEnvironmentEmpty
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return ServiceIdentity{}, ErrServiceNameEmpty
	}

	return ServiceIdentity{
		environment: environment,
		namespace:   strings.TrimSpace(namespace),
		name:        name,
	}, nil
}

func (identity ServiceIdentity) Environment() string { return identity.environment }
func (identity ServiceIdentity) Namespace() string   { return identity.namespace }
func (identity ServiceIdentity) Name() string        { return identity.name }

func (identity ServiceIdentity) isValid() bool {
	return strings.TrimSpace(identity.environment) != "" && strings.TrimSpace(identity.name) != ""
}

// DestinationHint carries names observed at runtime without claiming the
// provider-owned identity of a declared destination.
type DestinationHint struct {
	messagingSystem string
	physicalName    string
	logicalName     string
}

// NewDestinationHint normalizes the technology name and destination hints.
func NewDestinationHint(messagingSystem, physicalName, logicalName string) (DestinationHint, error) {
	messagingSystem = strings.ToLower(strings.TrimSpace(messagingSystem))
	if messagingSystem == "" {
		return DestinationHint{}, ErrDestinationMessagingSystemEmpty
	}
	physicalName = strings.TrimSpace(physicalName)
	if physicalName == "" {
		return DestinationHint{}, ErrDestinationPhysicalNameEmpty
	}

	return DestinationHint{
		messagingSystem: messagingSystem,
		physicalName:    physicalName,
		logicalName:     strings.TrimSpace(logicalName),
	}, nil
}

func (hint DestinationHint) MessagingSystem() string { return hint.messagingSystem }
func (hint DestinationHint) PhysicalName() string    { return hint.physicalName }
func (hint DestinationHint) LogicalName() string     { return hint.logicalName }

func (hint DestinationHint) isValid() bool {
	return strings.TrimSpace(hint.messagingSystem) != "" && strings.TrimSpace(hint.physicalName) != ""
}

// FactParams contains one normalized runtime relationship observation.
type FactParams struct {
	SourceID         topology.SourceID
	Scope            topology.DiscoveryScope
	ObservedAt       time.Time
	RelationshipKind topology.EdgeKind
	Service          ServiceIdentity
	Destination      DestinationHint
	Metadata         map[string]string
}

// Fact is an immutable, transport-independent runtime observation.
type Fact struct {
	sourceID         topology.SourceID
	scope            topology.DiscoveryScope
	observedAt       time.Time
	relationshipKind topology.EdgeKind
	service          ServiceIdentity
	destination      DestinationHint
	metadata         map[string]string
}

// NewFact validates and defensively copies one normalized observation.
func NewFact(params FactParams) (Fact, error) {
	if params.SourceID.String() == "" {
		return Fact{}, ErrFactSourceIDInvalid
	}
	if params.Scope.String() == "" {
		return Fact{}, ErrFactScopeInvalid
	}
	if params.ObservedAt.IsZero() {
		return Fact{}, ErrFactObservedAtZero
	}
	if params.RelationshipKind != topology.EdgeKindPublishes && params.RelationshipKind != topology.EdgeKindConsumes {
		return Fact{}, ErrFactRelationshipUnsupported
	}
	if !params.Service.isValid() {
		return Fact{}, ErrFactServiceIdentityInvalid
	}
	if !params.Destination.isValid() {
		return Fact{}, ErrFactDestinationHintInvalid
	}
	for key := range params.Metadata {
		if strings.TrimSpace(key) == "" {
			return Fact{}, ErrFactMetadataKeyEmpty
		}
	}

	metadata := maps.Clone(params.Metadata)
	if metadata == nil {
		metadata = make(map[string]string)
	}

	return Fact{
		sourceID:         params.SourceID,
		scope:            params.Scope,
		observedAt:       params.ObservedAt.UTC(),
		relationshipKind: params.RelationshipKind,
		service:          params.Service,
		destination:      params.Destination,
		metadata:         metadata,
	}, nil
}

func (fact Fact) SourceID() topology.SourceID         { return fact.sourceID }
func (fact Fact) Scope() topology.DiscoveryScope      { return fact.scope }
func (fact Fact) ObservedAt() time.Time               { return fact.observedAt }
func (fact Fact) RelationshipKind() topology.EdgeKind { return fact.relationshipKind }
func (fact Fact) Service() ServiceIdentity            { return fact.service }
func (fact Fact) Destination() DestinationHint        { return fact.destination }
func (fact Fact) Metadata() map[string]string         { return maps.Clone(fact.metadata) }

func (fact Fact) isValid() bool {
	if fact.sourceID.String() == "" || fact.scope.String() == "" || fact.observedAt.IsZero() {
		return false
	}
	if (fact.relationshipKind != topology.EdgeKindPublishes && fact.relationshipKind != topology.EdgeKindConsumes) || !fact.service.isValid() || !fact.destination.isValid() {
		return false
	}
	for key := range fact.metadata {
		if strings.TrimSpace(key) == "" {
			return false
		}
	}
	return true
}

// Key returns the low-cardinality identity used to aggregate repeated facts.
func (fact Fact) Key() Key {
	return Key{
		sourceID:         fact.sourceID,
		scope:            fact.scope,
		relationshipKind: fact.relationshipKind,
		service:          fact.service,
		destination:      fact.destination,
	}
}

// Key identifies one observation aggregate. Metadata and observation time do
// not participate in identity.
type Key struct {
	sourceID         topology.SourceID
	scope            topology.DiscoveryScope
	relationshipKind topology.EdgeKind
	service          ServiceIdentity
	destination      DestinationHint
}

func (key Key) SourceID() topology.SourceID         { return key.sourceID }
func (key Key) Scope() topology.DiscoveryScope      { return key.scope }
func (key Key) RelationshipKind() topology.EdgeKind { return key.relationshipKind }
func (key Key) Service() ServiceIdentity            { return key.service }
func (key Key) Destination() DestinationHint        { return key.destination }
