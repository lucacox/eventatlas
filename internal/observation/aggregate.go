package observation

import (
	"errors"
	"maps"
	"time"
)

var (
	ErrFactInvalid          = errors.New("observation fact is invalid")
	ErrAggregateKeyMismatch = errors.New("observation fact does not belong to aggregate")
)

// Aggregate summarizes repeated deliveries of the same observation fact.
type Aggregate struct {
	key              Key
	firstSeen        time.Time
	lastSeen         time.Time
	observationCount uint64
	metadata         map[string]string
}

// NewAggregate starts an aggregate from one valid fact.
func NewAggregate(fact Fact) (Aggregate, error) {
	if !fact.isValid() {
		return Aggregate{}, ErrFactInvalid
	}
	return Aggregate{
		key:              fact.Key(),
		firstSeen:        fact.ObservedAt(),
		lastSeen:         fact.ObservedAt(),
		observationCount: 1,
		metadata:         fact.Metadata(),
	}, nil
}

// Add returns an updated aggregate. Late observations extend firstSeen without
// moving lastSeen or its latest metadata backwards.
func (aggregate Aggregate) Add(fact Fact) (Aggregate, error) {
	if !fact.isValid() {
		return Aggregate{}, ErrFactInvalid
	}
	if fact.Key() != aggregate.key {
		return Aggregate{}, ErrAggregateKeyMismatch
	}

	observedAt := fact.ObservedAt()
	if observedAt.Before(aggregate.firstSeen) {
		aggregate.firstSeen = observedAt
	}
	if !observedAt.Before(aggregate.lastSeen) {
		aggregate.lastSeen = observedAt
		aggregate.metadata = fact.Metadata()
	}
	if aggregate.observationCount < ^uint64(0) {
		aggregate.observationCount++
	}
	return aggregate, nil
}

func (aggregate Aggregate) Key() Key                 { return aggregate.key }
func (aggregate Aggregate) FirstSeen() time.Time     { return aggregate.firstSeen }
func (aggregate Aggregate) LastSeen() time.Time      { return aggregate.lastSeen }
func (aggregate Aggregate) ObservationCount() uint64 { return aggregate.observationCount }
func (aggregate Aggregate) Metadata() map[string]string {
	return maps.Clone(aggregate.metadata)
}
