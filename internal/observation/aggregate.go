package observation

import (
	"errors"
	"maps"
	"time"
)

var (
	ErrFactInvalid               = errors.New("observation fact is invalid")
	ErrAggregateKeyMismatch      = errors.New("observation fact does not belong to aggregate")
	ErrAggregateFirstSeenZero    = errors.New("observation aggregate first seen must not be zero")
	ErrAggregateLastSeenZero     = errors.New("observation aggregate last seen must not be zero")
	ErrAggregateTimeRangeInvalid = errors.New("observation aggregate last seen must not precede first seen")
	ErrAggregateCountInvalid     = errors.New("observation aggregate count must be positive")
	ErrAggregateFactTimeMismatch = errors.New("observation aggregate fact time must equal last seen")
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
	return NewAggregateFromState(fact, fact.ObservedAt(), fact.ObservedAt(), 1)
}

// NewAggregateFromState restores one persisted aggregate. The fact carries
// the aggregate identity and the metadata associated with lastSeen.
func NewAggregateFromState(
	fact Fact,
	firstSeen time.Time,
	lastSeen time.Time,
	observationCount uint64,
) (Aggregate, error) {
	if !fact.isValid() {
		return Aggregate{}, ErrFactInvalid
	}
	if firstSeen.IsZero() {
		return Aggregate{}, ErrAggregateFirstSeenZero
	}
	if lastSeen.IsZero() {
		return Aggregate{}, ErrAggregateLastSeenZero
	}
	if lastSeen.Before(firstSeen) {
		return Aggregate{}, ErrAggregateTimeRangeInvalid
	}
	if observationCount == 0 {
		return Aggregate{}, ErrAggregateCountInvalid
	}
	if !fact.ObservedAt().Equal(lastSeen) {
		return Aggregate{}, ErrAggregateFactTimeMismatch
	}
	return Aggregate{
		key:              fact.Key(),
		firstSeen:        firstSeen.UTC(),
		lastSeen:         lastSeen.UTC(),
		observationCount: observationCount,
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
