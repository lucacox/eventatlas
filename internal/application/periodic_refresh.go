package application

import (
	"context"
	"errors"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

var (
	ErrTopologyRefresherNil   = errors.New("topology refresher cannot be nil")
	ErrRefreshIntervalInvalid = errors.New("refresh interval must be greater than zero")
	ErrRefreshTimeoutInvalid  = errors.New("refresh timeout must be greater than zero")
)

// TopologyRefresher is the application capability scheduled for reconciliation.
type TopologyRefresher interface {
	Refresh(ctx context.Context, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error)
}

type PeriodicRefreshConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

// RefreshOutcome reports one completed periodic refresh attempt.
type RefreshOutcome struct {
	Snapshot *topology.TopologySnapshot
	Err      error
}

// PeriodicRefresher runs non-overlapping topology refreshes until cancellation.
type PeriodicRefresher struct {
	refresher TopologyRefresher
	scope     topology.DiscoveryScope
	interval  time.Duration
	timeout   time.Duration
	newTicker func(time.Duration) refreshTicker
}

func NewPeriodicRefresher(refresher TopologyRefresher, scope topology.DiscoveryScope, config PeriodicRefreshConfig) (*PeriodicRefresher, error) {
	if isNilInterface(refresher) {
		return nil, ErrTopologyRefresherNil
	}
	if config.Interval <= 0 {
		return nil, ErrRefreshIntervalInvalid
	}
	if config.Timeout <= 0 {
		return nil, ErrRefreshTimeoutInvalid
	}
	return &PeriodicRefresher{
		refresher: refresher,
		scope:     scope,
		interval:  config.Interval,
		timeout:   config.Timeout,
		newTicker: newSystemTicker,
	}, nil
}

// Run blocks until ctx is canceled. Failed attempts are reported but do not
// stop later refreshes, and the underlying service preserves the last valid
// snapshot because it replaces storage only after successful discovery.
func (refresher *PeriodicRefresher) Run(ctx context.Context, report func(RefreshOutcome)) {
	ticker := refresher.newTicker(refresher.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			refreshContext, cancel := context.WithTimeout(ctx, refresher.timeout)
			snapshot, err := refresher.refresher.Refresh(refreshContext, refresher.scope)
			cancel()
			if ctx.Err() != nil {
				return
			}
			if report != nil {
				report(RefreshOutcome{Snapshot: snapshot, Err: err})
			}
		}
	}
}

type refreshTicker interface {
	C() <-chan time.Time
	Stop()
}

type systemTicker struct {
	*time.Ticker
}

func newSystemTicker(interval time.Duration) refreshTicker {
	return systemTicker{Ticker: time.NewTicker(interval)}
}

func (ticker systemTicker) C() <-chan time.Time {
	return ticker.Ticker.C
}
