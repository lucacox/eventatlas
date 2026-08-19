package application

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestPeriodicRefresherReportsSuccessAndContinuesAfterFailure(t *testing.T) {
	t.Parallel()

	snapshot := applicationTestSnapshot(t, "snapshot:periodic")
	wantErr := errors.New("temporary provider failure")
	refresher := &periodicFakeRefresher{
		results: []RefreshOutcome{{Snapshot: snapshot}, {Err: wantErr}},
	}
	scope, _ := topology.NewDiscoveryScope("account:test")
	scheduler, err := NewPeriodicRefresher(refresher, scope, PeriodicRefreshConfig{Interval: time.Minute, Timeout: time.Second})
	if err != nil {
		t.Fatalf("NewPeriodicRefresher() error = %v", err)
	}
	ticker := newPeriodicFakeTicker()
	scheduler.newTicker = func(interval time.Duration) refreshTicker {
		if interval != time.Minute {
			t.Errorf("ticker interval = %s, want 1m", interval)
		}
		return ticker
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	reports := make(chan RefreshOutcome, 2)
	go func() {
		defer close(done)
		scheduler.Run(ctx, func(outcome RefreshOutcome) { reports <- outcome })
	}()

	ticker.tick()
	first := receiveRefreshOutcome(t, reports)
	if first.Snapshot != snapshot || first.Err != nil {
		t.Errorf("first outcome = %+v, want snapshot and no error", first)
	}
	ticker.tick()
	second := receiveRefreshOutcome(t, reports)
	if second.Snapshot != nil || !errors.Is(second.Err, wantErr) {
		t.Errorf("second outcome = %+v, want temporary error", second)
	}
	if refresher.lastScope != scope || refresher.callCount() != 2 {
		t.Errorf("refresh calls/scope = (%d, %q), want (2, %q)", refresher.callCount(), refresher.lastScope, scope)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
	if !ticker.isStopped() {
		t.Error("ticker was not stopped")
	}
}

func TestPeriodicRefresherAppliesPerAttemptTimeout(t *testing.T) {
	t.Parallel()

	refresher := &periodicBlockingRefresher{}
	scope, _ := topology.NewDiscoveryScope("account:test")
	scheduler, err := NewPeriodicRefresher(refresher, scope, PeriodicRefreshConfig{Interval: time.Minute, Timeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewPeriodicRefresher() error = %v", err)
	}
	ticker := newPeriodicFakeTicker()
	scheduler.newTicker = func(time.Duration) refreshTicker { return ticker }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reports := make(chan RefreshOutcome, 1)
	go scheduler.Run(ctx, func(outcome RefreshOutcome) { reports <- outcome })
	ticker.tick()
	outcome := receiveRefreshOutcome(t, reports)
	if !errors.Is(outcome.Err, context.DeadlineExceeded) {
		t.Errorf("refresh error = %v, want deadline exceeded", outcome.Err)
	}
}

func TestPeriodicRefresherStopsWithoutReportingCancellation(t *testing.T) {
	t.Parallel()

	refresher := &periodicBlockingRefresher{started: make(chan struct{})}
	scope, _ := topology.NewDiscoveryScope("account:test")
	scheduler, err := NewPeriodicRefresher(refresher, scope, PeriodicRefreshConfig{Interval: time.Minute, Timeout: time.Minute})
	if err != nil {
		t.Fatalf("NewPeriodicRefresher() error = %v", err)
	}
	ticker := newPeriodicFakeTicker()
	scheduler.newTicker = func(time.Duration) refreshTicker { return ticker }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	reports := make(chan RefreshOutcome, 1)
	go func() {
		defer close(done)
		scheduler.Run(ctx, func(outcome RefreshOutcome) { reports <- outcome })
	}()
	ticker.tick()
	select {
	case <-refresher.started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
	select {
	case outcome := <-reports:
		t.Errorf("reported shutdown cancellation: %+v", outcome)
	default:
	}
}

func TestNewPeriodicRefresherValidatesDependenciesAndDurations(t *testing.T) {
	t.Parallel()

	validRefresher := &periodicFakeRefresher{}
	var typedNil *periodicFakeRefresher
	scope, _ := topology.NewDiscoveryScope("account:test")
	tests := []struct {
		name      string
		refresher TopologyRefresher
		config    PeriodicRefreshConfig
		wantErr   error
	}{
		{name: "nil refresher", config: PeriodicRefreshConfig{Interval: time.Second, Timeout: time.Second}, wantErr: ErrTopologyRefresherNil},
		{name: "typed nil refresher", refresher: typedNil, config: PeriodicRefreshConfig{Interval: time.Second, Timeout: time.Second}, wantErr: ErrTopologyRefresherNil},
		{name: "zero interval", refresher: validRefresher, config: PeriodicRefreshConfig{Timeout: time.Second}, wantErr: ErrRefreshIntervalInvalid},
		{name: "negative interval", refresher: validRefresher, config: PeriodicRefreshConfig{Interval: -time.Second, Timeout: time.Second}, wantErr: ErrRefreshIntervalInvalid},
		{name: "zero timeout", refresher: validRefresher, config: PeriodicRefreshConfig{Interval: time.Second}, wantErr: ErrRefreshTimeoutInvalid},
		{name: "negative timeout", refresher: validRefresher, config: PeriodicRefreshConfig{Interval: time.Second, Timeout: -time.Second}, wantErr: ErrRefreshTimeoutInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewPeriodicRefresher(test.refresher, scope, test.config); !errors.Is(err, test.wantErr) {
				t.Errorf("NewPeriodicRefresher() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

type periodicFakeRefresher struct {
	mu        sync.Mutex
	results   []RefreshOutcome
	calls     int
	lastScope topology.DiscoveryScope
}

func (refresher *periodicFakeRefresher) Refresh(_ context.Context, scope topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	refresher.mu.Lock()
	defer refresher.mu.Unlock()
	refresher.lastScope = scope
	index := refresher.calls
	refresher.calls++
	if index >= len(refresher.results) {
		return nil, nil
	}
	return refresher.results[index].Snapshot, refresher.results[index].Err
}

func (refresher *periodicFakeRefresher) callCount() int {
	refresher.mu.Lock()
	defer refresher.mu.Unlock()
	return refresher.calls
}

type periodicBlockingRefresher struct {
	started chan struct{}
}

func (refresher *periodicBlockingRefresher) Refresh(ctx context.Context, _ topology.DiscoveryScope) (*topology.TopologySnapshot, error) {
	if refresher.started != nil {
		close(refresher.started)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type periodicFakeTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func newPeriodicFakeTicker() *periodicFakeTicker {
	return &periodicFakeTicker{ticks: make(chan time.Time, 2), stopped: make(chan struct{})}
}

func (ticker *periodicFakeTicker) C() <-chan time.Time {
	return ticker.ticks
}

func (ticker *periodicFakeTicker) Stop() {
	ticker.once.Do(func() { close(ticker.stopped) })
}

func (ticker *periodicFakeTicker) tick() {
	ticker.ticks <- time.Now()
}

func (ticker *periodicFakeTicker) isStopped() bool {
	select {
	case <-ticker.stopped:
		return true
	default:
		return false
	}
}

func receiveRefreshOutcome(t *testing.T, reports <-chan RefreshOutcome) RefreshOutcome {
	t.Helper()
	select {
	case outcome := <-reports:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh outcome")
		return RefreshOutcome{}
	}
}
