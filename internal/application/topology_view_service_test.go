package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lucacox/eventatlas/internal/topology"
)

func TestTopologyViewServiceProjectsCurrentDeclaredTopology(t *testing.T) {
	t.Parallel()

	declared := topologyViewTestSnapshot(t, time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC))
	projected := topologyViewServiceTestView(t, declared, declared.CapturedAt().Add(time.Minute))
	reader := &topologyViewDeclaredReaderStub{snapshot: declared}
	projector := &topologyViewProjectorStub{view: projected}
	service, err := NewTopologyViewService(reader, projector)
	if err != nil {
		t.Fatalf("NewTopologyViewService() error = %v", err)
	}

	view, err := service.Current(t.Context())
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	if view != projected {
		t.Errorf("Current() view = %p, want %p", view, projected)
	}
	if projector.declared != declared {
		t.Errorf("Project() declared snapshot = %p, want %p", projector.declared, declared)
	}
}

func TestTopologyViewServiceFallsBackToLastSuccessfulView(t *testing.T) {
	t.Parallel()

	declared := topologyViewTestSnapshot(t, time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC))
	projected := topologyViewServiceTestView(t, declared, declared.CapturedAt().Add(time.Minute))
	reader := &topologyViewDeclaredReaderStub{snapshot: declared}
	projector := &topologyViewProjectorStub{view: projected}
	service, _ := NewTopologyViewService(reader, projector)

	if _, err := service.Current(t.Context()); err != nil {
		t.Fatalf("prime Current() error = %v", err)
	}
	projector.err = errors.New("observation store unavailable")
	if view, err := service.Current(t.Context()); err != nil || view != projected {
		t.Errorf("Current() after projection failure = (%p, %v), want cached %p, nil", view, err, projected)
	}

	reader.err = errors.New("declared store unavailable")
	if view, err := service.Current(t.Context()); err != nil || view != projected {
		t.Errorf("Current() after declared read failure = (%p, %v), want cached %p, nil", view, err, projected)
	}
}

func TestTopologyViewServiceReturnsFailuresBeforeFirstProjection(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("declared store unavailable")
	service, _ := NewTopologyViewService(
		&topologyViewDeclaredReaderStub{err: wantErr},
		&topologyViewProjectorStub{},
	)
	if view, err := service.Current(t.Context()); view != nil || !errors.Is(err, wantErr) {
		t.Errorf("Current() = (%p, %v), want nil and %v", view, err, wantErr)
	}
}

func TestNewTopologyViewServiceRejectsNilDependencies(t *testing.T) {
	t.Parallel()

	var nilReader *topologyViewDeclaredReaderStub
	var nilProjector *topologyViewProjectorStub
	if _, err := NewTopologyViewService(nilReader, &topologyViewProjectorStub{}); !errors.Is(err, ErrDeclaredTopologyReaderNil) {
		t.Errorf("NewTopologyViewService(nil reader) error = %v, want %v", err, ErrDeclaredTopologyReaderNil)
	}
	if _, err := NewTopologyViewService(&topologyViewDeclaredReaderStub{}, nilProjector); !errors.Is(err, ErrTopologyViewProjectorNil) {
		t.Errorf("NewTopologyViewService(nil projector) error = %v, want %v", err, ErrTopologyViewProjectorNil)
	}
}

type topologyViewDeclaredReaderStub struct {
	snapshot *topology.TopologySnapshot
	err      error
}

func (reader *topologyViewDeclaredReaderStub) Current(context.Context) (*topology.TopologySnapshot, error) {
	return reader.snapshot, reader.err
}

type topologyViewProjectorStub struct {
	view     *topology.TopologyView
	err      error
	declared *topology.TopologySnapshot
}

func (projector *topologyViewProjectorStub) Project(
	_ context.Context,
	declared *topology.TopologySnapshot,
) (*topology.TopologyView, error) {
	projector.declared = declared
	return projector.view, projector.err
}

func topologyViewServiceTestView(
	t *testing.T,
	declared *topology.TopologySnapshot,
	generatedAt time.Time,
) *topology.TopologyView {
	t.Helper()
	source, err := topology.NewTopologyViewSource(
		declared.SourceID(),
		topology.EvidenceModeDeclared,
		declared.CapturedAt(),
	)
	if err != nil {
		t.Fatalf("NewTopologyViewSource() error = %v", err)
	}
	view, err := topology.NewTopologyView(topology.TopologyViewParams{
		GeneratedAt: generatedAt,
		Scope:       declared.Scope(),
		Sources:     []topology.TopologyViewSource{source},
		Nodes:       declared.Nodes(),
		Edges:       declared.Edges(),
	})
	if err != nil {
		t.Fatalf("NewTopologyView() error = %v", err)
	}
	return view
}
