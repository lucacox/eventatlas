// Package api exposes EventAtlas application use cases over HTTP.
package api

import (
	"context"
	"errors"
	"net/http"
	"reflect"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	"github.com/lucacox/eventatlas/internal/application"
	"github.com/lucacox/eventatlas/internal/topology"
)

const Version = "0.1.0"

var ErrTopologyReaderNil = errors.New("topology reader cannot be nil")

type TopologyReader interface {
	Current(ctx context.Context) (*topology.TopologyView, error)
}

// NewHandler builds the HTTP adapter, including OpenAPI and interactive docs.
func NewHandler(reader TopologyReader) (http.Handler, error) {
	if isNilInterface(reader) {
		return nil, ErrTopologyReaderNil
	}

	mux := http.NewServeMux()
	config := huma.DefaultConfig("EventAtlas API", Version)
	config.DocsRenderer = huma.DocsRendererStoplightElements
	// Keep response bodies limited to the documented EventAtlas contract. The
	// OpenAPI document and per-model schemas remain available on their routes.
	config.CreateHooks = nil
	humaAPI := humago.New(mux, config)

	huma.Register(humaAPI, huma.Operation{
		OperationID: "get-topology",
		Method:      http.MethodGet,
		Path:        "/api/v1/topology",
		Summary:     "Get the current event-driven topology",
		Description: "Returns a merged topology view built from declared provider state and active runtime observations.",
		Tags:        []string{"Topology"},
		Errors:      []int{http.StatusNotFound},
	}, func(ctx context.Context, _ *struct{}) (*getTopologyOutput, error) {
		view, err := reader.Current(ctx)
		if errors.Is(err, application.ErrTopologyNotFound) {
			return nil, huma.Error404NotFound("topology has not been discovered yet")
		}
		if err != nil {
			return nil, huma.Error500InternalServerError("could not load topology")
		}
		if view == nil {
			return nil, huma.Error500InternalServerError("could not load topology")
		}
		return &getTopologyOutput{Body: newTopologyResponse(view)}, nil
	})

	return mux, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
