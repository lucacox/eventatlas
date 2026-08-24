package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

type namedHTTPServer struct {
	name   string
	server *http.Server
}

type httpServerResult struct {
	name string
	err  error
}

// runHTTPServers binds every configured address before serving traffic. A
// signal or one failed server cancels the shared runtime and gracefully stops
// all listeners within the same shutdown budget.
func runHTTPServers(
	ctx context.Context,
	cancel context.CancelFunc,
	servers []namedHTTPServer,
	shutdownTimeout time.Duration,
) error {
	listeners := make([]net.Listener, 0, len(servers))
	for _, configured := range servers {
		listener, err := net.Listen("tcp", configured.server.Addr)
		if err != nil {
			cancel()
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen for %s on %s: %w", configured.name, configured.server.Addr, err)
		}
		listeners = append(listeners, listener)
	}

	results := make(chan httpServerResult, len(servers))
	for index, configured := range servers {
		listener := listeners[index]
		log.Printf("%s listening on %s", configured.name, listener.Addr())
		go func(configured namedHTTPServer, listener net.Listener) {
			results <- httpServerResult{name: configured.name, err: configured.server.Serve(listener)}
		}(configured, listener)
	}

	serverTriggeredShutdown := false
	completed := make([]httpServerResult, 0, len(servers))
	select {
	case result := <-results:
		serverTriggeredShutdown = true
		completed = append(completed, result)
	case <-ctx.Done():
	}
	cancel()

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	var runtimeErrors []error
	shutdownResults := make(chan httpServerResult, len(servers))
	for _, configured := range servers {
		go func(configured namedHTTPServer) {
			var shutdownErrors []error
			if err := configured.server.Shutdown(shutdownContext); err != nil && !errors.Is(err, http.ErrServerClosed) {
				shutdownErrors = append(shutdownErrors, fmt.Errorf("shut down %s: %w", configured.name, err))
				if closeErr := configured.server.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
					shutdownErrors = append(shutdownErrors, fmt.Errorf("close %s: %w", configured.name, closeErr))
				}
			}
			shutdownResults <- httpServerResult{name: configured.name, err: errors.Join(shutdownErrors...)}
		}(configured)
	}
	for range servers {
		if result := <-shutdownResults; result.err != nil {
			runtimeErrors = append(runtimeErrors, result.err)
		}
	}
	for len(completed) < len(servers) {
		completed = append(completed, <-results)
	}
	for index, result := range completed {
		if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
			runtimeErrors = append(runtimeErrors, fmt.Errorf("serve %s: %w", result.name, result.err))
			continue
		}
		if serverTriggeredShutdown && index == 0 {
			runtimeErrors = append(runtimeErrors, fmt.Errorf("%s stopped unexpectedly", result.name))
		}
	}
	return errors.Join(runtimeErrors...)
}
