package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// serverReadHeaderTimeout bounds the header read on these endpoints. They are
// scraped by Prometheus and probed by the kubelet, both of which are fast and
// local, so a slow-header client is a misbehaving one.
const serverReadHeaderTimeout = 5 * time.Second

// ReadinessCheck reports why the controller is not ready, or nil when it is.
//
// An error rather than a bool, because "not ready" is only useful with a reason:
// the readiness body is often the first place an operator looks when a pod will
// not come up.
type ReadinessCheck func() error

// Server exposes the metrics and health endpoints.
//
// Two listeners, because the configuration has two addresses and they serve
// different audiences: the metrics port is scraped by Prometheus and may be
// restricted by a NetworkPolicy, while the health port must remain reachable by
// the kubelet. When both addresses are identical the routes are folded onto one
// listener rather than failing to bind twice.
type Server struct {
	log      *slog.Logger
	servers  []*http.Server
	mu       sync.Mutex
	serveErr error
}

// NewServer builds the HTTP surface.
func NewServer(cfg *config.Config, metrics *Metrics, ready ReadinessCheck, log *slog.Logger) *Server {
	s := &Server{log: log}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())

	healthMux := metricsMux
	if cfg.Controller.HealthAddr != cfg.Controller.MetricsAddr {
		healthMux = http.NewServeMux()
	}

	// /healthz answers "is the process alive?" and deliberately performs no
	// dependency checks. Failing liveness because metrics-server is down would
	// restart a controller that is behaving exactly as designed.
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writePlain(w, http.StatusOK, "ok")
	})

	// /readyz answers "should this replica be deciding?" — caches synced, config
	// valid, first sample collected.
	healthMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := ready(); err != nil {
			writePlain(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writePlain(w, http.StatusOK, "ok")
	})

	s.servers = append(s.servers, &http.Server{
		Addr:              cfg.Controller.MetricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: serverReadHeaderTimeout,
	})
	if healthMux != metricsMux {
		s.servers = append(s.servers, &http.Server{
			Addr:              cfg.Controller.HealthAddr,
			Handler:           healthMux,
			ReadHeaderTimeout: serverReadHeaderTimeout,
		})
	}
	return s
}

// Start launches the listeners in the background.
//
// A bind failure is recorded rather than fatal to the caller's goroutine, and
// surfaced by Err. A controller whose metrics port is already in use should
// still be observable through its logs while it shuts down.
func (s *Server) Start() {
	for _, srv := range s.servers {
		server := srv
		go func() {
			s.log.Info("serving", slog.String("addr", server.Addr))
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("http server stopped", slog.String("addr", server.Addr), slog.Any("error", err))
				s.mu.Lock()
				if s.serveErr == nil {
					s.serveErr = fmt.Errorf("serving %s: %w", server.Addr, err)
				}
				s.mu.Unlock()
			}
		}()
	}
}

// Err returns the first serving error, if any.
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serveErr
}

// Shutdown stops the listeners within the caller's budget.
func (s *Server) Shutdown(ctx context.Context) error {
	var firstErr error
	for _, srv := range s.servers {
		if err := srv.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("shutting down %s: %w", srv.Addr, err)
		}
	}
	return firstErr
}

func writePlain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	// The write error is dropped: the client has gone, and there is no second
	// channel on which to report that fact.
	_, _ = w.Write([]byte(body + "\n"))
}
