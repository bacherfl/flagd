package middleware

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/open-feature/flagd/core/pkg/otel"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/open-feature/flagd/core/pkg/logger"
)

var (
	_ http.ResponseWriter = &responseWriterInterceptor{}
	_ http.Hijacker       = &responseWriterInterceptor{}
	_ http.Flusher        = &responseWriterInterceptor{}
)

type middlewareConfig struct {
	MetricRecorder     *otel.MetricsRecorder
	Logger             *logger.Logger
	Service            string
	GroupedStatus      bool
	DisableMeasureSize bool
}

type Middleware struct {
	cfg middlewareConfig
}

func New(cfg middlewareConfig) Middleware {
	cfg.defaults()
	m := Middleware{
		cfg: cfg,
	}
	return m
}

func (cfg *middlewareConfig) defaults() {
	if cfg.Logger == nil {
		log.Fatal("missing logger")
	}
	if cfg.MetricRecorder == nil {
		log.Fatal("missing OpenTelemetry metric recorder")
	}
}

func (m Middleware) Measure(ctx context.Context, handlerID string, reporter Reporter, next func()) {
	// If there isn't predefined handler ID we
	// set that ID as the URL path.
	hid := handlerID
	if handlerID == "" {
		hid = reporter.URLPath()
	}

	// If we need to group the status code, it uses the
	// first number of the status code because is the least
	// required identification way.
	var code string
	if m.cfg.GroupedStatus {
		code = fmt.Sprintf("%dxx", reporter.StatusCode()/100)
	} else {
		code = strconv.Itoa(reporter.StatusCode())
	}

	httpAttrs := m.cfg.MetricRecorder.HTTPAttributes(
		m.cfg.Service,
		hid,
		reporter.Method(),
		code,
	)

	m.cfg.MetricRecorder.OTelInFlightRequestStart(ctx, httpAttrs)
	defer m.cfg.MetricRecorder.OTelInFlightRequestEnd(ctx, httpAttrs)

	// Start the timer and when finishing measure the duration.
	start := time.Now()
	defer func() {
		duration := time.Since(start)

		m.cfg.MetricRecorder.HTTPRequestDuration(ctx, duration, httpAttrs)

		// Measure size of response if required.
		if !m.cfg.DisableMeasureSize {
			m.cfg.MetricRecorder.HttpResponseSize(ctx, reporter.BytesWritten(), httpAttrs)
		}
	}()

	// Call the wrapped logic.
	next()
}

// Handler returns an measuring standard http.Handler.
func Handler(handlerID string, m Middleware, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wi := &responseWriterInterceptor{
			statusCode:     http.StatusOK,
			ResponseWriter: w,
		}
		reporter := &stdReporter{
			w: wi,
			r: r,
		}
		m.Measure(r.Context(), handlerID, reporter, func() {
			h.ServeHTTP(wi, r)
		})
	})
}

type Reporter interface {
	Method() string
	URLPath() string
	StatusCode() int
	BytesWritten() int64
}

type stdReporter struct {
	w *responseWriterInterceptor
	r *http.Request
}

func (s *stdReporter) Method() string { return s.r.Method }

func (s *stdReporter) URLPath() string { return s.r.URL.Path }

func (s *stdReporter) StatusCode() int { return s.w.statusCode }

func (s *stdReporter) BytesWritten() int64 { return int64(s.w.bytesWritten) }

// responseWriterInterceptor is a simple wrapper to intercept set data on a
// ResponseWriter.
type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (w *responseWriterInterceptor) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriterInterceptor) Write(p []byte) (int, error) {
	w.bytesWritten += len(p)
	return w.ResponseWriter.Write(p)
}

func (w *responseWriterInterceptor) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("type assertion failed http.ResponseWriter not a http.Hijacker")
	}
	return h.Hijack()
}

func (w *responseWriterInterceptor) Flush() {
	f, ok := w.ResponseWriter.(http.Flusher)
	if !ok {
		return
	}

	f.Flush()
}
