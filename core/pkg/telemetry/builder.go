package telemetry

import (
	connect2 "connectrpc.com/connect"
	"context"
	"fmt"
	"time"

	otelconnect2 "connectrpc.com/otelconnect"
	"github.com/bufbuild/connect-go"
	otelconnect "github.com/bufbuild/connect-opentelemetry-go"

	"go.opentelemetry.io/otel/propagation"

	"github.com/open-feature/flagd/core/pkg/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	metricsExporterOtel = "otel"
	exportInterval      = 2 * time.Second
)

// Config of the telemetry runtime. These are expected to be mapped to start-up arguments
type Config struct {
	MetricsExporter string
	CollectorTarget string
}

// BuildMetricsRecorder is a helper to build telemetry.MetricsRecorder based on configurations
func BuildMetricsRecorder(
	ctx context.Context, svcName string, svcVersion string, config Config,
) (*MetricsRecorder, error) {
	// Build metric reader based on configurations
	mReader, err := buildMetricReader(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup metric reader: %w", err)
	}

	// Build telemetry resource identifier
	rsc, err := buildResourceFor(ctx, svcName, svcVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to setup resource identifier: %w", err)
	}

	return NewOTelRecorder(mReader, rsc, svcName), nil
}

// BuildTraceProvider build and register the trace provider and propagator for the caller runtime. This method
// attempt to register a global TracerProvider backed by batch SpanProcessor.Config. CollectorTarget can be used to
// provide the grpc collector target. Providing empty target results in skipping provider & propagator registration.
// This results in tracers having NoopTracerProvider and propagator having No-Op TextMapPropagator performing no action
func BuildTraceProvider(ctx context.Context, logger *logger.Logger, svc string, svcVersion string, cfg Config) error {
	if cfg.CollectorTarget == "" {
		logger.Warn("skipping trace provider setup as collector target is not set." +
			" Traces will use NoopTracerProvider provider and propagator will use no-Op TextMapPropagator")
		return nil
	}

	exporter, err := buildOtlpExporter(ctx, cfg.CollectorTarget)
	if err != nil {
		return err
	}

	res, err := buildResourceFor(ctx, svc, svcVersion)
	if err != nil {
		return err
	}

	provider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSpanProcessor(trace.NewBatchSpanProcessor(exporter)),
		trace.WithResource(res))

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return nil
}

// BuildConnectOptions is a helper to build connect options based on telemetry configurations
func BuildConnectOptions(cfg Config) []connect.HandlerOption {
	options := []connect.HandlerOption{}

	// add interceptor if configuration is available for collector
	if cfg.CollectorTarget != "" {
		options = append(options, connect.WithInterceptors(
			otelconnect.NewInterceptor(otelconnect.WithTrustRemote()),
		))
	}

	return options
}

// BuildConnect2Options is a helper to build connect options based on telemetry configurations
func BuildConnect2Options(cfg Config) []connect2.HandlerOption {
	options := []connect2.HandlerOption{}

	// add interceptor if configuration is available for collector
	if cfg.CollectorTarget != "" {
		options = append(options, connect2.WithInterceptors(
			otelconnect2.NewInterceptor(otelconnect2.WithTrustRemote()),
		))
	}

	return options
}

// buildMetricReader builds a metric reader based on provided configurations
func buildMetricReader(ctx context.Context, cfg Config) (metric.Reader, error) {
	if cfg.MetricsExporter == "" {
		return buildDefaultMetricReader()
	}

	// Handle metric reader override
	if cfg.MetricsExporter != metricsExporterOtel {
		return nil, fmt.Errorf("provided metrics operator %s is not supported. currently only support %s",
			cfg.MetricsExporter, metricsExporterOtel)
	}

	// Otel override require target configuration
	if cfg.CollectorTarget == "" {
		return nil, fmt.Errorf("metric exporter is set(%s) without providing otel collector target."+
			" collector target is required for this option", cfg.MetricsExporter)
	}

	// Non-blocking, insecure grpc connection
	conn, err := grpc.DialContext(ctx, cfg.CollectorTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	// Otel metric exporter
	otelExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	return metric.NewPeriodicReader(otelExporter, metric.WithInterval(exportInterval)), nil
}

// buildOtlpExporter is a helper to build grpc backed otlp trace exporter
func buildOtlpExporter(ctx context.Context, collectorTarget string) (*otlptrace.Exporter, error) {
	// Non-blocking, insecure grpc connection
	conn, err := grpc.DialContext(ctx, collectorTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	traceClient := otlptracegrpc.NewClient(otlptracegrpc.WithGRPCConn(conn))
	return otlptrace.New(ctx, traceClient)
}

// buildDefaultMetricReader provides the default metric reader
func buildDefaultMetricReader() (metric.Reader, error) {
	return prometheus.New()
}

// buildResourceFor builds a resource identifier with set of resources and service key as attributes
func buildResourceFor(ctx context.Context, serviceName string, serviceVersion string) (*resource.Resource, error) {
	return resource.New(
		ctx,
		resource.WithOS(),
		resource.WithHost(),
		resource.WithProcessRuntimeVersion(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(serviceVersion)),
	)
}
