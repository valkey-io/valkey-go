package gcp

import (
	"cmp"
	"context"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// captureExporter stands in for the Telemetry API's metric endpoint: it keeps what it is sent.
type captureExporter struct {
	mu       sync.Mutex
	exported []metricdata.ResourceMetrics
	shutdown bool
}

func (e *captureExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

func (e *captureExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

func (e *captureExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.exported = append(e.exported, *rm)
	return nil
}

func (e *captureExporter) ForceFlush(context.Context) error { return nil }

func (e *captureExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

func (e *captureExporter) isShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

func (e *captureExporter) exports() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.exported)
}

// metric returns the last exported data of the metric called name.
func (e *captureExporter) metric(t *testing.T, name string) metricdata.Metrics {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := len(e.exported) - 1; i >= 0; i-- {
		for _, sm := range e.exported[i].ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == name {
					return m
				}
			}
		}
	}
	t.Fatalf("metric %s wasn't exported", name)
	return metricdata.Metrics{}
}

// resourceValue returns a resource attribute of the last export.
func (e *captureExporter) resourceValue(t *testing.T, key string) string {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.exported) == 0 {
		t.Fatal("nothing was exported")
	}
	v, _ := e.exported[len(e.exported)-1].Resource.Set().Value(attribute.Key(key))
	return v.AsString()
}

// keepSpans is an in-memory span exporter whose Shutdown keeps the spans, so tests can read
// them after Telemetry.Shutdown; tracetest.InMemoryExporter clears them.
type keepSpans struct{ *tracetest.InMemoryExporter }

func (keepSpans) Shutdown(context.Context) error { return nil }

// fakeCloud is what fakeGoogleCloud put in place of Google Cloud.
type fakeCloud struct {
	metrics *captureExporter
	spans   *tracetest.InMemoryExporter

	mu           sync.Mutex
	metricTarget target
	traceTarget  target
	credLookups  int
}

func (f *fakeCloud) targets() (metricTarget, traceTarget target, credLookups int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.metricTarget, f.traceTarget, f.credLookups
}

// otelEnv lists the environment variables Setup and the exporters read. fakeGoogleCloud clears
// them, so the developer's environment can't change the tests.
var otelEnv = []string{
	"OTEL_SDK_DISABLED", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
	"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
	"OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_METRICS_INSECURE", "OTEL_EXPORTER_OTLP_TRACES_INSECURE",
	"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_METRICS_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
	"OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
	"OTEL_METRIC_EXPORT_INTERVAL", "OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG", CredentialsEnv, quotaProjectEnv,
}

// fakeGoogleCloud replaces Application Default Credentials (project "adc-project") and the
// exporters with in-memory ones, skips environment detection, and unsets the OTEL_* variables.
func fakeGoogleCloud(t *testing.T) *fakeCloud {
	t.Helper()
	for _, name := range otelEnv {
		t.Setenv(name, "")    // restores the developer's value after the test
		_ = os.Unsetenv(name) // unset rather than empty: OpenTelemetry rejects an empty OTEL_TRACES_SAMPLER
	}
	f := &fakeCloud{metrics: &captureExporter{}, spans: tracetest.NewInMemoryExporter()}
	origCreds, origMetric, origSpan, origDetectors := findCredentials, newMetricExporter, newSpanExporter, detectors
	findCredentials = func(context.Context) (*google.Credentials, error) {
		f.mu.Lock()
		f.credLookups++
		f.mu.Unlock()
		return &google.Credentials{
			ProjectID:   "adc-project",
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"}),
		}, nil
	}
	newMetricExporter = func(_ context.Context, tg target, _ *google.Credentials) (sdkmetric.Exporter, error) {
		f.mu.Lock()
		f.metricTarget = tg
		f.mu.Unlock()
		return f.metrics, nil
	}
	newSpanExporter = func(_ context.Context, tg target, _ *google.Credentials) (sdktrace.SpanExporter, error) {
		f.mu.Lock()
		f.traceTarget = tg
		f.mu.Unlock()
		return keepSpans{f.spans}, nil
	}
	detectors = func() []resource.Detector { return nil }
	t.Cleanup(func() {
		findCredentials, newMetricExporter, newSpanExporter, detectors = origCreds, origMetric, origSpan, origDetectors
	})
	return f
}

// withRegion makes environment detection report a region, as it does on Google Cloud.
func withRegion(t *testing.T) {
	t.Helper()
	detectors = func() []resource.Detector {
		return []resource.Detector{resource.StringDetector("", "cloud.region", func() (string, error) { return "us-east1", nil })}
	}
}

// errorLog collects what goes to OpenTelemetry's global error handler.
type errorLog struct {
	mu   sync.Mutex
	errs []error
}

func captureErrors(t *testing.T) *errorLog {
	t.Helper()
	l := &errorLog{}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.errs = append(l.errs, err)
	}))
	t.Cleanup(func() { otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {})) })
	return l
}

func (l *errorLog) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, err := range l.errs {
		if strings.Contains(err.Error(), s) {
			return true
		}
	}
	return false
}

func setup(t *testing.T, opt Options) *Telemetry {
	t.Helper()
	tel, err := Setup(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	return tel
}

func shutdown(t *testing.T, tel *Telemetry) {
	t.Helper()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// recordOne records one value, so the next export carries the resource.
func recordOne(t *testing.T, tel *Telemetry) {
	t.Helper()
	c, err := tel.MeterProvider().Meter("test").Int64Counter("test_counter")
	if err != nil {
		t.Fatal(err)
	}
	c.Add(context.Background(), 1)
}

func histogramPoints(t *testing.T, m metricdata.Metrics) []metricdata.HistogramDataPoint[float64] {
	t.Helper()
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok || len(h.DataPoints) == 0 {
		t.Fatalf("%s: got %T, want float64 histogram points", m.Name, m.Data)
	}
	return h.DataPoints
}

func resourceString(res *resource.Resource, key string) string {
	v, _ := res.Set().Value(attribute.Key(key))
	return v.AsString()
}

func TestNormalize(t *testing.T) {
	fakeGoogleCloud(t)
	opt, err := normalize(Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Interval and sampling stay unset here: Setup leaves them to the OTEL_* variables first.
	if opt.MetricInterval != 0 || opt.TraceSampleRatio != 0 || !slices.Equal(opt.LatencyBuckets, DefaultLatencyBuckets) || opt.GoogleCredentials != CredentialsAuto {
		t.Errorf("defaults = %+v", opt)
	}

	buckets := []float64{.001, .002}
	opt, err = normalize(Options{MetricInterval: 10 * time.Second, TraceSampleRatio: -1, LatencyBuckets: buckets, GoogleCredentials: CredentialsOff})
	if err != nil || opt.MetricInterval != 10*time.Second || opt.TraceSampleRatio != -1 || !slices.Equal(opt.LatencyBuckets, buckets) || opt.GoogleCredentials != CredentialsOff {
		t.Fatalf("explicit options = %+v, %v", opt, err)
	}
	buckets[0] = 5
	if opt.LatencyBuckets[0] != .001 {
		t.Error("LatencyBuckets shares the caller's slice")
	}
	if opt, _ := normalize(Options{MetricInterval: -time.Second}); opt.MetricInterval != 0 {
		t.Errorf("a negative MetricInterval = %v, want 0 (unset)", opt.MetricInterval)
	}

	t.Setenv(CredentialsEnv, "on")
	if opt, err := normalize(Options{}); err != nil || opt.GoogleCredentials != CredentialsOn {
		t.Errorf("%s=on: got %v, %v", CredentialsEnv, opt.GoogleCredentials, err)
	}
	if opt, err := normalize(Options{GoogleCredentials: CredentialsOff}); err != nil || opt.GoogleCredentials != CredentialsOff {
		t.Errorf("Options wins over %s: got %v, %v", CredentialsEnv, opt.GoogleCredentials, err)
	}
	t.Setenv(CredentialsEnv, "sometimes")
	if _, err := normalize(Options{}); err == nil {
		t.Errorf("%s=sometimes was accepted", CredentialsEnv)
	}
	t.Setenv(CredentialsEnv, "")

	for _, bad := range []Options{
		{TraceSampleRatio: 1.5},
		{TraceSampleRatio: math.NaN()},
		{LatencyBuckets: []float64{.002, .001}},
		{LatencyBuckets: []float64{.001, .001}},
		{LatencyBuckets: []float64{.001, math.Inf(1)}},
		{GoogleCredentials: 99},
	} {
		if _, err := normalize(bad); err == nil {
			t.Errorf("normalize(%+v) returned no error", bad)
		}
	}
}

func TestBuildResource(t *testing.T) {
	fakeGoogleCloud(t)
	ctx := context.Background()
	check := func(t *testing.T, res *resource.Resource, want map[string]string) {
		t.Helper()
		for key, value := range want {
			if got := resourceString(res, key); got != value {
				t.Errorf("%s = %q, want %q", key, got, value)
			}
		}
	}

	res, id := buildResource(ctx, Options{ServiceName: "checkout"}, "adc-project")
	check(t, res, map[string]string{"service.name": "checkout", projectAttribute: "adc-project", "telemetry.sdk.language": "go"})
	if id == "" || resourceString(res, string(serviceInstanceIDKey)) != id {
		t.Errorf("service.instance.id: returned %q, resource has %q", id, resourceString(res, string(serviceInstanceIDKey)))
	}

	res, _ = buildResource(ctx, Options{}, "")
	check(t, res, map[string]string{"service.name": "unknown_service:" + filepath.Base(os.Args[0])})
	if _, ok := res.Set().Value(projectAttribute); ok {
		t.Errorf("%s is set without a project", projectAttribute)
	}

	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	res, _ = buildResource(ctx, Options{}, "adc-project")
	check(t, res, map[string]string{"service.name": "from-env"})
	t.Setenv("OTEL_SERVICE_NAME", "")

	// OTEL_RESOURCE_ATTRIBUTES wins over the defaults, including the credentials' project.
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=env-name,deployment.environment=test,service.instance.id=env-id,gcp.project_id=env-project")
	res, id = buildResource(ctx, Options{}, "adc-project")
	check(t, res, map[string]string{"service.name": "env-name", "deployment.environment": "test", "service.instance.id": "env-id", projectAttribute: "env-project"})
	if id != "env-id" {
		t.Errorf("returned service.instance.id %q, want env-id", id)
	}

	// Options.ResourceAttributes win over OTEL_RESOURCE_ATTRIBUTES.
	attrs := []attribute.KeyValue{
		attribute.String("service.name", "attr-name"),
		attribute.String("deployment.environment", "prod"),
		attribute.String("service.instance.id", "attr-id"),
		attribute.String(projectAttribute, "attr-project"),
	}
	res, id = buildResource(ctx, Options{ResourceAttributes: attrs}, "adc-project")
	check(t, res, map[string]string{"service.name": "attr-name", "deployment.environment": "prod", "service.instance.id": "attr-id", projectAttribute: "attr-project"})
	if id != "attr-id" {
		t.Errorf("returned service.instance.id %q, want attr-id, the exported one", id)
	}

	// The Options fields win over everything.
	res, id = buildResource(ctx, Options{ServiceName: "checkout", ServiceInstanceID: "copy-1", ProjectID: "my-project", ResourceAttributes: attrs}, "adc-project")
	check(t, res, map[string]string{"service.name": "checkout", "deployment.environment": "prod", "service.instance.id": "copy-1", projectAttribute: "my-project"})
	if id != "copy-1" {
		t.Errorf("returned service.instance.id %q, want copy-1", id)
	}
}

func TestNewResource(t *testing.T) {
	f := fakeGoogleCloud(t)
	withRegion(t)
	res := NewResource(context.Background(), Options{ServiceName: "checkout"})
	if got := resourceString(res, projectAttribute); got != "adc-project" {
		t.Errorf("%s = %q, want the credentials' project", projectAttribute, got)
	}
	if got := resourceString(res, "cloud.region"); got != "us-east1" {
		t.Errorf("cloud.region = %q, want the detected one", got)
	}
	if resourceString(res, string(serviceInstanceIDKey)) == "" {
		t.Error("no service.instance.id")
	}
	if _, _, lookups := f.targets(); lookups != 1 {
		t.Errorf("%d credential lookups, want 1", lookups)
	}
	if got := resourceString(NewResource(context.Background(), Options{ProjectID: "mine"}), projectAttribute); got != "mine" {
		t.Errorf("Options.ProjectID: got %q", got)
	}
}

func TestSetup_Project(t *testing.T) {
	t.Run("from the credentials", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		tel := setup(t, Options{})
		recordOne(t, tel)
		shutdown(t, tel)
		if got := f.metrics.resourceValue(t, projectAttribute); got != "adc-project" {
			t.Errorf("%s = %q, want the credentials' adc-project", projectAttribute, got)
		}
	})
	t.Run("Options.ProjectID wins", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		tel := setup(t, Options{ProjectID: "my-project"})
		recordOne(t, tel)
		shutdown(t, tel)
		if got := f.metrics.resourceValue(t, projectAttribute); got != "my-project" {
			t.Errorf("%s = %q, want my-project", projectAttribute, got)
		}
	})
	t.Run("no project anywhere", func(t *testing.T) {
		fakeGoogleCloud(t)
		findCredentials = func(context.Context) (*google.Credentials, error) {
			return &google.Credentials{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"})}, nil
		}
		if _, err := Setup(context.Background(), Options{}); err == nil || !strings.Contains(err.Error(), "no project") {
			t.Fatalf("got %v, want the missing project error", err)
		}
	})
	t.Run("OTEL_RESOURCE_ATTRIBUTES wins over the credentials", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "gcp.project_id=env-project")
		tel := setup(t, Options{})
		recordOne(t, tel)
		shutdown(t, tel)
		if got := f.metrics.resourceValue(t, projectAttribute); got != "env-project" {
			t.Errorf("%s = %q, want env-project", projectAttribute, got)
		}
	})
	t.Run("OTEL_RESOURCE_ATTRIBUTES is enough when the credentials name no project", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		findCredentials = func(context.Context) (*google.Credentials, error) {
			return &google.Credentials{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"})}, nil
		}
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "gcp.project_id=env-project")
		tel := setup(t, Options{})
		recordOne(t, tel)
		shutdown(t, tel)
		if got := f.metrics.resourceValue(t, projectAttribute); got != "env-project" {
			t.Errorf("%s = %q, want env-project", projectAttribute, got)
		}
	})
	t.Run("a local collector needs neither credentials nor a project", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		findCredentials = func(context.Context) (*google.Credentials, error) { return nil, errors.New("no ADC here") }
		tel := setup(t, Options{Endpoint: "http://localhost:4317"})
		recordOne(t, tel)
		shutdown(t, tel)
		mt, tt, _ := f.targets()
		if mt.credentials || tt.credentials || !mt.insecure {
			t.Errorf("targets = %+v / %+v, want plaintext without credentials", mt, tt)
		}
		if got := f.metrics.resourceValue(t, projectAttribute); got != "" {
			t.Errorf("%s = %q, want none", projectAttribute, got)
		}
	})
}

func TestSetup_Endpoints(t *testing.T) {
	t.Run("the Telemetry API by default, with credentials", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		shutdown(t, setup(t, Options{}))
		mt, tt, lookups := f.targets()
		if mt.endpoint != TelemetryEndpoint || tt.endpoint != TelemetryEndpoint || !mt.credentials || !tt.credentials || lookups != 1 {
			t.Errorf("targets = %+v / %+v, %d credential lookups", mt, tt, lookups)
		}
	})
	t.Run("OTEL_EXPORTER_OTLP_ENDPOINT, then the per-signal variables", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://telemetry.googleapis.com:443")
		t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317")
		shutdown(t, setup(t, Options{}))
		mt, tt, _ := f.targets()
		if mt.endpoint != "https://telemetry.googleapis.com:443" || !mt.credentials {
			t.Errorf("metrics target = %+v, want the Google URL with credentials", mt)
		}
		if tt.endpoint != "http://localhost:4317" || tt.credentials || !tt.insecure {
			t.Errorf("trace target = %+v, want the plaintext collector without credentials", tt)
		}
	})
	t.Run("Options.Endpoint wins", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
		shutdown(t, setup(t, Options{Endpoint: "telemetry-psc.p.googleapis.com:443"}))
		if mt, _, _ := f.targets(); mt.endpoint != "telemetry-psc.p.googleapis.com:443" || !mt.credentials {
			t.Errorf("metrics target = %+v, want the Private Service Connect name with credentials", mt)
		}
	})
}

func TestSetup_Tracing(t *testing.T) {
	t.Run("sampled spans reach Cloud Trace", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		tel := setup(t, Options{TraceSampleRatio: 1})
		_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
		span.End()
		shutdown(t, tel)
		if got := f.spans.GetSpans(); len(got) != 1 || got[0].Name != "GET" {
			t.Fatalf("spans = %v, want one GET span", got)
		}
	})
	t.Run("a sampled parent wins over the ratio", func(t *testing.T) {
		fakeGoogleCloud(t)
		tel := setup(t, Options{TraceSampleRatio: 1e-9})
		parent := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled, Remote: true,
		})
		_, span := tel.TracerProvider().Tracer("test").Start(trace.ContextWithRemoteSpanContext(context.Background(), parent), "GET")
		if !span.IsRecording() {
			t.Error("the child of a sampled parent isn't sampled")
		}
		span.End()
		shutdown(t, tel)
	})
	t.Run("a negative ratio turns tracing off", func(t *testing.T) {
		fakeGoogleCloud(t)
		created := false
		newSpanExporter = func(context.Context, target, *google.Credentials) (sdktrace.SpanExporter, error) {
			created = true
			return tracetest.NewInMemoryExporter(), nil
		}
		tel := setup(t, Options{TraceSampleRatio: -1})
		_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
		if span.IsRecording() || created {
			t.Errorf("span recording %v, trace exporter created %v; want neither", span.IsRecording(), created)
		}
		shutdown(t, tel)
	})
	t.Run("OTEL_TRACES_SAMPLER applies when the ratio is unset", func(t *testing.T) {
		fakeGoogleCloud(t)
		t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
		tel := setup(t, Options{})
		_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
		if !span.IsRecording() {
			t.Error("OTEL_TRACES_SAMPLER=always_on was ignored")
		}
		span.End()
		shutdown(t, tel)
	})
	t.Run("Options.TraceSampleRatio wins over OTEL_TRACES_SAMPLER", func(t *testing.T) {
		fakeGoogleCloud(t)
		t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
		tel := setup(t, Options{TraceSampleRatio: 1})
		_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
		if !span.IsRecording() {
			t.Error("TraceSampleRatio 1 lost to OTEL_TRACES_SAMPLER=always_off")
		}
		span.End()
		shutdown(t, tel)
	})
	// OpenTelemetry samples every trace when these variables are invalid; Setup samples 1%.
	for _, env := range [][2]string{{"sometimes", ""}, {"traceidratio", "2"}, {"parentbased_traceidratio", "half"}} {
		t.Run("an invalid OTEL_TRACES_SAMPLER gets the default ratio: "+env[0]+" "+env[1], func(t *testing.T) {
			fakeGoogleCloud(t)
			captureErrors(t) // OpenTelemetry reports the invalid variable
			t.Setenv("OTEL_TRACES_SAMPLER", env[0])
			if env[1] != "" {
				t.Setenv("OTEL_TRACES_SAMPLER_ARG", env[1])
			}
			tel := setup(t, Options{})
			recording := 0
			for range 1000 {
				_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
				if span.IsRecording() {
					recording++
				}
				span.End()
			}
			shutdown(t, tel)
			if recording > 100 { // about 10 expected at 1%
				t.Errorf("%d of 1000 new traces sampled, want about 1%%", recording)
			}
		})
	}
}

func TestEnvSamplerValid(t *testing.T) {
	fakeGoogleCloud(t)
	for _, tc := range []struct {
		sampler, arg string
		hasArg       bool
		want         bool
	}{
		{sampler: "", want: false},
		{sampler: "always_on", want: true},
		{sampler: " ALWAYS_OFF ", want: true},
		{sampler: "parentbased_always_on", want: true},
		{sampler: "parentbased_always_off", want: true},
		{sampler: "traceidratio", want: true}, // the ratio is then 1
		{sampler: "parentbased_traceidratio", arg: "0.25", hasArg: true, want: true},
		{sampler: "parentbased_traceidratio", arg: "0", hasArg: true, want: true},
		{sampler: "traceidratio", arg: "1.5", hasArg: true, want: false},
		{sampler: "traceidratio", arg: "-0.1", hasArg: true, want: false},
		{sampler: "traceidratio", arg: "NaN", hasArg: true, want: false},
		{sampler: "traceidratio", arg: "", hasArg: true, want: false},
		{sampler: "jaeger_remote", want: false}, // not supported by OpenTelemetry Go
	} {
		t.Setenv("OTEL_TRACES_SAMPLER", tc.sampler)
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", tc.arg)
		if !tc.hasArg {
			_ = os.Unsetenv("OTEL_TRACES_SAMPLER_ARG")
		}
		if got := envSamplerValid(); got != tc.want {
			t.Errorf("OTEL_TRACES_SAMPLER=%q OTEL_TRACES_SAMPLER_ARG=%q (set %v): got %v, want %v", tc.sampler, tc.arg, tc.hasArg, got, tc.want)
		}
	}
}

func TestSetup_MetricInterval(t *testing.T) {
	waitForExport := func(f *fakeCloud, within time.Duration) bool {
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			if f.metrics.exports() > 0 {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}
	t.Run("OTEL_METRIC_EXPORT_INTERVAL applies when the interval is unset", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100") // milliseconds
		tel := setup(t, Options{Endpoint: "http://localhost:4317"})
		recordOne(t, tel)
		if !waitForExport(f, 3*time.Second) {
			t.Error("no export within 3s with OTEL_METRIC_EXPORT_INTERVAL=100")
		}
		shutdown(t, tel)
	})
	t.Run("Options.MetricInterval wins", func(t *testing.T) {
		f := fakeGoogleCloud(t)
		t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100")
		tel := setup(t, Options{MetricInterval: time.Hour})
		recordOne(t, tel)
		if waitForExport(f, 500*time.Millisecond) {
			t.Error("exported before the one-hour interval")
		}
		shutdown(t, tel)
		if f.metrics.exports() == 0 {
			t.Error("Shutdown didn't send the last metrics")
		}
	})
	t.Run("below 5s is refused for Google Cloud only", func(t *testing.T) {
		fakeGoogleCloud(t)
		if _, err := Setup(context.Background(), Options{MetricInterval: time.Second}); err == nil || !strings.Contains(err.Error(), "shorter than 5s") {
			t.Errorf("got %v, want the minimum interval error", err)
		}
		shutdown(t, setup(t, Options{MetricInterval: time.Second, Endpoint: "http://localhost:4317"}))
	})
	t.Run("OTEL_METRIC_EXPORT_INTERVAL below 5s is refused for Google Cloud too", func(t *testing.T) {
		fakeGoogleCloud(t)
		t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "1000")
		if _, err := Setup(context.Background(), Options{}); err == nil || !strings.Contains(err.Error(), "1s is shorter than 5s") {
			t.Errorf("got %v, want the minimum interval error", err)
		}
		shutdown(t, setup(t, Options{MetricInterval: time.Minute})) // Options wins
	})
	t.Run("the limit is Google's, so it holds without Google credentials too", func(t *testing.T) {
		fakeGoogleCloud(t)
		if _, err := Setup(context.Background(), Options{MetricInterval: time.Second, GoogleCredentials: CredentialsOff}); err == nil {
			t.Error("a 1s interval to the Telemetry API was accepted")
		}
	})
	t.Run("an invalid OTEL_METRIC_EXPORT_INTERVAL is ignored, as OpenTelemetry ignores it", func(t *testing.T) {
		fakeGoogleCloud(t)
		captureErrors(t) // OpenTelemetry reports the invalid variable
		for _, v := range []string{" 1000", "1s", "-5"} {
			t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", v)
			tel, err := Setup(context.Background(), Options{})
			if err != nil {
				t.Errorf("OTEL_METRIC_EXPORT_INTERVAL=%q: %v", v, err)
				continue
			}
			shutdown(t, tel)
		}
	})
}

func TestSetup_Errors(t *testing.T) {
	f := fakeGoogleCloud(t)
	newSpanExporter = func(context.Context, target, *google.Credentials) (sdktrace.SpanExporter, error) {
		return nil, errors.New("dial failed")
	}
	if _, err := Setup(context.Background(), Options{}); err == nil || !strings.Contains(err.Error(), "trace exporter: dial failed") {
		t.Fatalf("got %v, want the trace exporter error", err)
	}
	if !f.metrics.isShutdown() {
		t.Error("the meter provider wasn't shut down after the trace exporter error")
	}

	newMetricExporter = func(context.Context, target, *google.Credentials) (sdkmetric.Exporter, error) {
		return nil, errors.New("dial failed")
	}
	if _, err := Setup(context.Background(), Options{}); err == nil || !strings.Contains(err.Error(), "metric exporter: dial failed") {
		t.Fatalf("got %v, want the metric exporter error", err)
	}

	findCredentials = func(context.Context) (*google.Credentials, error) { return nil, errors.New("no ADC") }
	if _, err := Setup(context.Background(), Options{}); err == nil || !strings.Contains(err.Error(), "Application Default Credentials not found: no ADC") {
		t.Fatalf("got %v, want the credentials error", err)
	}
	if _, err := Setup(context.Background(), Options{TraceSampleRatio: 2}); err == nil {
		t.Fatal("a sample ratio above 1 was accepted")
	}
	if _, err := Setup(context.Background(), Options{Endpoint: "ftp://example.com"}); err == nil {
		t.Fatal("an ftp endpoint was accepted")
	}
	if _, err := Setup(context.Background(), Options{Endpoint: "http://localhost:4317", GoogleCredentials: CredentialsOn}); err == nil {
		t.Fatal("Google credentials over plaintext were accepted")
	}
}

func TestSetup_LeavesGlobalsAlone(t *testing.T) {
	fakeGoogleCloud(t)
	mp, tp := otel.GetMeterProvider(), otel.GetTracerProvider()
	tel := setup(t, Options{})
	if otel.GetMeterProvider() != mp || otel.GetTracerProvider() != tp {
		t.Error("Setup changed OpenTelemetry's global providers")
	}
	shutdown(t, tel)
}

func TestSetup_SetGlobal(t *testing.T) {
	t.Cleanup(func() {
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
	})
	t.Run("installs the providers Setup created", func(t *testing.T) {
		fakeGoogleCloud(t)
		tel := setup(t, Options{SetGlobal: true})
		if otel.GetMeterProvider() != tel.MeterProvider() || otel.GetTracerProvider() != tel.TracerProvider() {
			t.Error("SetGlobal didn't install Setup's providers")
		}
		shutdown(t, tel)
	})
	t.Run("leaves the application's own providers alone", func(t *testing.T) {
		fakeGoogleCloud(t)
		before := metricnoop.NewMeterProvider()
		otel.SetMeterProvider(before)
		own := sdkmetric.NewMeterProvider()
		tel := setup(t, Options{SetGlobal: true, MeterProvider: own})
		if otel.GetMeterProvider() != before {
			t.Error("SetGlobal installed the application's own meter provider")
		}
		if otel.GetTracerProvider() != tel.TracerProvider() {
			t.Error("SetGlobal didn't install the tracer provider Setup created")
		}
		shutdown(t, tel)
	})
}

func TestSetup_BringYourOwnProviders(t *testing.T) {
	fakeGoogleCloud(t)
	newMetricExporter = func(context.Context, target, *google.Credentials) (sdkmetric.Exporter, error) {
		t.Error("a metric exporter was created for the application's own meter provider")
		return &captureExporter{}, nil
	}
	newSpanExporter = func(context.Context, target, *google.Credentials) (sdktrace.SpanExporter, error) {
		t.Error("a span exporter was created for the application's own tracer provider")
		return tracetest.NewInMemoryExporter(), nil
	}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tp := sdktrace.NewTracerProvider()
	tel := setup(t, Options{MeterProvider: mp, TracerProvider: tp})
	if tel.MeterProvider() != mp || tel.TracerProvider() != tp {
		t.Error("Telemetry doesn't use the application's providers")
	}
	if n := len(tel.ClientOptions()); n != 4 {
		t.Errorf("%d client options, want 4", n)
	}
	if id := tel.ServiceInstanceID(); id != "" {
		t.Errorf("ServiceInstanceID = %q, want none: the application's providers have their own", id)
	}
	shutdown(t, tel)
	if err := reader.Collect(context.Background(), &metricdata.ResourceMetrics{}); err != nil {
		t.Errorf("Shutdown stopped the application's meter provider: %v", err)
	}
	_ = mp.Shutdown(context.Background())
	_ = tp.Shutdown(context.Background())
}

func TestSetup_SDKDisabled(t *testing.T) {
	f := fakeGoogleCloud(t)
	t.Setenv("OTEL_SDK_DISABLED", "true")
	newMetricExporter = func(context.Context, target, *google.Credentials) (sdkmetric.Exporter, error) {
		t.Error("a metric exporter was created with OTEL_SDK_DISABLED=true")
		return &captureExporter{}, nil
	}
	tel := setup(t, Options{})
	if _, _, lookups := f.targets(); lookups != 0 {
		t.Errorf("%d credential lookups, want none", lookups)
	}
	_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
	if span.IsRecording() {
		t.Error("a span records with OTEL_SDK_DISABLED=true")
	}
	if tel.ServiceInstanceID() != "" || len(tel.ClientOptions()) != 2 {
		t.Errorf("instance ID %q, %d client options; want none and 2", tel.ServiceInstanceID(), len(tel.ClientOptions()))
	}
	shutdown(t, tel)
}

func TestSetup_ServiceInstanceID(t *testing.T) {
	f := fakeGoogleCloud(t)
	tel := setup(t, Options{})
	recordOne(t, tel)
	shutdown(t, tel)
	id := tel.ServiceInstanceID()
	if id == "" || f.metrics.resourceValue(t, string(serviceInstanceIDKey)) != id {
		t.Errorf("ServiceInstanceID %q, exported %q", id, f.metrics.resourceValue(t, string(serviceInstanceIDKey)))
	}
	other := setup(t, Options{})
	shutdown(t, other)
	if other.ServiceInstanceID() == id {
		t.Errorf("two Setups share the service.instance.id %q", id)
	}
	explicit := setup(t, Options{ServiceInstanceID: "copy-1"})
	shutdown(t, explicit)
	if got := explicit.ServiceInstanceID(); got != "copy-1" {
		t.Errorf("Options.ServiceInstanceID: got %q", got)
	}

	// The returned ID is always the exported one, also when ResourceAttributes set it.
	f = fakeGoogleCloud(t)
	fromAttrs := setup(t, Options{ResourceAttributes: []attribute.KeyValue{attribute.String("service.instance.id", "attr-id")}})
	recordOne(t, fromAttrs)
	shutdown(t, fromAttrs)
	if got, exported := fromAttrs.ServiceInstanceID(), f.metrics.resourceValue(t, string(serviceInstanceIDKey)); got != "attr-id" || exported != "attr-id" {
		t.Errorf("ServiceInstanceID %q, exported %q, want attr-id for both", got, exported)
	}
}

func TestSetup_WarnsWithoutLocation(t *testing.T) {
	t.Run("no location detected", func(t *testing.T) {
		fakeGoogleCloud(t)
		log := captureErrors(t)
		shutdown(t, setup(t, Options{}))
		if !log.contains("no location") {
			t.Error("no warning about the missing location")
		}
	})
	t.Run("location detected", func(t *testing.T) {
		fakeGoogleCloud(t)
		withRegion(t)
		log := captureErrors(t)
		shutdown(t, setup(t, Options{}))
		if log.contains("no location") {
			t.Error("warned although the region was detected")
		}
	})
	t.Run("not sending to Google", func(t *testing.T) {
		fakeGoogleCloud(t)
		log := captureErrors(t)
		shutdown(t, setup(t, Options{Endpoint: "http://localhost:4317"}))
		if log.contains("no location") {
			t.Error("warned although a collector receives the data")
		}
	})
	t.Run("only traces go to Google", func(t *testing.T) {
		fakeGoogleCloud(t)
		log := captureErrors(t)
		shutdown(t, setup(t, Options{MeterProvider: sdkmetric.NewMeterProvider()}))
		if log.contains("no location") {
			t.Error("warned although the metrics go to the application's own meter provider")
		}
	})
}

func TestClientOptions(t *testing.T) {
	fakeGoogleCloud(t)
	tel := setup(t, Options{})
	defer shutdown(t, tel)
	if n := len(tel.ClientOptions()); n != 4 {
		t.Errorf("%d options without an instance, want 4 (providers, command attribute, buckets)", n)
	}
	opts := tel.ClientOptions()
	opts[0] = nil
	if tel.ClientOptions()[0] == nil {
		t.Error("ClientOptions returns the internal slice")
	}

	withInstance := setup(t, Options{MemorystoreInstance: "inst-1"})
	defer shutdown(t, withInstance)
	if n := len(withInstance.ClientOptions()); n != 6 {
		t.Errorf("%d options with an instance, want 6 (plus the metric and span attributes)", n)
	}
}

// TestTelemetry_LiveClient sends commands to a real Valkey and checks what Cloud Monitoring
// and Cloud Trace would receive: the command and instance attributes, the latency buckets, the
// client-side cache counters and the spans. It needs a local Valkey without a password, at
// 127.0.0.1:6379 or VALKEYOTEL_GCP_TEST_ADDR, and is skipped without one.
func TestTelemetry_LiveClient(t *testing.T) {
	addr := cmp.Or(os.Getenv("VALKEYOTEL_GCP_TEST_ADDR"), "127.0.0.1:6379")
	f := fakeGoogleCloud(t)
	tel := setup(t, Options{MemorystoreInstance: "inst-1", TraceSampleRatio: 1})
	client, err := tel.NewClient(valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		Dialer:            net.Dialer{Timeout: 300 * time.Millisecond},
	})
	if err != nil {
		_ = tel.Shutdown(context.Background())
		t.Skipf("no local Valkey without a password at %s: %v", addr, err)
	}
	ctx := context.Background()
	const key = "valkeyotel-gcp:test"
	if err := client.Do(ctx, client.B().Set().Key(key).Value("v").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, client.B().Get().Key(key).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	for range 2 { // the second read is answered by the client-side cache
		if err := client.DoCache(ctx, client.B().Get().Key(key).Cache(), time.Minute).Error(); err != nil {
			t.Fatal(err)
		}
	}
	_ = client.Do(ctx, client.B().Del().Key(key).Build()).Error()
	client.Close()
	shutdown(t, tel)

	var ops []string
	for _, p := range histogramPoints(t, f.metrics.metric(t, "valkey_command_duration_seconds")) {
		if v, _ := p.Attributes.Value(InstanceAttribute); v.AsString() != "inst-1" {
			t.Errorf("latency point %v lacks %s=inst-1", p.Attributes.ToSlice(), InstanceAttribute)
		}
		if !slices.Equal(p.Bounds, DefaultLatencyBuckets) {
			t.Errorf("command latency buckets = %v, want %v", p.Bounds, DefaultLatencyBuckets)
		}
		op, _ := p.Attributes.Value("operation")
		ops = append(ops, op.AsString())
	}
	if !slices.Contains(ops, "SET") || !slices.Contains(ops, "GET") {
		t.Errorf("operations = %v, want SET and GET", ops)
	}
	if p := histogramPoints(t, f.metrics.metric(t, "valkey_dial_latency"))[0]; !slices.Equal(p.Bounds, DefaultLatencyBuckets) {
		t.Errorf("dial latency buckets = %v, want %v", p.Bounds, DefaultLatencyBuckets)
	}

	hits, ok := f.metrics.metric(t, "valkey_do_cache_hits").Data.(metricdata.Sum[int64])
	if !ok || len(hits.DataPoints) != 1 || hits.DataPoints[0].Value < 1 {
		t.Fatalf("cache hits = %+v, want at least 1", hits.DataPoints)
	}
	if v, _ := hits.DataPoints[0].Attributes.Value(InstanceAttribute); v.AsString() != "inst-1" {
		t.Errorf("cache hits lack %s=inst-1", InstanceAttribute)
	}

	traced := false
	for _, s := range f.spans.GetSpans() {
		for _, a := range s.Attributes {
			if a.Key == InstanceAttribute && a.Value.AsString() == "inst-1" {
				traced = true
			}
		}
	}
	if !traced {
		t.Error("no span carries the instance attribute")
	}
}
