// Package valkeyotelgcp sends valkeyotel's metrics to Cloud Monitoring and its traces to Cloud
// Trace, with defaults that suit Memorystore for Valkey.
//
// It uses the standard OpenTelemetry OTLP exporters with Google Cloud's OTLP endpoint, the
// Telemetry API, and Application Default Credentials. Setup creates the meter and tracer
// providers once per process. Its clients come from Telemetry.NewClient, or from
// valkeyotel.NewClient with Telemetry.ClientOptions:
//
//	tel, err := valkeyotelgcp.Setup(ctx, valkeyotelgcp.Options{
//		ServiceName:         "checkout",
//		MemorystoreInstance: "my-instance",
//	})
//	if err != nil {
//		return err
//	}
//	defer tel.Shutdown(context.Background()) // sends the last metrics and spans
//
//	client, err := tel.NewClient(valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}})
//
// Every setting is decided the same way: a value set in Options wins, then the standard
// OpenTelemetry environment variable (OTEL_*), then the package default. With
// OTEL_SDK_DISABLED=true, Setup returns a Telemetry that records and sends nothing;
// OTEL_METRICS_EXPORTER=none or OTEL_TRACES_EXPORTER=none turns off one signal.
//
// Setup changes nothing global unless Options.SetGlobal asks it to. An application that
// already has OpenTelemetry providers passes them in Options: Setup then creates no exporters
// of its own, and NewResource gives those providers what Google Cloud needs.
//
// The metrics are the ones valkeyotel records, such as valkey_command_duration_seconds and
// valkey_do_cache_hits. Cloud Monitoring stores them in Google Cloud Managed Service for
// Prometheus, as prometheus.googleapis.com/<name>/<kind>, so PromQL can query them.
//
// It is a module of its own, next to valkeyotel, so applications that use only valkeyotel
// don't get its dependencies, such as gRPC, the OTLP exporters and the GCP resource detector.
package valkeyotelgcp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeyotel"
	gcpdetector "go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Defaults used when neither Options nor the matching OTEL_* environment variable sets a value.
const (
	// DefaultMetricInterval is how often metrics are sent: OpenTelemetry's own default, which
	// also applies when OTEL_METRIC_EXPORT_INTERVAL isn't set. Nothing is sampled: every
	// command is counted, and the interval only says how often the totals are sent.
	DefaultMetricInterval = 60 * time.Second
	// DefaultTraceSampleRatio is the share of new traces that are sampled: 1%. A span whose
	// parent is sampled is always sampled, so an application that traces its own requests
	// keeps its sampling decision for the valkey-go spans inside them.
	DefaultTraceSampleRatio = 0.01
)

// MinMetricInterval is the shortest metric interval accepted, from Options.MetricInterval or
// OTEL_METRIC_EXPORT_INTERVAL, when metrics go to Google Cloud: Cloud Monitoring accepts one
// point per time series every 5 seconds.
const MinMetricInterval = 5 * time.Second

// InstanceAttribute is the attribute that Options.MemorystoreInstance adds to every valkey-go
// metric and span. It avoids "instance", which Google Cloud reserves for the copy of the
// application (service.instance.id).
const InstanceAttribute = "valkey.instance"

// projectAttribute is the resource attribute that tells the Telemetry API which project
// receives the data.
const projectAttribute = "gcp.project_id"

// projectEnv names the project, as for Google's client libraries. User credentials, such as
// those of gcloud auth application-default login, name none.
const projectEnv = "GOOGLE_CLOUD_PROJECT"

// defaultLatencyBuckets are the boundaries that DefaultLatencyBuckets returns.
var defaultLatencyBuckets = []float64{
	.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10,
}

// DefaultLatencyBuckets returns the default histogram boundaries, in seconds, for command and
// dial latency. Memorystore usually answers in well under a millisecond, so they start at
// 100µs; valkeyotel's own buckets start at 5ms, which puts nearly every command in the first
// bucket. Each call returns a new slice, so changing it changes no default.
func DefaultLatencyBuckets() []float64 {
	return slices.Clone(defaultLatencyBuckets)
}

// Options configures Setup. The zero value works on Google Cloud: the project comes from
// Application Default Credentials, and the environment (Compute Engine, GKE, Cloud Run and
// others) is detected. For every field, a value set here wins over the matching OTEL_*
// environment variable, which wins over the default.
type Options struct {
	// Where the data goes

	// ProjectID is the project that receives the metrics and traces (the gcp.project_id
	// resource attribute). Empty means gcp.project_id in ResourceAttributes or
	// OTEL_RESOURCE_ATTRIBUTES, else GOOGLE_CLOUD_PROJECT, else the project of Application
	// Default Credentials. User credentials (gcloud auth application-default login) name no
	// project, so with them one of the others must be set.
	ProjectID string
	// Endpoint is where the OTLP exporters send: host:port (TLS), or a URL, https:// for TLS or
	// http:// for a plaintext local collector. Empty means OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
	// or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, then OTEL_EXPORTER_OTLP_ENDPOINT, then
	// TelemetryEndpoint.
	Endpoint string
	// GoogleCredentials says when Google credentials go with the requests; see
	// CredentialsMode. The default, CredentialsDefault, reads CredentialsEnv, else uses
	// CredentialsAuto: credentials for *.googleapis.com endpoints only. With user credentials
	// (gcloud auth application-default login), Google also needs a quota project:
	// GOOGLE_CLOUD_QUOTA_PROJECT, else the quota_project_id of the credentials file.
	GoogleCredentials CredentialsMode

	// Who is sending

	// ServiceName identifies the application in Cloud Monitoring and Cloud Trace (the
	// service.name resource attribute, which becomes the job label). Empty means service.name
	// in ResourceAttributes, else OTEL_SERVICE_NAME or service.name in
	// OTEL_RESOURCE_ATTRIBUTES, else "unknown_service:" followed by the executable name.
	ServiceName string
	// ServiceInstanceID identifies this running copy of the application (the
	// service.instance.id resource attribute, which becomes the instance label). Copies that
	// share one collide and lose points. Empty means service.instance.id in
	// ResourceAttributes or OTEL_RESOURCE_ATTRIBUTES, else a generated <host>-<pid>-<random>
	// that is unique per process, and the same for every Setup and NewResource call in it;
	// see Telemetry.ServiceInstanceID. A second Setup in one process that sends the same
	// metrics, with the same ServiceName, needs its own ServiceInstanceID.
	ServiceInstanceID string
	// ResourceAttributes are added to the resource that describes the application, for
	// example deployment.environment. They win over OTEL_RESOURCE_ATTRIBUTES, and ServiceName,
	// ServiceInstanceID and ProjectID win over them.
	ResourceAttributes []attribute.KeyValue
	// MemorystoreInstance adds InstanceAttribute to every valkey-go metric and span: the
	// Memorystore instance ID, so charts can be split per instance and set next to
	// Memorystore's own metrics. Empty adds nothing.
	MemorystoreInstance string
	// MetricAttributes are added to every valkey-go metric, with InstanceAttribute, which
	// wins over one of them with the same key. Each distinct set of values is its own time
	// series, so keep them few and fixed: never keys or other per-request values.
	MetricAttributes []attribute.KeyValue
	// TraceAttributes are added to every valkey-go span, with InstanceAttribute, which wins
	// over one of them with the same key.
	TraceAttributes []attribute.KeyValue

	// How much

	// MetricInterval is how often metrics are sent. Zero or negative means
	// OTEL_METRIC_EXPORT_INTERVAL, else DefaultMetricInterval. Shorter intervals cost more;
	// below MinMetricInterval, from either place, is an error when metrics go to Google Cloud.
	MetricInterval time.Duration
	// TraceSampleRatio is the share of new traces that are sampled, from 0 to 1. Zero means
	// OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG, else DefaultTraceSampleRatio; negative
	// turns tracing off.
	TraceSampleRatio float64
	// LatencyBuckets are the histogram boundaries, in seconds, for command and dial latency.
	// They must be strictly increasing. Empty means DefaultLatencyBuckets. A
	// valkeyotel.WithHistogramOption passed to Telemetry.NewClient wins over them.
	LatencyBuckets []float64

	// Bring your own

	// MeterProvider is the application's own meter provider. When set, valkey-go's metrics go
	// to it, Setup creates no metric exporter, OTEL_METRICS_EXPORTER doesn't apply, and the
	// application is responsible for the provider's resource, including service.instance.id
	// and the location: use NewResource.
	MeterProvider metric.MeterProvider
	// TracerProvider is the application's own tracer provider. When set, valkey-go's spans go
	// to it, Setup creates no trace exporter, and OTEL_TRACES_EXPORTER doesn't apply. A
	// negative TraceSampleRatio still wins: tracing is then off, and this provider gets no
	// valkey-go spans.
	TracerProvider trace.TracerProvider
	// SetGlobal also installs the providers that Setup created as OpenTelemetry's global
	// providers. Providers passed in Options are left alone, and no propagator is installed.
	SetGlobal bool
}

// Telemetry holds the meter and tracer providers that valkey-go's telemetry goes to. Create
// it once per process with Setup, and call Shutdown before the process exits.
type Telemetry struct {
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	ownMeter       *sdkmetric.MeterProvider // created by Setup; nil when the app passed its own or metrics are off
	ownTracer      *sdktrace.TracerProvider // created by Setup; nil when the app passed its own or tracing is off
	clientOptions  []valkeyotel.Option
	instanceID     string

	shutdownOnce sync.Once
	shutdownErr  error       // the result of the one real shutdown
	shutDown     atomic.Bool // Shutdown has started
}

// tokenTimeout bounds each request for an access token. Without it, JSON credentials (user
// credentials, service account keys, workload identity federation) would ask with
// http.DefaultClient, which never gives up: gRPC asks for the token without a context, so a
// token server that never answered would hold up the export, past its timeout, and Shutdown
// with it. 30 seconds is the default of Google's newer auth library, cloud.google.com/go/auth.
// On Google Cloud, tokens come from the metadata server, whose client has its own limit. It is
// a variable so tests can shorten it.
var tokenTimeout = 30 * time.Second

// findCredentials looks up Application Default Credentials. The HTTP client in ctx is the one
// the credentials use for every later token request too. It is a variable so tests can replace
// Google Cloud.
var findCredentials = func(ctx context.Context) (*google.Credentials, error) {
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: tokenTimeout})
	return google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
}

// detectors find the Google Cloud environment the process runs in. It is a variable so tests
// can skip the metadata server.
var detectors = func() []resource.Detector { return []resource.Detector{gcpdetector.NewDetector()} }

// Setup creates the meter and tracer providers that send valkey-go's metrics to Cloud
// Monitoring and its traces to Cloud Trace, through the Telemetry API, or to the endpoint and
// providers that Options and the OTEL_* environment variables choose. ctx bounds the
// environment detection.
//
// Setup changes nothing global unless Options.SetGlobal is set. Errors while sending, such as
// a missing permission, go to OpenTelemetry's global error handler, which logs them.
func Setup(ctx context.Context, opt Options) (*Telemetry, error) {
	if sdkDisabled() {
		return disabledTelemetry(), nil
	}
	opt, err := normalize(opt)
	if err != nil {
		return nil, err
	}
	sendMetrics := opt.MeterProvider == nil
	sendTraces := opt.TracerProvider == nil && opt.TraceSampleRatio >= 0
	// OTEL_METRICS_EXPORTER and OTEL_TRACES_EXPORTER apply only to the providers Setup creates.
	var metricsOff, tracesOff bool
	if sendMetrics {
		if metricsOff, err = exporterOff(signalMetrics); err != nil {
			return nil, err
		}
		sendMetrics = !metricsOff
	}
	if sendTraces {
		if tracesOff, err = exporterOff(signalTraces); err != nil {
			return nil, err
		}
		sendTraces = !tracesOff
	}

	var metricTarget, traceTarget target
	if sendMetrics {
		if metricTarget, err = resolveTarget(opt.Endpoint, signalMetrics, opt.GoogleCredentials); err != nil {
			return nil, err
		}
		if interval := cmp.Or(opt.MetricInterval, envMetricInterval()); metricTarget.google && interval > 0 && interval < MinMetricInterval {
			return nil, fmt.Errorf("valkeyotelgcp: metric interval %v is shorter than %v, the shortest interval Cloud Monitoring accepts", interval, MinMetricInterval)
		}
	}
	if sendTraces {
		if traceTarget, err = resolveTarget(opt.Endpoint, signalTraces, opt.GoogleCredentials); err != nil {
			return nil, err
		}
	}

	// Google credentials and a project are needed only when data goes straight to Google.
	toGoogle := (sendMetrics && metricTarget.credentials) || (sendTraces && traceTarget.credentials)
	var creds *google.Credentials
	var credsProject string
	if toGoogle {
		// The credentials refresh their tokens for the life of the process, so they must not
		// inherit ctx's cancellation.
		if creds, err = findCredentials(context.WithoutCancel(ctx)); err != nil {
			return nil, fmt.Errorf("valkeyotelgcp: Application Default Credentials not found: %w", err)
		}
		credsProject = creds.ProjectID
	}

	t := &Telemetry{}
	var res *resource.Resource
	if sendMetrics || sendTraces { // otherwise the application's providers have their own resource
		res, t.instanceID = buildResource(ctx, opt, credsProject)
	}
	if toGoogle && stringAttr(res, projectAttribute) == "" {
		return nil, errors.New("valkeyotelgcp: no project: set Options.ProjectID, GOOGLE_CLOUD_PROJECT or gcp.project_id in OTEL_RESOURCE_ATTRIBUTES, or use credentials that name a project")
	}
	if sendMetrics && metricTarget.google {
		warnIfNoLocation(res)
	}

	switch {
	case opt.MeterProvider != nil:
		t.meterProvider = opt.MeterProvider
	case metricsOff:
		t.meterProvider = metricnoop.NewMeterProvider() // OTEL_METRICS_EXPORTER=none
	default:
		exporter, err := newMetricExporter(ctx, metricTarget, creds)
		if err != nil {
			return nil, fmt.Errorf("valkeyotelgcp: metric exporter: %w", err)
		}
		var readerOpts []sdkmetric.PeriodicReaderOption
		if opt.MetricInterval > 0 { // otherwise the reader reads OTEL_METRIC_EXPORT_INTERVAL
			readerOpts = append(readerOpts, sdkmetric.WithInterval(opt.MetricInterval))
		}
		t.ownMeter = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(splitExporter{exporter, maxDataPointsPerRequest}, readerOpts...)),
		)
		t.meterProvider = t.ownMeter
	}

	switch {
	case opt.TraceSampleRatio < 0, tracesOff: // tracesOff: OTEL_TRACES_EXPORTER=none
		t.tracerProvider = tracenoop.NewTracerProvider() // also keeps valkey-go spans off the global provider
	case opt.TracerProvider != nil:
		t.tracerProvider = opt.TracerProvider
	default:
		exporter, err := newSpanExporter(ctx, traceTarget, creds)
		if err != nil {
			_ = t.Shutdown(ctx)
			return nil, fmt.Errorf("valkeyotelgcp: trace exporter: %w", err)
		}
		tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res), sdktrace.WithBatcher(exporter)}
		if s := sampler(opt.TraceSampleRatio); s != nil { // otherwise OTEL_TRACES_SAMPLER decides
			tpOpts = append(tpOpts, sdktrace.WithSampler(s))
		}
		t.ownTracer = sdktrace.NewTracerProvider(tpOpts...)
		t.tracerProvider = t.ownTracer
	}

	if opt.SetGlobal {
		if t.ownMeter != nil {
			otel.SetMeterProvider(t.ownMeter)
		}
		if t.ownTracer != nil {
			otel.SetTracerProvider(t.ownTracer)
		}
	}
	t.clientOptions = clientOptions(t.meterProvider, t.tracerProvider, opt)
	return t, nil
}

// sdkDisabled reports whether OTEL_SDK_DISABLED turns OpenTelemetry off. The Go SDK doesn't
// read it itself.
func sdkDisabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true")
}

// exporterOff reads OTEL_METRICS_EXPORTER or OTEL_TRACES_EXPORTER: "none" turns the signal
// off, and "otlp" or no value sends it. Setup sends only OTLP, so any other value is an error
// rather than being ignored.
func exporterOff(sig signal) (bool, error) {
	name := "OTEL_" + string(sig) + "_EXPORTER"
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv(name))); v {
	case "", "otlp":
		return false, nil
	case "none":
		return true, nil
	default:
		return false, fmt.Errorf("valkeyotelgcp: %s=%q: Setup sends only OTLP; want otlp or none", name, v)
	}
}

// envMetricInterval returns OTEL_METRIC_EXPORT_INTERVAL (in milliseconds), or 0 when it is
// unset or invalid. It parses the value as OpenTelemetry does, which ignores invalid values,
// including those too large for a time.Duration.
func envMetricInterval() time.Duration {
	ms, err := strconv.ParseInt(os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"), 10, 64)
	if err != nil || ms <= 0 || ms > int64(time.Duration(math.MaxInt64)/time.Millisecond) {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// disabledTelemetry is the Telemetry of OTEL_SDK_DISABLED=true: clients work, and nothing is
// recorded or sent.
func disabledTelemetry() *Telemetry {
	t := &Telemetry{meterProvider: metricnoop.NewMeterProvider(), tracerProvider: tracenoop.NewTracerProvider()}
	t.clientOptions = []valkeyotel.Option{
		valkeyotel.WithMeterProvider(t.meterProvider),
		valkeyotel.WithTracerProvider(t.tracerProvider),
	}
	return t
}

// normalize fills in the defaults that don't depend on the environment, and checks opt.
func normalize(opt Options) (Options, error) {
	if opt.MetricInterval < 0 {
		opt.MetricInterval = 0
	}
	if opt.TraceSampleRatio > 1 || math.IsNaN(opt.TraceSampleRatio) {
		return Options{}, fmt.Errorf("valkeyotelgcp: TraceSampleRatio %v is not between 0 and 1", opt.TraceSampleRatio)
	}
	if len(opt.LatencyBuckets) == 0 {
		opt.LatencyBuckets = defaultLatencyBuckets
	}
	for i, b := range opt.LatencyBuckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= opt.LatencyBuckets[i-1]) {
			return Options{}, errors.New("valkeyotelgcp: LatencyBuckets must be finite and strictly increasing")
		}
	}
	// Later changes to the caller's slices don't matter.
	opt.LatencyBuckets = slices.Clone(opt.LatencyBuckets)
	opt.ResourceAttributes = slices.Clone(opt.ResourceAttributes)
	opt.MetricAttributes = slices.Clone(opt.MetricAttributes)
	opt.TraceAttributes = slices.Clone(opt.TraceAttributes)
	switch opt.GoogleCredentials {
	case CredentialsDefault:
		mode, err := credentialsFromEnv()
		if err != nil {
			return Options{}, err
		}
		opt.GoogleCredentials = mode
	case CredentialsAuto, CredentialsOn, CredentialsOff:
	default:
		return Options{}, fmt.Errorf("valkeyotelgcp: unknown GoogleCredentials %d", opt.GoogleCredentials)
	}
	return opt, nil
}

// sampler returns the sampler for a TraceSampleRatio of 0 or more, or nil to leave the choice
// to OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG: OpenTelemetry ignores them when a
// sampler is passed in code. Values OpenTelemetry Go doesn't accept count as unset, so they
// get DefaultTraceSampleRatio instead of OpenTelemetry's fallback, which samples every trace.
func sampler(ratio float64) sdktrace.Sampler {
	if ratio == 0 {
		if envSamplerValid() {
			return nil
		}
		ratio = DefaultTraceSampleRatio
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
}

// envSamplerValid reports whether OTEL_TRACES_SAMPLER, with OTEL_TRACES_SAMPLER_ARG, names a
// sampler that OpenTelemetry Go accepts.
func envSamplerValid() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER"))) {
	case "always_on", "always_off", "parentbased_always_on", "parentbased_always_off":
		return true
	case "traceidratio", "parentbased_traceidratio":
		arg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))
		if arg == "" {
			return true // the ratio is then 1, as OpenTelemetry specifies; blank counts as unset
		}
		r, err := strconv.ParseFloat(arg, 64)
		return err == nil && r >= 0 && r <= 1
	default:
		return false
	}
}

// clientOptions returns the valkeyotel options of a Telemetry.
func clientOptions(mp metric.MeterProvider, tp trace.TracerProvider, opt Options) []valkeyotel.Option {
	opts := []valkeyotel.Option{
		valkeyotel.WithMeterProvider(mp),
		valkeyotel.WithTracerProvider(tp),    // never the global one, unless SetGlobal made it ours
		valkeyotel.WithOperationMetricAttr(), // the command name, so charts can be split per command
		// The buckets go through valkeyotel rather than a View, so they also apply to a meter
		// provider the application passed in, and a later WithHistogramOption still wins.
		valkeyotel.WithHistogramOption(valkeyotel.HistogramOption{Buckets: opt.LatencyBuckets}),
	}
	// valkeyotel.MetricAttrs and TraceAttrs replace the attributes set before them, so each is
	// passed once, with all of them.
	metricAttrs, traceAttrs := optionAttributes(opt)
	if len(metricAttrs) > 0 {
		opts = append(opts, valkeyotel.MetricAttrs(metricAttrs...))
	}
	if len(traceAttrs) > 0 {
		opts = append(opts, valkeyotel.TraceAttrs(traceAttrs...))
	}
	return opts
}

// optionAttributes returns the attributes that Options add to every valkey-go metric and span:
// MetricAttributes and TraceAttributes, and InstanceAttribute with MemorystoreInstance, which
// wins over an attribute with the same key. Each key appears once.
func optionAttributes(opt Options) (metricAttrs, traceAttrs []attribute.KeyValue) {
	metricAttrs, traceAttrs = opt.MetricAttributes, opt.TraceAttributes
	if opt.MemorystoreInstance != "" {
		instance := attribute.String(InstanceAttribute, opt.MemorystoreInstance)
		// Last, so it wins over an attribute with the same key.
		metricAttrs = append(slices.Clone(metricAttrs), instance)
		traceAttrs = append(slices.Clone(traceAttrs), instance)
	}
	return lastValueWins(metricAttrs), lastValueWins(traceAttrs)
}

// lastValueWins returns attrs with each key once, keeping its last value.
func lastValueWins(attrs []attribute.KeyValue) []attribute.KeyValue {
	set := attribute.NewSet(attrs...)
	return set.ToSlice()
}

// NewResource returns the resource that Setup gives the providers it creates: the detected
// Google Cloud environment (which includes the location), OTEL_RESOURCE_ATTRIBUTES,
// Options.ResourceAttributes, the service name, a service.instance.id unique to this process,
// and the project (Options.ProjectID, else gcp.project_id in ResourceAttributes or
// OTEL_RESOURCE_ATTRIBUTES, else GOOGLE_CLOUD_PROJECT, else the project of Application Default
// Credentials, if any).
//
// An application that passes its own providers to Setup uses it for them, so Google Cloud can
// tell the copies of the application apart and file the data under the right project and
// location:
//
//	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(valkeyotelgcp.NewResource(ctx, opt)), ...)
//
// Unless Options, ResourceAttributes or OTEL_RESOURCE_ATTRIBUTES set service.instance.id, the
// generated one is the same for every NewResource and Setup call in the process. So an
// application that builds only its meter provider with NewResource, and lets Setup create the
// tracer provider, sends the same ID on its metrics and its spans.
func NewResource(ctx context.Context, opt Options) *resource.Resource {
	var credsProject string
	if opt.ProjectID == "" && strings.TrimSpace(os.Getenv(projectEnv)) == "" {
		if creds, err := findCredentials(context.WithoutCancel(ctx)); err == nil {
			credsProject = creds.ProjectID
		}
	}
	res, _ := buildResource(ctx, opt, credsProject)
	return res
}

// buildResource describes the process to Cloud Monitoring and Cloud Trace, and returns its
// service.instance.id. Later layers win:
//
//  1. the detected environment, and OpenTelemetry's telemetry.sdk attributes;
//  2. the defaults: service.name "unknown_service:<executable>", and the project from
//     GOOGLE_CLOUD_PROJECT, else credsProject;
//  3. OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME;
//  4. Options.ResourceAttributes;
//  5. Options.ServiceName, Options.ProjectID and the service.instance.id (see
//     chooseInstanceID).
//
// Errors don't stop it: they go to OpenTelemetry's error handler, and the rest of the
// description is used.
func buildResource(ctx context.Context, opt Options, credsProject string) (*resource.Resource, string) {
	detected, err := resource.New(ctx, resource.WithDetectors(detectors()...), resource.WithTelemetrySDK())
	if err != nil && !onlySchemaConflicts(err) {
		otel.Handle(fmt.Errorf("valkeyotelgcp: resource detection: %w", err))
	}
	fromEnv, err := resource.New(ctx, resource.WithFromEnv())
	if err != nil && !onlySchemaConflicts(err) {
		otel.Handle(fmt.Errorf("valkeyotelgcp: OTEL_RESOURCE_ATTRIBUTES: %w", err))
	}
	defaults := []attribute.KeyValue{attribute.String("service.name", "unknown_service:"+filepath.Base(os.Args[0]))}
	if project := cmp.Or(strings.TrimSpace(os.Getenv(projectEnv)), credsProject); project != "" {
		defaults = append(defaults, attribute.String(projectAttribute, project))
	}
	res := mergeResources(detected, resource.NewSchemaless(defaults...), fromEnv, resource.NewSchemaless(opt.ResourceAttributes...))

	var fromOptions []attribute.KeyValue
	if opt.ServiceName != "" {
		fromOptions = append(fromOptions, attribute.String("service.name", opt.ServiceName))
	}
	if opt.ProjectID != "" {
		fromOptions = append(fromOptions, attribute.String(projectAttribute, opt.ProjectID))
	}
	instanceID := chooseInstanceID(opt.ServiceInstanceID, res)
	fromOptions = append(fromOptions, serviceInstanceIDKey.String(instanceID))
	return mergeResources(res, resource.NewSchemaless(fromOptions...)), instanceID
}

// mergeResources merges resources, later ones winning. A conflict between their schema URLs
// leaves the merged attributes intact (see onlySchemaConflicts), so it isn't reported.
func mergeResources(layers ...*resource.Resource) *resource.Resource {
	res := resource.Empty()
	for _, layer := range layers {
		var err error
		if res, err = resource.Merge(res, layer); err != nil && !onlySchemaConflicts(err) {
			otel.Handle(fmt.Errorf("valkeyotelgcp: resource: %w", err))
		}
	}
	return res
}

// onlySchemaConflicts reports whether err holds nothing but schema URL conflicts
// (resource.ErrSchemaURLConflict). They happen when resources come from different versions
// of OpenTelemetry's semantic conventions, for example when an application's go.mod moves the
// SDK to a newer version than the GCP detector uses. The merged resource then has no schema
// URL but all its attributes, so nothing is lost. Errors that hold anything else, such as a
// detector that failed, are still reported.
func onlySchemaConflicts(err error) bool {
	switch e := err.(type) {
	case nil:
		return false
	case interface{ Unwrap() []error }: // errors.Join, as resource.New uses
		errs := e.Unwrap()
		for _, inner := range errs {
			if !onlySchemaConflicts(inner) {
				return false
			}
		}
		return len(errs) > 0
	}
	if err == resource.ErrSchemaURLConflict { // the sentinel itself, reached by unwrapping
		return true
	}
	if inner := errors.Unwrap(err); inner != nil {
		return onlySchemaConflicts(inner)
	}
	return false
}

// warnIfNoLocation warns when res has no location: Google Cloud rejects metric points
// without one. On Google Cloud the location is detected; elsewhere the application sets it.
func warnIfNoLocation(res *resource.Resource) {
	for _, key := range []attribute.Key{"location", "cloud.availability_zone", "cloud.region"} {
		if stringAttr(res, key) != "" {
			return
		}
	}
	otel.Handle(errors.New("valkeyotelgcp: no location (cloud.region or cloud.availability_zone) was detected, and Google Cloud rejects metric points without one; outside Google Cloud, set it, for example OTEL_RESOURCE_ATTRIBUTES=cloud.region=us-east1"))
}

// ClientOptions returns the valkeyotel options that send a client's telemetry through t: the
// meter and tracer providers, the command name as a metric attribute, the latency buckets,
// and the metric and span attributes (Options.MemorystoreInstance, Options.MetricAttributes
// and Options.TraceAttributes).
func (t *Telemetry) ClientOptions() []valkeyotel.Option {
	return slices.Clone(t.clientOptions)
}

// NewClient is valkeyotel.NewClient with t's ClientOptions, followed by opts, which win.
//
// valkeyotel.MetricAttrs and valkeyotel.TraceAttrs replace the attributes set before them, so
// passing either one here drops InstanceAttribute, and Options.MetricAttributes or
// Options.TraceAttributes, from that client's metrics or spans. Set extra attributes in
// Options instead.
func (t *Telemetry) NewClient(option valkey.ClientOption, opts ...valkeyotel.Option) (valkey.Client, error) {
	return valkeyotel.NewClient(option, append(t.ClientOptions(), opts...)...)
}

// MeterProvider returns the meter provider, for instrumenting the rest of the application.
// It is the one passed in Options.MeterProvider, if any.
func (t *Telemetry) MeterProvider() metric.MeterProvider {
	return t.meterProvider
}

// TracerProvider returns the tracer provider, for instrumenting the rest of the application.
// It is the one passed in Options.TracerProvider, if any, and a no-op provider when tracing
// is off.
func (t *Telemetry) TracerProvider() trace.TracerProvider {
	return t.tracerProvider
}

// ServiceInstanceID returns the service.instance.id of the providers Setup created:
// Options.ServiceInstanceID, the one in OTEL_RESOURCE_ATTRIBUTES, or the generated one. It is
// empty when Setup created no provider: with OTEL_SDK_DISABLED=true, when the application
// passed both of its own, or when OTEL_METRICS_EXPORTER and OTEL_TRACES_EXPORTER are both
// none. Logging it helps match a process to its time series.
func (t *Telemetry) ServiceInstanceID() string {
	return t.instanceID
}

// Shutdown sends the metrics and spans that haven't been sent yet and stops the providers
// that Setup created; ctx bounds how long it waits. Providers passed in Options are left
// running: they belong to the application. Telemetry recorded afterwards is dropped.
//
// When metrics go to Cloud Monitoring, a final send that comes less than 5 seconds after the
// previous one is refused, and the counts since then are lost, as no send follows it. On a
// planned stop, such as Cloud Run's SIGTERM, call Flush, wait 5 seconds, then call Shutdown:
// the flush restarts the timer of the periodic sends, so with a MetricInterval above 5 seconds
// the final send is the next one.
//
// Shutdown is safe to call more than once, for example from a defer and from a signal
// handler: the first call does the work, and every call returns its result, waiting for it
// when it is still running. A later call doesn't try again, even with more time, so give the
// first call the time it needs.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	t.shutdownOnce.Do(func() {
		t.shutDown.Store(true)
		var err error
		if t.ownMeter != nil {
			err = t.ownMeter.Shutdown(ctx)
		}
		if t.ownTracer != nil {
			err = errors.Join(err, t.ownTracer.Shutdown(ctx))
		}
		t.shutdownErr = err
	})
	return t.shutdownErr
}

// Flush sends the metrics and spans recorded so far, without stopping the providers that
// Setup created, and waits until they are sent or ctx ends. It is for Cloud Run with
// request-based billing, where the CPU is throttled between requests and background sends can
// be late: calling Flush at the end of a request sends the data while the CPU is allocated,
// at the cost of that time on the request. Keep it at least 5 seconds from every other send,
// the periodic ones included: Cloud Monitoring accepts at most one point per time series every
// 5 seconds and refuses the rest. Each Flush also restarts the timer of the periodic sends.
// Providers passed in Options are left alone, and after Shutdown has started, Flush does
// nothing.
func (t *Telemetry) Flush(ctx context.Context) error {
	if t.shutDown.Load() {
		return nil
	}
	var err error
	if t.ownMeter != nil {
		err = t.ownMeter.ForceFlush(ctx)
		// When Shutdown starts meanwhile, the reader reports that it is shut down: Shutdown
		// sends the rest and reports its own errors. Any other error is this flush's.
		if t.shutDown.Load() && errors.Is(err, sdkmetric.ErrReaderShutdown) {
			err = nil
		}
	}
	if t.ownTracer != nil {
		err = errors.Join(err, t.ownTracer.ForceFlush(ctx)) // nil once the tracer provider is shut down
	}
	return err
}
