// Package gcp sends valkeyotel's metrics to Cloud Monitoring and its traces to Cloud Trace,
// with defaults that suit Memorystore for Valkey.
//
// It uses the standard OpenTelemetry OTLP exporters with Google Cloud's OTLP endpoint, the
// Telemetry API, and Application Default Credentials. Setup creates the meter and tracer
// providers once per process. Its clients come from Telemetry.NewClient, or from
// valkeyotel.NewClient with Telemetry.ClientOptions:
//
//	tel, err := gcp.Setup(ctx, gcp.Options{
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
// OTEL_SDK_DISABLED=true, Setup returns a Telemetry that records and sends nothing.
//
// Setup changes nothing global unless Options.SetGlobal asks it to. An application that
// already has OpenTelemetry providers passes them in Options: Setup then creates no exporters
// of its own, and NewResource gives those providers what Google Cloud needs.
//
// The metrics are the ones valkeyotel records, such as valkey_command_duration_seconds and
// valkey_do_cache_hits. Cloud Monitoring stores them in Google Cloud Managed Service for
// Prometheus, as prometheus.googleapis.com/<name>/<kind>, so PromQL can query them.
//
// The package lives in the valkeyotel module, but valkeyotel doesn't import it: applications
// that don't import it don't link its exporters.
package gcp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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

// DefaultLatencyBuckets are the histogram boundaries, in seconds, for command and dial
// latency. Memorystore usually answers in well under a millisecond, so they start at 100µs;
// valkeyotel's own buckets start at 5ms, which puts nearly every command in the first bucket.
var DefaultLatencyBuckets = []float64{
	.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10,
}

// Options configures Setup. The zero value works on Google Cloud: the project comes from
// Application Default Credentials, and the environment (Compute Engine, GKE, Cloud Run and
// others) is detected. For every field, a value set here wins over the matching OTEL_*
// environment variable, which wins over the default.
type Options struct {
	// Where the data goes

	// ProjectID is the project that receives the metrics and traces (the gcp.project_id
	// resource attribute). Empty means gcp.project_id in ResourceAttributes or
	// OTEL_RESOURCE_ATTRIBUTES, else the project of Application Default Credentials.
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
	// that is unique per process; see Telemetry.ServiceInstanceID.
	ServiceInstanceID string
	// ResourceAttributes are added to the resource that describes the application, for
	// example deployment.environment. They win over OTEL_RESOURCE_ATTRIBUTES, and ServiceName,
	// ServiceInstanceID and ProjectID win over them.
	ResourceAttributes []attribute.KeyValue
	// MemorystoreInstance adds InstanceAttribute to every valkey-go metric and span: the
	// Memorystore instance ID, so charts can be split per instance and set next to
	// Memorystore's own metrics. Empty adds nothing.
	MemorystoreInstance string

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
	// to it, Setup creates no metric exporter, and the application is responsible for the
	// provider's resource, including service.instance.id and the location: use NewResource.
	MeterProvider metric.MeterProvider
	// TracerProvider is the application's own tracer provider. When set, valkey-go's spans go
	// to it and Setup creates no trace exporter.
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
	ownMeter       *sdkmetric.MeterProvider // created by Setup; nil when the app passed its own
	ownTracer      *sdktrace.TracerProvider // created by Setup; nil when the app passed its own or tracing is off
	clientOptions  []valkeyotel.Option
	instanceID     string
}

// findCredentials looks up Application Default Credentials. It is a variable so tests can
// replace Google Cloud.
var findCredentials = func(ctx context.Context) (*google.Credentials, error) {
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

	var metricTarget, traceTarget target
	if sendMetrics {
		if metricTarget, err = resolveTarget(opt.Endpoint, signalMetrics, opt.GoogleCredentials); err != nil {
			return nil, err
		}
		if interval := cmp.Or(opt.MetricInterval, envMetricInterval()); metricTarget.google && interval > 0 && interval < MinMetricInterval {
			return nil, fmt.Errorf("valkeyotel/gcp: metric interval %v is shorter than %v, the shortest interval Cloud Monitoring accepts", interval, MinMetricInterval)
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
			return nil, fmt.Errorf("valkeyotel/gcp: Application Default Credentials not found: %w", err)
		}
		credsProject = creds.ProjectID
	}

	t := &Telemetry{}
	var res *resource.Resource
	if sendMetrics || sendTraces { // otherwise the application's providers have their own resource
		res, t.instanceID = buildResource(ctx, opt, credsProject)
	}
	if toGoogle && stringAttr(res, projectAttribute) == "" {
		return nil, errors.New("valkeyotel/gcp: no project: set Options.ProjectID or gcp.project_id in OTEL_RESOURCE_ATTRIBUTES, or use credentials that name a project")
	}
	if sendMetrics && metricTarget.google {
		warnIfNoLocation(res)
	}

	if sendMetrics {
		exporter, err := newMetricExporter(ctx, metricTarget, creds)
		if err != nil {
			return nil, fmt.Errorf("valkeyotel/gcp: metric exporter: %w", err)
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
	} else {
		t.meterProvider = opt.MeterProvider
	}

	switch {
	case opt.TraceSampleRatio < 0:
		t.tracerProvider = tracenoop.NewTracerProvider() // also keeps valkey-go spans off the global provider
	case opt.TracerProvider != nil:
		t.tracerProvider = opt.TracerProvider
	default:
		exporter, err := newSpanExporter(ctx, traceTarget, creds)
		if err != nil {
			_ = t.Shutdown(ctx)
			return nil, fmt.Errorf("valkeyotel/gcp: trace exporter: %w", err)
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

// envMetricInterval returns OTEL_METRIC_EXPORT_INTERVAL (in milliseconds), or 0 when it is
// unset or invalid. It parses the value as OpenTelemetry does, which ignores invalid values.
func envMetricInterval() time.Duration {
	ms, err := strconv.Atoi(os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"))
	if err != nil || ms <= 0 {
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
		return Options{}, fmt.Errorf("valkeyotel/gcp: TraceSampleRatio %v is not between 0 and 1", opt.TraceSampleRatio)
	}
	if len(opt.LatencyBuckets) == 0 {
		opt.LatencyBuckets = DefaultLatencyBuckets
	}
	for i, b := range opt.LatencyBuckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= opt.LatencyBuckets[i-1]) {
			return Options{}, errors.New("valkeyotel/gcp: LatencyBuckets must be finite and strictly increasing")
		}
	}
	opt.LatencyBuckets = slices.Clone(opt.LatencyBuckets) // later changes to the caller's slice don't matter
	opt.ResourceAttributes = slices.Clone(opt.ResourceAttributes)
	switch opt.GoogleCredentials {
	case CredentialsDefault:
		mode, err := credentialsFromEnv()
		if err != nil {
			return Options{}, err
		}
		opt.GoogleCredentials = mode
	case CredentialsAuto, CredentialsOn, CredentialsOff:
	default:
		return Options{}, fmt.Errorf("valkeyotel/gcp: unknown GoogleCredentials %d", opt.GoogleCredentials)
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
		arg, ok := os.LookupEnv("OTEL_TRACES_SAMPLER_ARG")
		if !ok {
			return true // the ratio is then 1, as OpenTelemetry specifies
		}
		r, err := strconv.ParseFloat(strings.TrimSpace(arg), 64)
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
	if opt.MemorystoreInstance != "" {
		instance := attribute.String(InstanceAttribute, opt.MemorystoreInstance)
		opts = append(opts, valkeyotel.MetricAttrs(instance), valkeyotel.TraceAttrs(instance))
	}
	return opts
}

// NewResource returns the resource that Setup gives the providers it creates: the detected
// Google Cloud environment (which includes the location), OTEL_RESOURCE_ATTRIBUTES,
// Options.ResourceAttributes, the service name, a service.instance.id unique to this process,
// and the project (Options.ProjectID, else gcp.project_id in ResourceAttributes or
// OTEL_RESOURCE_ATTRIBUTES, else the project of Application Default Credentials, if any).
//
// An application that passes its own providers to Setup uses it for them, so Google Cloud can
// tell the copies of the application apart and file the data under the right project and
// location:
//
//	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(gcp.NewResource(ctx, opt)), ...)
//
// Unless Options, ResourceAttributes or OTEL_RESOURCE_ATTRIBUTES set service.instance.id,
// every call generates a new one, so call it once per process and share the result.
func NewResource(ctx context.Context, opt Options) *resource.Resource {
	var credsProject string
	if opt.ProjectID == "" {
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
//  2. the defaults: service.name "unknown_service:<executable>" and credsProject;
//  3. OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME;
//  4. Options.ResourceAttributes;
//  5. Options.ServiceName, Options.ProjectID and the service.instance.id (see
//     chooseInstanceID).
//
// Errors don't stop it: they go to OpenTelemetry's error handler, and the rest of the
// description is used.
func buildResource(ctx context.Context, opt Options, credsProject string) (*resource.Resource, string) {
	detected, err := resource.New(ctx, resource.WithDetectors(detectors()...), resource.WithTelemetrySDK())
	if err != nil {
		otel.Handle(fmt.Errorf("valkeyotel/gcp: resource detection: %w", err))
	}
	fromEnv, err := resource.New(ctx, resource.WithFromEnv())
	if err != nil {
		otel.Handle(fmt.Errorf("valkeyotel/gcp: OTEL_RESOURCE_ATTRIBUTES: %w", err))
	}
	defaults := []attribute.KeyValue{attribute.String("service.name", "unknown_service:"+filepath.Base(os.Args[0]))}
	if credsProject != "" {
		defaults = append(defaults, attribute.String(projectAttribute, credsProject))
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
// goes to OpenTelemetry's error handler, and the merged attributes are used anyway.
func mergeResources(layers ...*resource.Resource) *resource.Resource {
	res := resource.Empty()
	for _, layer := range layers {
		var err error
		if res, err = resource.Merge(res, layer); err != nil {
			otel.Handle(fmt.Errorf("valkeyotel/gcp: resource: %w", err))
		}
	}
	return res
}

// warnIfNoLocation warns when res has no location: Google Cloud rejects metric points
// without one. On Google Cloud the location is detected; elsewhere the application sets it.
func warnIfNoLocation(res *resource.Resource) {
	for _, key := range []attribute.Key{"location", "cloud.availability_zone", "cloud.region"} {
		if stringAttr(res, key) != "" {
			return
		}
	}
	otel.Handle(errors.New("valkeyotel/gcp: no location (cloud.region or cloud.availability_zone) was detected, and Google Cloud rejects metric points without one; outside Google Cloud, set it, for example OTEL_RESOURCE_ATTRIBUTES=cloud.region=us-east1"))
}

// ClientOptions returns the valkeyotel options that send a client's telemetry through t: the
// meter and tracer providers, the command name as a metric attribute, the latency buckets
// and, with Options.MemorystoreInstance, the instance attribute.
func (t *Telemetry) ClientOptions() []valkeyotel.Option {
	return slices.Clone(t.clientOptions)
}

// NewClient is valkeyotel.NewClient with t's ClientOptions, followed by opts, which win.
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
// empty when Setup created no provider: with OTEL_SDK_DISABLED=true, or when the application
// passed both of its own. Logging it helps match a process to its time series.
func (t *Telemetry) ServiceInstanceID() string {
	return t.instanceID
}

// Shutdown sends the metrics and spans that haven't been sent yet and stops the providers
// that Setup created; ctx bounds how long it waits. Providers passed in Options are left
// running: they belong to the application. Telemetry recorded afterwards is dropped.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var err error
	if t.ownMeter != nil {
		err = t.ownMeter.Shutdown(ctx)
	}
	if t.ownTracer != nil {
		err = errors.Join(err, t.ownTracer.Shutdown(ctx))
	}
	return err
}
