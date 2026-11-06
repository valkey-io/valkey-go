# valkeyotel/gcp: valkey-go telemetry in Google Cloud

Package `gcp` sends [`valkeyotel`](..)'s metrics to **Cloud Monitoring** and its traces to **Cloud Trace**, with defaults that suit Memorystore for Valkey. One call at startup wires it up.

It uses the standard OpenTelemetry OTLP exporters and Google Cloud's OTLP endpoint, the [Telemetry API](https://cloud.google.com/stackdriver/docs/reference/telemetry/overview) (`telemetry.googleapis.com`). Google's older Go exporters for Cloud Monitoring and Cloud Trace are deprecated.

`valkeyotel` doesn't import this package, so applications that don't use it don't link gRPC or the exporters.

## Quickstart

```go
import "github.com/valkey-io/valkey-go/valkeyotel/gcp"

tel, err := gcp.Setup(ctx, gcp.Options{
	ServiceName:         "checkout",    // the application: the job label, and the service in Cloud Trace
	MemorystoreInstance: "my-instance", // optional: the valkey.instance label on every metric and span
})
if err != nil {
	return err
}
defer tel.Shutdown(context.Background()) // sends the last metrics and spans

client, err := tel.NewClient(valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}})
```

On Compute Engine, GKE and Cloud Run, that's all: the project comes from Application Default Credentials, and the environment, including the location, is detected. The identity needs permission to write metrics and traces; see [Required setup](#required-setup).

`tel.NewClient` is `valkeyotel.NewClient` with `tel.ClientOptions()`; options passed to it win. To build the `valkey.ClientOption` another way (for example with IAM or Token Auth), pass the result to `tel.NewClient`, or pass `tel.ClientOptions()` to `valkeyotel.NewClient`. Call `Setup` once per process and share `tel` between clients.

## Settings

For each setting, a value in `Options` wins, then the standard OpenTelemetry environment variable, then the default.

| Setting | `Options` field | Environment variable | Default |
|---|---|---|---|
| Project that receives the data | `ProjectID` | `gcp.project_id` in `OTEL_RESOURCE_ATTRIBUTES` | The project of Application Default Credentials |
| Endpoint | `Endpoint` | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then `OTEL_EXPORTER_OTLP_ENDPOINT` | `telemetry.googleapis.com:443` |
| Google credentials | `GoogleCredentials` | `VALKEYOTEL_GCP_CREDENTIALS` (`auto`, `on` or `off`) | `auto`: for `*.googleapis.com` endpoints only |
| Service name (`job` label) | `ServiceName` | `OTEL_SERVICE_NAME`, or `service.name` in `OTEL_RESOURCE_ATTRIBUTES` | `unknown_service:<executable>` |
| ID of this running copy (`instance` label) | `ServiceInstanceID` | `service.instance.id` in `OTEL_RESOURCE_ATTRIBUTES` | Generated; see [One ID per running copy](#one-id-per-running-copy) |
| Other resource attributes | `ResourceAttributes` | `OTEL_RESOURCE_ATTRIBUTES` | The detected environment |
| Memorystore instance label | `MemorystoreInstance` | — | None |
| How often metrics are sent | `MetricInterval` | `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds) | 60 s; at least 5 s when sending to Google |
| Share of new traces sampled | `TraceSampleRatio` (negative turns tracing off) | `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` | 1%, and always when the parent span is sampled |
| Latency buckets | `LatencyBuckets` | — | 100 µs to 10 s |
| The application's own providers | `MeterProvider`, `TracerProvider` | — | Setup creates its own |
| Also install as OpenTelemetry's global providers | `SetGlobal` | — | Off |
| Turn everything off | — | `OTEL_SDK_DISABLED=true` | — |

- `ResourceAttributes` win over `OTEL_RESOURCE_ATTRIBUTES`, and `ServiceName`, `ServiceInstanceID` and `ProjectID` win over the same attributes in `ResourceAttributes`.
- An invalid environment variable counts as unset. For example, an unknown `OTEL_TRACES_SAMPLER` gets the 1% default, rather than OpenTelemetry's fallback, which samples every trace.
- The exporters read the other `OTEL_EXPORTER_OTLP_*` variables as usual: headers, timeout, compression, and `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION=base2_exponential_bucket_histogram` for exponential latency histograms instead of fixed buckets.
- Setup always sends OTLP over gRPC: it doesn't read `OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_TRACES_EXPORTER` or `OTEL_METRICS_EXPORTER`. A collector must accept OTLP/gRPC, usually on port 4317.

**Why every 60 seconds:** it's OpenTelemetry's default. Nothing is sampled: every command is counted and timed in memory, and the interval only says how often the totals are sent. Cloud Monitoring charts work at the minute level, and every send costs one sample or more per time series. Use a shorter interval for tests.

## Endpoints and credentials

Google credentials follow the endpoint that's actually used, wherever it was set:

| Endpoint | Google credentials, with the default `auto` |
|---|---|
| `telemetry.googleapis.com:443`, the default | Attached |
| A regional endpoint, `telemetry.REGION.rep.googleapis.com:443` | Attached. It stores only the metric points whose location is in its region |
| Private Service Connect, `telemetry-ENDPOINT.p.googleapis.com:443` | Attached |
| Private Service Connect behind a custom DNS name | Not attached, so a Google token never goes to a host that may not be Google's. Set `GoogleCredentials: gcp.CredentialsOn` or `VALKEYOTEL_GCP_CREDENTIALS=on`: the token is attached, and `telemetry.googleapis.com` is sent as the TLS server name and `:authority`, as Google requires |
| A local OpenTelemetry Collector, such as `http://localhost:4317` | Not attached: the collector authenticates to Google itself |

A URL uses TLS with `https://` and plaintext with `http://`. A `host:port` endpoint uses TLS, unless `OTEL_EXPORTER_OTLP_INSECURE=true`. Credentials are never sent in plaintext. Setup looks up the credentials and the project only when it attaches credentials, so an application that sends to a collector needs neither.

**Quota project.** Service accounts, as on Compute Engine, GKE and Cloud Run, need nothing more. User credentials (`gcloud auth application-default login`), for example on a laptop, also need a quota project: set `GOOGLE_CLOUD_QUOTA_PROJECT`, or run `gcloud auth application-default set-quota-project PROJECT_ID`. Setup sends it in the `x-goog-user-project` header, unless `OTEL_EXPORTER_OTLP_HEADERS` already does: Google rejects requests that carry it twice.

## One ID per running copy

Google Cloud labels each time series with `instance`, taken from `service.instance.id`. Copies of an application that share an ID write to the same time series, so their points collide. Google refuses some of them with `One or more points were written more frequently than the maximum sampling period configured for the metric`, and what's left mixes the copies' values. A common cause is a fixed `service.instance.id` in the environment of a Kubernetes Deployment, which all its pods share. Setup uses the first one found:

1. `Options.ServiceInstanceID`.
2. `service.instance.id` in `Options.ResourceAttributes` or `OTEL_RESOURCE_ATTRIBUTES`.
3. A generated `<host>-<pid>-<8 random hex digits>`. The host is the first one found of:
   - the Cloud Run instance ID;
   - `k8s.pod.name` in `OTEL_RESOURCE_ATTRIBUTES`;
   - the Compute Engine VM name;
   - the OS hostname: the pod name on GKE, the machine's name on a laptop, in CI or on another cloud.

   Without any of them, or with only `localhost`, the host is `unknown` and Setup logs a warning.

The generated ID is unique per process, even for processes that share a host and a PID, as containers running as PID 1 do. It stays the same while the process runs, and a restarted process gets a new one; an application that needs a stable ID sets 1 or 2. `tel.ServiceInstanceID()` returns the ID, so it can be logged.

## Applications that already use OpenTelemetry

- Setup changes nothing global: no global providers and no propagator. With `SetGlobal: true`, it installs the providers it created, and never the application's own.
- To use the application's own providers, pass them in `Options`. Setup then creates no exporters, and `tel.Shutdown` leaves those providers running. valkey-go's metrics still get the latency buckets and the command and instance labels.
- The application is then responsible for its providers' resource: Google Cloud needs `service.instance.id` and the location. `gcp.NewResource` returns the resource that Setup would use, so that's one line:

```go
res := gcp.NewResource(ctx, gcp.Options{ServiceName: "checkout"})
mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(exporter))

tel, err := gcp.Setup(ctx, gcp.Options{MeterProvider: mp, TracerProvider: tp, MemorystoreInstance: "my-instance"})
```

Call `NewResource` once per process: unless an ID is set, each call generates a new one.

## Platform notes

- **Cloud Run:** telemetry is sent in the background, so use instance-based billing (CPU always allocated). With request-based billing, the CPU is throttled between requests, and sends can be late or lost. Cloud Run sends `SIGTERM` 10 seconds before it stops an instance: call `tel.Shutdown` then, to send the last data.
- **GKE:** the `namespace` label stays empty unless the pod sets `k8s.namespace.name` in `OTEL_RESOURCE_ATTRIBUTES`, for example from the Kubernetes downward API.
- **Outside Google Cloud** (a laptop, CI or another cloud): set the location (see below) and, with user credentials, the quota project.

## Making sure the data arrives

- **Location.** Google Cloud rejects metric points without one. On Google Cloud it's detected; elsewhere, set it, for example `OTEL_RESOURCE_ATTRIBUTES=cloud.region=us-east1`. Setup logs a warning when metrics go to Google without a location.
- **200 data points per request.** That's the Telemetry API's limit, and the OTLP exporter sends each interval in a single request. Setup splits larger intervals into several requests.
- **One point every 5 seconds.** That's the most Cloud Monitoring accepts per time series, so Setup refuses a shorter interval when metrics go to the Telemetry API.
- **No reserved labels.** `location`, `cluster`, `namespace`, `job`, `instance` and `project_id` belong to `prometheus_target`, so the instance label is `valkey.instance`.
- **Errors.** A failed send, for example because of a missing role, never affects valkey-go's commands. Temporary errors are retried within the exporter's timeout, 10 seconds by default (`OTEL_EXPORTER_OTLP_TIMEOUT`); after that, the send is dropped. The metrics are cumulative, so the next send still carries the full totals, and no counts are lost. Errors go to OpenTelemetry's error handler, which logs them; `otel.SetErrorHandler` sends them elsewhere.
- **The first write to a project.** Cloud Monitoring creates each metric's definition the first time it receives the metric. Copies that start at the same moment can then get `Too many concurrent edits to the metric` once. Google returns it as `InvalidArgument`, so it isn't retried: that copy loses one interval, and its next send goes through. It doesn't happen again once the definitions exist.

## Metrics in Cloud Monitoring

The Telemetry API stores OTLP metrics in Google Cloud Managed Service for Prometheus, as `prometheus.googleapis.com/<name>/<kind>`, under the `prometheus_target` resource (labels `project_id`, `location`, `cluster`, `namespace`, `job` and `instance`). Query them with PromQL.

| Metric | Kind | Unit | Labels | Meaning |
|---|---|---|---|---|
| `valkey_command_duration_seconds` | Histogram | s | `operation`, `valkey.instance` | Command latency |
| `valkey_command_errors` | Counter | | `operation`, `valkey.instance` | Command errors; a nil reply isn't an error |
| `valkey_do_cache_hits`, `valkey_do_cache_miss` | Counter | | `valkey.instance` | Client-side cache hits and misses (`DoCache`) |
| `valkey_dial_attempt`, `valkey_dial_success` | Counter | | `valkey.instance` | Connection attempts and successes |
| `valkey_dial_conns` | Up-down counter | | `valkey.instance` | Open connections |
| `valkey_dial_latency` | Histogram | s | `valkey.instance` | Time to open a connection |

- `operation` is the command name, such as `GET`. Pipelines (`DoMulti`) are labelled with up to five command names, such as `SET GET`.
- `valkey.instance` is there only with `MemorystoreInstance`. In PromQL, a label name with a dot is quoted: `{"valkey.instance"="my-instance"}`.
- The resource attributes that aren't labels, such as `cloud.platform`, go to the `target_info` metric.

## Logs and traces

The package sends no logs. To show an application's logs with its traces in Cloud Logging, write structured JSON logs with these fields, taking the IDs from `trace.SpanContextFromContext(ctx)`:

- `logging.googleapis.com/trace`: `projects/PROJECT_ID/traces/TRACE_ID`;
- `logging.googleapis.com/spanId`: the span ID;
- `logging.googleapis.com/trace_sampled`: whether the span is sampled.

## Required setup

- The Telemetry API enabled in the project: `gcloud services enable telemetry.googleapis.com --project=PROJECT_ID`.
- For the identity of Application Default Credentials, for example the VM's service account, permission to write metrics and traces in the project, with either:
  - `roles/telemetry.writer` (Cloud Telemetry Writer), which covers metrics, traces and logs; or
  - `roles/monitoring.metricWriter` (Monitoring Metric Writer) for metrics, and `roles/telemetry.tracesWriter` (Cloud Telemetry Traces Writer) for traces.
- Google's documentation also lists `roles/serviceusage.serviceUsageConsumer` (Service Usage Consumer) on the quota project.

```bash
gcloud projects add-iam-policy-binding PROJECT_ID --member=serviceAccount:SA_EMAIL --role=roles/telemetry.writer
```

## Cost

Metrics are billed at Managed Service for Prometheus rates, per sample: a histogram point costs 2 samples, plus 1 per bucket that has data. Spans are billed by Cloud Trace. The defaults keep both low: metrics are sent once a minute, labels are limited to the command name and the instance (never keys), and 1% of new traces are sampled. Each distinct pipeline label is its own time series.

## Testing

```bash
go test -race ./gcp/...
```

The unit tests replace Google Cloud with in-memory exporters and a local OTLP server. `TestTelemetry_LiveClient` also needs a local Valkey without a password at `127.0.0.1:6379` (or `VALKEYOTEL_GCP_TEST_ADDR`), and is skipped without one.

`gcp/cmd/otel_e2e` sends traffic to a real instance, so the metrics and traces show up in the Console; its package comment lists the settings. To check the per-copy ID, run two copies on the same machine: both must appear, with different `instance` values. As a control, start both with `OTEL_RESOURCE_ATTRIBUTES=service.instance.id=same`: their points collide.
