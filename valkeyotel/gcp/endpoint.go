package gcp

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

// TelemetryEndpoint is Google Cloud's OTLP endpoint: the global endpoint of the Telemetry API,
// which feeds Cloud Monitoring and Cloud Trace. It is the default endpoint. The regional
// endpoints, telemetry.REGION.rep.googleapis.com:443, also work; they store only the metric
// points whose location is in their region.
const TelemetryEndpoint = "telemetry.googleapis.com:443"

// telemetryHost is the host name Google expects as the TLS server name and :authority, also
// when the Telemetry API is reached through a custom DNS name.
const telemetryHost = "telemetry.googleapis.com"

// quotaProjectEnv names the quota project, the project that Google counts the requests
// against, as for Google's client libraries. Service accounts don't need one; user
// credentials, such as those of gcloud auth application-default login, do.
const quotaProjectEnv = "GOOGLE_CLOUD_QUOTA_PROJECT"

// quotaProjectHeader is the request header that carries the quota project.
const quotaProjectHeader = "x-goog-user-project"

// CredentialsMode says when the exporters attach Google credentials (an access token from
// Application Default Credentials) to their requests.
type CredentialsMode int

const (
	// CredentialsDefault leaves the choice to the CredentialsEnv environment variable ("auto",
	// "on" or "off"), and to CredentialsAuto when it isn't set.
	CredentialsDefault CredentialsMode = iota
	// CredentialsAuto attaches Google credentials when the endpoint is a Google API host
	// (*.googleapis.com, which includes the regional endpoints and Private Service Connect
	// names such as telemetry-myendpoint.p.googleapis.com) reached over TLS. Any other
	// endpoint, such as a local OpenTelemetry Collector, gets none, so a Google token is never
	// sent to a server that isn't Google's.
	CredentialsAuto
	// CredentialsOn always attaches Google credentials. It is for Private Service Connect
	// reached through a custom DNS name: the exporters then also send telemetry.googleapis.com
	// as the TLS server name and :authority, as Google requires. The endpoint must use TLS.
	CredentialsOn
	// CredentialsOff never attaches Google credentials, for example when a collector
	// authenticates to Google itself.
	CredentialsOff
)

// CredentialsEnv is the environment variable that sets the CredentialsMode ("auto", "on" or
// "off") when Options.GoogleCredentials is CredentialsDefault.
const CredentialsEnv = "VALKEYOTEL_GCP_CREDENTIALS"

// credentialsFromEnv reads CredentialsEnv.
func credentialsFromEnv() (CredentialsMode, error) {
	switch v := strings.ToLower(strings.TrimSpace(os.Getenv(CredentialsEnv))); v {
	case "", "auto":
		return CredentialsAuto, nil
	case "on":
		return CredentialsOn, nil
	case "off":
		return CredentialsOff, nil
	default:
		return 0, fmt.Errorf("valkeyotel/gcp: %s=%q: want auto, on or off", CredentialsEnv, v)
	}
}

// signal is the OTLP signal an exporter sends, as spelled in the OTEL_EXPORTER_OTLP_<SIGNAL>_*
// environment variables.
type signal string

const (
	signalMetrics signal = "METRICS"
	signalTraces  signal = "TRACES"
)

// target is where one exporter sends its data, and how.
type target struct {
	sig         signal
	endpoint    string // as configured: host:port or a URL
	url         string // the endpoint as the http:// or https:// URL given to the exporter
	host        string
	insecure    bool   // plaintext, without TLS
	google      bool   // the Telemetry API: a *.googleapis.com host, or any host with CredentialsOn
	credentials bool   // attach Google credentials
	serverName  string // TLS server name and :authority to send instead of the endpoint's host
}

// resolveTarget picks the endpoint of one signal and decides how to reach it. The endpoint is
// the first one set of: Options.Endpoint, OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT,
// OTEL_EXPORTER_OTLP_ENDPOINT and TelemetryEndpoint. A URL's scheme decides TLS (https) or
// plaintext (http); a host:port endpoint uses TLS unless OTEL_EXPORTER_OTLP_<SIGNAL>_INSECURE
// or OTEL_EXPORTER_OTLP_INSECURE says otherwise, as the OTLP exporters do. Google credentials
// then follow mode (see CredentialsMode), whichever place the endpoint came from.
func resolveTarget(endpoint string, sig signal, mode CredentialsMode) (target, error) {
	raw := cmp.Or(
		strings.TrimSpace(endpoint),
		strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_"+string(sig)+"_ENDPOINT")),
		strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		TelemetryEndpoint,
	)
	t := target{sig: sig, endpoint: raw, url: raw}
	if !strings.Contains(raw, "://") {
		// The exporter is given a URL for host:port too. Its scheme sets TLS explicitly, so
		// the scheme of an OTEL_EXPORTER_OTLP_*ENDPOINT that raw replaced can't turn TLS off.
		t.url = "https://" + raw
		if envBool("OTEL_EXPORTER_OTLP_"+string(sig)+"_INSECURE", envBool("OTEL_EXPORTER_OTLP_INSECURE", false)) {
			t.url = "http://" + raw
		}
	}
	u, err := url.Parse(t.url)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return target{}, fmt.Errorf("valkeyotel/gcp: endpoint %q: want host:port, or an http:// or https:// URL", raw)
	}
	t.host, t.insecure = u.Hostname(), u.Scheme == "http"

	google := strings.HasSuffix(strings.ToLower(t.host), ".googleapis.com")
	t.google = google
	switch mode {
	case CredentialsOn:
		if t.insecure {
			return target{}, fmt.Errorf("valkeyotel/gcp: Google credentials need TLS, but endpoint %q is plaintext", raw)
		}
		t.google, t.credentials = true, true
		if !google {
			t.serverName = telemetryHost
		}
	case CredentialsOff:
	default: // CredentialsAuto
		t.credentials = google && !t.insecure
	}
	return t, nil
}

// envBool reads a boolean environment variable, or returns def when it is unset or invalid.
func envBool(name string, def bool) bool {
	if b, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(name))); err == nil {
		return b
	}
	return def
}

// newMetricExporter creates the OTLP exporter that sends metrics to t. It is a variable so
// tests can replace Google Cloud.
var newMetricExporter = func(ctx context.Context, t target, creds *google.Credentials) (sdkmetric.Exporter, error) {
	opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(t.url)}
	if t.serverName != "" {
		opts = append(opts, otlpmetricgrpc.WithTLSCredentials(serverNameTLS(t.serverName)))
	}
	if t.credentials && creds != nil {
		opts = append(opts, otlpmetricgrpc.WithDialOption(grpc.WithPerRPCCredentials(newGoogleAuth(t.sig, creds))))
	}
	return otlpmetricgrpc.New(ctx, opts...)
}

// newSpanExporter creates the OTLP exporter that sends spans to t. It is a variable so tests
// can replace Google Cloud.
var newSpanExporter = func(ctx context.Context, t target, creds *google.Credentials) (sdktrace.SpanExporter, error) {
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(t.url)}
	if t.serverName != "" {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(serverNameTLS(t.serverName)))
	}
	if t.credentials && creds != nil {
		opts = append(opts, otlptracegrpc.WithDialOption(grpc.WithPerRPCCredentials(newGoogleAuth(t.sig, creds))))
	}
	return otlptracegrpc.New(ctx, opts...)
}

// rootCAs are the certificate authorities that serverNameTLS trusts; nil means the system's.
// It is a variable so tests can use their own.
var rootCAs *x509.CertPool

// serverNameTLS returns TLS settings that send name as the TLS server name and verify the
// server as name, whatever host name the endpoint uses. gRPC also sends it as :authority.
func serverNameTLS(name string) credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{ServerName: name, RootCAs: rootCAs, MinVersion: tls.VersionTLS12})
}

// googleAuth attaches Google credentials to each request: an access token and, when there is
// one, the quota project.
type googleAuth struct {
	credentials.PerRPCCredentials // the access token
	quotaProject                  string
}

// newGoogleAuth returns the credentials of the exporter of sig.
func newGoogleAuth(sig signal, creds *google.Credentials) googleAuth {
	a := googleAuth{PerRPCCredentials: oauth.TokenSource{TokenSource: creds.TokenSource}}
	// Google rejects requests that carry the quota project twice, so headers set through
	// OTEL_EXPORTER_OTLP_*HEADERS win.
	if !envHeader(sig, quotaProjectHeader) {
		a.quotaProject = quotaProject(creds)
	}
	return a
}

// GetRequestMetadata returns the access token and the quota project, as request headers.
func (a googleAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	md, err := a.PerRPCCredentials.GetRequestMetadata(ctx, uri...)
	if err != nil || a.quotaProject == "" {
		return md, err
	}
	out := make(map[string]string, len(md)+1)
	maps.Copy(out, md)
	out[quotaProjectHeader] = a.quotaProject
	return out, nil
}

// quotaProject returns the quota project: GOOGLE_CLOUD_QUOTA_PROJECT, else the
// quota_project_id of the credentials file, as Google's client libraries do. It is empty for
// the credentials of Compute Engine, GKE and Cloud Run, which don't need one.
func quotaProject(creds *google.Credentials) string {
	if p := strings.TrimSpace(os.Getenv(quotaProjectEnv)); p != "" {
		return p
	}
	var file struct {
		QuotaProjectID string `json:"quota_project_id"`
	}
	if creds == nil || json.Unmarshal(creds.JSON, &file) != nil {
		return ""
	}
	return strings.TrimSpace(file.QuotaProjectID)
}

// envHeader reports whether the headers that the exporter of sig reads from the environment
// include name: those of OTEL_EXPORTER_OTLP_<SIGNAL>_HEADERS, which replace those of
// OTEL_EXPORTER_OTLP_HEADERS.
func envHeader(sig signal, name string) bool {
	headers := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_" + string(sig) + "_HEADERS"))
	if headers == "" {
		headers = os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")
	}
	for _, pair := range strings.Split(headers, ",") {
		if key, _, ok := strings.Cut(pair, "="); ok && strings.EqualFold(strings.TrimSpace(key), name) {
			return true
		}
	}
	return false
}
