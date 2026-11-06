package gcp

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"testing"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func TestResolveTarget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		env      map[string]string
		endpoint string
		sig      signal
		mode     CredentialsMode
		want     target // sig and endpoint aren't compared
		wantErr  bool
	}{
		{
			name: "the Telemetry API by default",
			want: target{google: true, url: "https://telemetry.googleapis.com:443", host: telemetryHost, credentials: true},
		},
		{
			name: "OTEL_EXPORTER_OTLP_ENDPOINT set to Google: credentials too",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://telemetry.googleapis.com"},
			want: target{google: true, url: "https://telemetry.googleapis.com", host: telemetryHost, credentials: true},
		},
		{
			name: "the per-signal variable wins for its signal",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://telemetry.googleapis.com"},
			sig:  signalTraces,
			want: target{google: true, url: "https://telemetry.googleapis.com", host: telemetryHost, credentials: true},
		},
		{
			name: "and not for the other",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://telemetry.googleapis.com"},
			want: target{url: "http://collector:4317", host: "collector", insecure: true},
		},
		{
			name:     "Options.Endpoint wins: a regional endpoint",
			env:      map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317"},
			endpoint: "telemetry.us-east1.rep.googleapis.com:443",
			want:     target{google: true, url: "https://telemetry.us-east1.rep.googleapis.com:443", host: "telemetry.us-east1.rep.googleapis.com", credentials: true},
		},
		{
			name:     "a Private Service Connect name",
			endpoint: "https://telemetry-myendpoint.p.googleapis.com",
			want:     target{google: true, url: "https://telemetry-myendpoint.p.googleapis.com", host: "telemetry-myendpoint.p.googleapis.com", credentials: true},
		},
		{
			name:     "case doesn't matter",
			endpoint: "HTTPS://Telemetry.GoogleAPIs.com:443",
			want:     target{google: true, url: "HTTPS://Telemetry.GoogleAPIs.com:443", host: "Telemetry.GoogleAPIs.com", credentials: true},
		},
		{
			name:     "a custom DNS name gets no credentials by default",
			endpoint: "telemetry.internal.example.com:443",
			want:     target{url: "https://telemetry.internal.example.com:443", host: "telemetry.internal.example.com"},
		},
		{
			name:     "CredentialsOn: credentials and Google's server name",
			endpoint: "telemetry.internal.example.com:443",
			mode:     CredentialsOn,
			want:     target{google: true, url: "https://telemetry.internal.example.com:443", host: "telemetry.internal.example.com", credentials: true, serverName: telemetryHost},
		},
		{
			name:     "CredentialsOn with a Google host keeps its name",
			endpoint: "telemetry-myendpoint.p.googleapis.com:443",
			mode:     CredentialsOn,
			want:     target{google: true, url: "https://telemetry-myendpoint.p.googleapis.com:443", host: "telemetry-myendpoint.p.googleapis.com", credentials: true},
		},
		{
			name:     "CredentialsOn over plaintext",
			endpoint: "http://telemetry.internal.example.com:4317",
			mode:     CredentialsOn,
			wantErr:  true,
		},
		{
			name: "CredentialsOff, even for Google",
			mode: CredentialsOff,
			want: target{google: true, url: "https://telemetry.googleapis.com:443", host: telemetryHost},
		},
		{
			name:     "Google over plaintext gets no credentials",
			endpoint: "http://telemetry.googleapis.com:443",
			want:     target{google: true, url: "http://telemetry.googleapis.com:443", host: telemetryHost, insecure: true},
		},
		{
			name:     "host:port with OTEL_EXPORTER_OTLP_INSECURE",
			env:      map[string]string{"OTEL_EXPORTER_OTLP_INSECURE": "true"},
			endpoint: "localhost:4317",
			want:     target{url: "http://localhost:4317", host: "localhost", insecure: true},
		},
		{
			name:     "the per-signal INSECURE wins",
			env:      map[string]string{"OTEL_EXPORTER_OTLP_INSECURE": "true", "OTEL_EXPORTER_OTLP_METRICS_INSECURE": "false"},
			endpoint: "collector:4317",
			want:     target{url: "https://collector:4317", host: "collector"},
		},
		{
			name:     "an http:// OTEL_EXPORTER_OTLP_ENDPOINT doesn't make Options.Endpoint plaintext",
			env:      map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317"},
			endpoint: "telemetry.googleapis.com:443",
			want:     target{google: true, url: "https://telemetry.googleapis.com:443", host: telemetryHost, credentials: true},
		},
		{name: "another scheme", endpoint: "ftp://example.com", wantErr: true},
		{name: "a URL without a host", endpoint: "http://", wantErr: true},
		{name: "a broken URL", endpoint: "https://exa mple.com:443", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeGoogleCloud(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			sig := cmp.Or(tc.sig, signalMetrics)
			mode := cmp.Or(tc.mode, CredentialsAuto)
			got, err := resolveTarget(tc.endpoint, sig, mode)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.sig != sig || got.endpoint == "" {
				t.Errorf("signal %q, endpoint %q", got.sig, got.endpoint)
			}
			got.sig, got.endpoint = "", ""
			if got != tc.want {
				t.Errorf("got  %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestCredentialsFromEnv(t *testing.T) {
	fakeGoogleCloud(t)
	for v, want := range map[string]CredentialsMode{"": CredentialsAuto, "AUTO": CredentialsAuto, " on ": CredentialsOn, "off": CredentialsOff} {
		t.Setenv(CredentialsEnv, v)
		if got, err := credentialsFromEnv(); err != nil || got != want {
			t.Errorf("%s=%q: got %v, %v; want %v", CredentialsEnv, v, got, err, want)
		}
	}
	t.Setenv(CredentialsEnv, "yes")
	if _, err := credentialsFromEnv(); err == nil {
		t.Errorf("%s=yes was accepted", CredentialsEnv)
	}
}

func TestQuotaProject(t *testing.T) {
	fakeGoogleCloud(t)
	userCreds := &google.Credentials{JSON: []byte(`{"type": "authorized_user", "quota_project_id": "from-file"}`)}
	if got := quotaProject(userCreds); got != "from-file" {
		t.Errorf("credentials file: got %q, want from-file", got)
	}
	if got := quotaProject(&google.Credentials{}); got != "" {
		t.Errorf("metadata server credentials: got %q, want none", got)
	}
	if got := quotaProject(&google.Credentials{JSON: []byte(`{"type": "service_account"}`)}); got != "" {
		t.Errorf("service account file: got %q, want none", got)
	}
	t.Setenv(quotaProjectEnv, "from-env")
	if got := quotaProject(userCreds); got != "from-env" {
		t.Errorf("%s: got %q, want it to win", quotaProjectEnv, got)
	}
}

func TestEnvHeader(t *testing.T) {
	fakeGoogleCloud(t)
	if envHeader(signalMetrics, quotaProjectHeader) {
		t.Error("found a header without any variable")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "api-key=1, X-Goog-User-Project = p")
	if !envHeader(signalMetrics, quotaProjectHeader) {
		t.Error("missed the header in OTEL_EXPORTER_OTLP_HEADERS")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "api-key=2")
	if envHeader(signalTraces, quotaProjectHeader) {
		t.Error("OTEL_EXPORTER_OTLP_TRACES_HEADERS replaces OTEL_EXPORTER_OTLP_HEADERS, but the header was still found")
	}
}

// staticToken stands in for the access token's credentials.
type staticToken map[string]string

func (s staticToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if s == nil {
		return nil, errors.New("no token")
	}
	return s, nil
}

func (staticToken) RequireTransportSecurity() bool { return true }

func TestGoogleAuth(t *testing.T) {
	ctx := context.Background()
	token := staticToken{"authorization": "Bearer t"}
	md, err := googleAuth{PerRPCCredentials: token, quotaProject: "q"}.GetRequestMetadata(ctx)
	if err != nil || !maps.Equal(md, map[string]string{"authorization": "Bearer t", quotaProjectHeader: "q"}) {
		t.Errorf("got %v, %v; want the token and the quota project", md, err)
	}
	if len(token) != 1 {
		t.Errorf("the token's metadata was changed: %v", token)
	}
	if md, err := (googleAuth{PerRPCCredentials: token}).GetRequestMetadata(ctx); err != nil || !maps.Equal(md, token) {
		t.Errorf("without a quota project: got %v, %v; want the token only", md, err)
	}
	if _, err := (googleAuth{PerRPCCredentials: staticToken(nil), quotaProject: "q"}).GetRequestMetadata(ctx); err == nil {
		t.Error("the token's error was lost")
	}

	fakeGoogleCloud(t)
	t.Setenv(quotaProjectEnv, "q")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-goog-user-project=q")
	creds := &google.Credentials{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"})}
	if a := newGoogleAuth(signalMetrics, creds); a.quotaProject != "q" || !a.RequireTransportSecurity() {
		t.Errorf("metrics: quota project %q, TLS required %v; want q and true", a.quotaProject, a.RequireTransportSecurity())
	}
	if a := newGoogleAuth(signalTraces, creds); a.quotaProject != "" {
		t.Errorf("traces: quota project %q, want none: OTEL_EXPORTER_OTLP_TRACES_HEADERS sets it", a.quotaProject)
	}
}
