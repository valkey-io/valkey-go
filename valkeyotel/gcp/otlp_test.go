package gcp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// The exporter constructors as the package defines them, before tests replace them.
var realMetricExporter, realSpanExporter = newMetricExporter, newSpanExporter

// withRealExporters undoes fakeGoogleCloud's exporters, so the data really goes over OTLP/gRPC.
// fakeGoogleCloud's cleanup restores them.
func withRealExporters(t *testing.T) {
	t.Helper()
	newMetricExporter, newSpanExporter = realMetricExporter, realSpanExporter
}

// otlpRequest is what one OTLP export request carried.
type otlpRequest struct {
	service       string // "metrics" or "traces"
	authority     string
	authorization string
	quotaProjects []string // every x-goog-user-project value, to catch duplicates
	points        int      // metric data points
	spans         int
}

// otlpServer is a local OTLP/gRPC receiver standing in for the Telemetry API or a collector.
type otlpServer struct {
	addr string

	mu         sync.Mutex
	requests   []otlpRequest
	serverName []string // the TLS server name (SNI) of each connection
}

func (s *otlpServer) record(ctx context.Context, r otlpRequest) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get(":authority"); len(v) > 0 {
		r.authority = v[0]
	}
	if v := md.Get("authorization"); len(v) > 0 {
		r.authorization = v[0]
	}
	r.quotaProjects = md.Get(quotaProjectHeader)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r)
}

// received returns the requests of one service.
func (s *otlpServer) received(service string) []otlpRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []otlpRequest
	for _, r := range s.requests {
		if r.service == service {
			out = append(out, r)
		}
	}
	return out
}

func (s *otlpServer) serverNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.serverName)
}

type metricsReceiver struct {
	colmetricpb.UnimplementedMetricsServiceServer
	s *otlpServer
}

func (r metricsReceiver) Export(ctx context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	points := 0
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				points += len(m.GetSum().GetDataPoints()) + len(m.GetGauge().GetDataPoints()) +
					len(m.GetHistogram().GetDataPoints()) + len(m.GetExponentialHistogram().GetDataPoints()) +
					len(m.GetSummary().GetDataPoints())
			}
		}
	}
	r.s.record(ctx, otlpRequest{service: "metrics", points: points})
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

type traceReceiver struct {
	coltracepb.UnimplementedTraceServiceServer
	s *otlpServer
}

func (r traceReceiver) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	spans := 0
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			spans += len(ss.GetSpans())
		}
	}
	r.s.record(ctx, otlpRequest{service: "traces", spans: spans})
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// startOTLPServer starts a local OTLP receiver: plaintext with a nil cert, else TLS.
func startOTLPServer(t *testing.T, cert *tls.Certificate) *otlpServer {
	t.Helper()
	s := &otlpServer{}
	var opts []grpc.ServerOption
	if cert != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{*cert},
			MinVersion:   tls.VersionTLS12,
			GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.serverName = append(s.serverName, hello.ServerName)
				return nil, nil
			},
		})))
	}
	srv := grpc.NewServer(opts...)
	colmetricpb.RegisterMetricsServiceServer(srv, metricsReceiver{s: s})
	coltracepb.RegisterTraceServiceServer(srv, traceReceiver{s: s})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.addr = lis.Addr().String()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return s
}

// googleCertificate returns a self-signed certificate for telemetry.googleapis.com, and makes
// the exporters trust it, as Google's certificate is trusted in production.
func googleCertificate(t *testing.T) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: telemetryHost},
		DNSNames:              []string{telemetryHost},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	orig := rootCAs
	rootCAs = pool
	t.Cleanup(func() { rootCAs = orig })
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// sendOne records a metric and a sampled span, then shuts tel down, which sends both. A broken
// connection fails within the timeout instead of retrying for a minute.
func sendOne(t *testing.T, tel *Telemetry) {
	t.Helper()
	recordOne(t, tel)
	_, span := tel.TracerProvider().Tracer("test").Start(context.Background(), "GET")
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tel.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// TestOTLP_PrivateServiceConnectCustomName sends through the real exporters to a local TLS
// server that stands in for Private Service Connect behind a custom DNS name: Google needs
// telemetry.googleapis.com as the TLS server name and :authority, and the credentials.
func TestOTLP_PrivateServiceConnectCustomName(t *testing.T) {
	check := func(t *testing.T, srv *otlpServer, wantQuota []string) {
		t.Helper()
		for _, service := range []string{"metrics", "traces"} {
			reqs := srv.received(service)
			if len(reqs) == 0 {
				t.Errorf("no %s request arrived", service)
				continue
			}
			for _, r := range reqs {
				if r.authority != telemetryHost || r.authorization != "Bearer test-token" || !slices.Equal(r.quotaProjects, wantQuota) {
					t.Errorf("%s request: :authority %q, authorization %q, quota project %q; want %q, the token and %q",
						service, r.authority, r.authorization, r.quotaProjects, telemetryHost, wantQuota)
				}
			}
		}
		for _, name := range srv.serverNames() {
			if name != telemetryHost {
				t.Errorf("TLS server name %q, want %q", name, telemetryHost)
			}
		}
	}

	t.Run("credentials, server name and quota project", func(t *testing.T) {
		fakeGoogleCloud(t)
		withRealExporters(t)
		withRegion(t)
		srv := startOTLPServer(t, googleCertificate(t))
		t.Setenv(quotaProjectEnv, "quota-project")
		sendOne(t, setup(t, Options{Endpoint: srv.addr, GoogleCredentials: CredentialsOn, TraceSampleRatio: 1}))
		check(t, srv, []string{"quota-project"})
	})
	t.Run("the quota project of OTEL_EXPORTER_OTLP_HEADERS isn't sent twice", func(t *testing.T) {
		fakeGoogleCloud(t)
		withRealExporters(t)
		withRegion(t)
		srv := startOTLPServer(t, googleCertificate(t))
		t.Setenv(quotaProjectEnv, "quota-project")
		t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-goog-user-project=from-headers")
		sendOne(t, setup(t, Options{Endpoint: srv.addr, GoogleCredentials: CredentialsOn, TraceSampleRatio: 1}))
		check(t, srv, []string{"from-headers"})
	})
}

// TestOTLP_HostPortMeansTLS checks that a host:port endpoint uses TLS, as documented, even
// when the OTEL_EXPORTER_OTLP_ENDPOINT it replaces is an http:// URL: the exporters read that
// variable too, and would otherwise send in plaintext, for example to Google.
func TestOTLP_HostPortMeansTLS(t *testing.T) {
	send := func(t *testing.T, srv *otlpServer) {
		t.Helper()
		tel := setup(t, Options{Endpoint: srv.addr, TraceSampleRatio: -1})
		recordOne(t, tel)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tel.Shutdown(ctx) // fails when the connection does
	}
	t.Run("TLS, so a plaintext server gets nothing", func(t *testing.T) {
		fakeGoogleCloud(t)
		withRealExporters(t)
		captureErrors(t) // the failed export
		srv := startOTLPServer(t, nil)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")
		send(t, srv)
		if reqs := srv.received("metrics"); len(reqs) > 0 {
			t.Errorf("%d plaintext requests arrived, want none", len(reqs))
		}
	})
	t.Run("plaintext with OTEL_EXPORTER_OTLP_INSECURE", func(t *testing.T) {
		fakeGoogleCloud(t)
		withRealExporters(t)
		srv := startOTLPServer(t, nil)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:4317")
		t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
		send(t, srv)
		if reqs := srv.received("metrics"); len(reqs) == 0 {
			t.Error("no plaintext request arrived")
		}
	})
}

// TestOTLP_LocalCollector sends to a local plaintext collector, chosen with
// OTEL_EXPORTER_OTLP_ENDPOINT: no Google credentials are needed or sent.
func TestOTLP_LocalCollector(t *testing.T) {
	f := fakeGoogleCloud(t)
	withRealExporters(t)
	srv := startOTLPServer(t, nil)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+srv.addr)
	sendOne(t, setup(t, Options{TraceSampleRatio: 1}))
	for _, service := range []string{"metrics", "traces"} {
		reqs := srv.received(service)
		if len(reqs) == 0 {
			t.Errorf("no %s request arrived", service)
		}
		for _, r := range reqs {
			if r.authorization != "" || len(r.quotaProjects) > 0 {
				t.Errorf("%s request carried Google credentials: %q, %q", service, r.authorization, r.quotaProjects)
			}
		}
	}
	if _, _, lookups := f.targets(); lookups != 0 {
		t.Errorf("%d credential lookups, want none", lookups)
	}
}

// TestOTLP_SplitsLargeExports checks the 200-point limit through the real metric exporter.
func TestOTLP_SplitsLargeExports(t *testing.T) {
	fakeGoogleCloud(t)
	withRealExporters(t)
	srv := startOTLPServer(t, nil)
	tel := setup(t, Options{Endpoint: "http://" + srv.addr, TraceSampleRatio: -1})
	counter, err := tel.MeterProvider().Meter("test").Int64Counter("test_series")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 250 { // 250 time series, so 250 points
		counter.Add(context.Background(), 1, metric.WithAttributes(attribute.Int("n", i)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	var points []int
	for _, r := range srv.received("metrics") {
		points = append(points, r.points)
	}
	if !slices.Equal(points, []int{maxDataPointsPerRequest, 50}) {
		t.Errorf("points per request = %v, want [200 50]", points)
	}
}
