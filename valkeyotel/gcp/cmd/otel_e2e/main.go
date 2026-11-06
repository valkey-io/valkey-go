// Command otel_e2e sends traffic to a Memorystore for Valkey instance through valkeyotel/gcp,
// so valkey-go's metrics show up in Cloud Monitoring and its traces in Cloud Trace. Stop it
// with Ctrl+C: it sends the last metrics and spans before it exits.
//
//	MEMORYSTORE_TEST_DISCOVERY_ENDPOINT=10.0.0.15:6379 go run ./gcp/cmd/otel_e2e
//
// Optional settings of this command:
//   - MEMORYSTORE_TEST_TOKEN: a password or Token Auth token, sent as AUTH default <token>.
//   - VALKEYOTEL_GCP_INSTANCE: the Memorystore instance ID, added as the valkey.instance label.
//   - VALKEYOTEL_GCP_INTERVAL: how often metrics are sent, for example 10s (default 60s).
//   - VALKEYOTEL_GCP_TRACE_RATIO: the share of new traces sampled, for example 0.1 (default
//     0.01); negative turns tracing off.
//
// valkeyotel/gcp also reads the standard OpenTelemetry variables, for example:
//   - OTEL_SERVICE_NAME: the service name, the job label (default valkeyotel-gcp-e2e).
//   - OTEL_RESOURCE_ATTRIBUTES=service.instance.id=copy-a: a fixed ID for this copy. Starting
//     two copies with the same ID is the control case of the two-copies test: their points
//     collide. Without it, each copy generates its own ID, which is logged at startup.
//   - OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317: send to a local collector instead.
//   - OTEL_METRIC_EXPORT_INTERVAL=10000 and OTEL_TRACES_SAMPLER=always_on: the interval (in
//     milliseconds) and the sampler, when the two settings above aren't set.
//   - OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION=base2_exponential_bucket_histogram:
//     exponential latency histograms instead of the fixed buckets.
//
// And two Google settings: VALKEYOTEL_GCP_CREDENTIALS=on attaches Google credentials for a
// Private Service Connect endpoint with a custom DNS name, and GOOGLE_CLOUD_QUOTA_PROJECT
// names the quota project when the credentials are a user's rather than a service account's.
//
// Every 200ms it writes a key, reads it back, reads a hot key through the client-side cache
// (DoCache) and reads a key that doesn't exist. It logs the counts about once a minute.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeyotel/gcp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	endpoint := os.Getenv("MEMORYSTORE_TEST_DISCOVERY_ENDPOINT")
	if endpoint == "" {
		return fmt.Errorf("MEMORYSTORE_TEST_DISCOVERY_ENDPOINT must be set (e.g. 10.0.0.15:6379)")
	}
	opt := gcp.Options{MemorystoreInstance: os.Getenv("VALKEYOTEL_GCP_INSTANCE")}
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		opt.ServiceName = "valkeyotel-gcp-e2e" // otherwise Setup reads OTEL_SERVICE_NAME
	}
	if s := os.Getenv("VALKEYOTEL_GCP_INTERVAL"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("VALKEYOTEL_GCP_INTERVAL=%q: %w", s, err)
		}
		opt.MetricInterval = d
	}
	if s := os.Getenv("VALKEYOTEL_GCP_TRACE_RATIO"); s != "" {
		r, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("VALKEYOTEL_GCP_TRACE_RATIO=%q: %w", s, err)
		}
		opt.TraceSampleRatio = r
	}

	ctx := context.Background()
	tel, err := gcp.Setup(ctx, opt)
	if err != nil {
		return fmt.Errorf("telemetry setup failed: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := tel.Shutdown(sctx); err != nil {
			log.Printf("⚠️ sending the last metrics and spans failed: %v", err)
			return
		}
		log.Println("📤 last metrics and spans sent")
	}()
	log.Printf("📊 sending valkey-go telemetry (service.instance.id %q, valkey.instance %q)", tel.ServiceInstanceID(), opt.MemorystoreInstance)

	client, err := tel.NewClient(valkey.ClientOption{
		InitAddress: []string{endpoint},
		Password:    os.Getenv("MEMORYSTORE_TEST_TOKEN"),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", endpoint, err)
	}
	defer client.Close()

	const hotKey = "otel-e2e:hot"
	if err := client.Do(ctx, client.B().Set().Key(hotKey).Value("hot").Ex(time.Hour).Build()).Error(); err != nil {
		return fmt.Errorf("initial SET failed: %w", err)
	}
	log.Printf("✅ connected to %s; sending traffic (press Ctrl+C to stop)...", endpoint)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	var ticks, failed int
	for {
		select {
		case <-ticker.C:
			ticks++
			if !tick(ctx, client, ticks, hotKey) {
				failed++
			}
			if ticks%300 == 0 {
				log.Printf("[Elapsed: %v] %d rounds, %d failed", time.Since(start).Round(time.Second), ticks, failed)
			}
		case <-stop:
			log.Printf("Stopping: %d rounds, %d failed in %v", ticks, failed, time.Since(start).Round(time.Second))
			return nil
		}
	}
}

// tick sends one round of commands and reports whether all of them worked.
func tick(ctx context.Context, client valkey.Client, n int, hotKey string) bool {
	key := fmt.Sprintf("otel-e2e:key:%d", n%100)
	val := strconv.Itoa(n)
	ok := true
	if err := client.Do(ctx, client.B().Set().Key(key).Value(val).Ex(time.Minute).Build()).Error(); err != nil {
		log.Printf("❌ SET failed: %v", err)
		ok = false
	}
	if got, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString(); err != nil || got != val {
		log.Printf("❌ GET failed or returned the wrong value: %v", err)
		ok = false
	}
	if err := client.DoCache(ctx, client.B().Get().Key(hotKey).Cache(), 30*time.Second).Error(); err != nil {
		log.Printf("❌ cached GET failed: %v", err)
		ok = false
	}
	if err := client.Do(ctx, client.B().Get().Key("otel-e2e:missing").Build()).Error(); !valkey.IsValkeyNil(err) {
		log.Printf("❌ GET of a missing key: got %v, want a nil reply", err)
		ok = false
	}
	return ok
}
