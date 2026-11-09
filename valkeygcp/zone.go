package valkeygcp

import (
	"context"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// metadataTimeout bounds the metadata server query, so that creating a client
// doesn't hang outside GCP, such as on laptops and in CI.
const metadataTimeout = 100 * time.Millisecond

var (
	// metadataZoneURL is a variable so that tests can use a fake metadata server.
	metadataZoneURL = "http://169.254.169.254/computeMetadata/v1/instance/zone"
	// metadataClient doesn't use HTTP proxies: the link-local metadata server is only reachable directly.
	metadataClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
)

// WithZoneAwareReads makes NewClient and ResolveClientOption send the read-only commands of a Cluster Mode Enabled
// instance to replicas in the caller's zone first, then to replicas in other zones, and then to the primary.
// It enables ClientOption.EnableReplicaAZInfo and, unless already set, routes reads as with a reader endpoint
// (ClientOption.SendToReplicas) and uses valkey.AZAffinityNodeSelector with the zone from DetectCurrentGCPZone as
// ClientOption.ReadNodeSelector.
// With a reader endpoint in ClientOption.Standalone.ReplicaAddress, it changes nothing: the reader endpoint, not the
// client, picks the replica.
// Zone matching needs the servers to report their availability zone; otherwise reads are spread across replicas.
func WithZoneAwareReads() Option {
	return func(c *config) { c.zoneAwareReads = true }
}

// withZoneAwareReads sets up opt for WithZoneAwareReads.
func withZoneAwareReads(ctx context.Context, opt valkey.ClientOption) valkey.ClientOption {
	if len(opt.Standalone.ReplicaAddress) > 0 {
		return opt // the reader endpoint picks the replica
	}
	opt.EnableReplicaAZInfo = true
	if opt.SendToReplicas == nil {
		opt.SendToReplicas = sendReadsToReplicas
	}
	if opt.ReadNodeSelector == nil {
		opt.ReadNodeSelector = valkey.AZAffinityNodeSelector(DetectCurrentGCPZone(ctx))
	}
	return opt
}

// DetectCurrentGCPZone returns the GCP zone the caller runs in, such as "us-central1-a".
// It uses the GCP_ZONE or CLOUD_ZONE environment variable if set, and otherwise asks the
// Compute Engine metadata server, waiting at most 100ms.
// A full zone resource path, such as "projects/123456789/zones/us-central1-a", is reduced to the zone name.
// It returns "" if the zone can't be determined.
func DetectCurrentGCPZone(ctx context.Context) string {
	for _, env := range [...]string{"GCP_ZONE", "CLOUD_ZONE"} {
		if zone := zoneName(os.Getenv(env)); zone != "" {
			return zone
		}
	}
	ctx, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataZoneURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := metadataClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	return zoneName(string(body))
}

// zoneName returns the last element of a zone resource path,
// such as "us-central1-a" for "projects/123456789/zones/us-central1-a".
func zoneName(zone string) string {
	if zone = strings.TrimSpace(zone); zone == "" {
		return ""
	}
	return path.Base(zone)
}
