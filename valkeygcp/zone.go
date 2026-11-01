package valkeygcp

import (
	"context"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
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
