package valkeygcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// setMetadataZoneURL points DetectCurrentGCPZone at url and clears the environment variables that take precedence.
func setMetadataZoneURL(t *testing.T, url string) {
	t.Setenv("GCP_ZONE", "")
	t.Setenv("CLOUD_ZONE", "")
	prev := metadataZoneURL
	metadataZoneURL = url
	t.Cleanup(func() { metadataZoneURL = prev })
}

// fakeMetadataServer points DetectCurrentGCPZone at a metadata server served by handler.
func fakeMetadataServer(t *testing.T, handler http.HandlerFunc) {
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	setMetadataZoneURL(t, server.URL)
}

func TestDetectCurrentGCPZoneFromEnv(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	for _, tc := range []struct {
		gcpZone, cloudZone, want string
	}{
		{"us-central1-a", "", "us-central1-a"},
		{"projects/1234567890/zones/us-central1-b\n", "", "us-central1-b"},
		{"", "us-east1-c", "us-east1-c"},
		{" ", "projects/1234567890/zones/us-east1-d", "us-east1-d"},
		{"us-central1-a", "us-east1-c", "us-central1-a"},
	} {
		t.Setenv("GCP_ZONE", tc.gcpZone)
		t.Setenv("CLOUD_ZONE", tc.cloudZone)
		if got := DetectCurrentGCPZone(context.Background()); got != tc.want {
			t.Errorf("GCP_ZONE=%q CLOUD_ZONE=%q: got %q, want %q", tc.gcpZone, tc.cloudZone, got, tc.want)
		}
	}
}

func TestDetectCurrentGCPZoneFromMetadataServer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"resource path", http.StatusOK, "projects/987654321/zones/us-central1-f", "us-central1-f"},
		{"trailing newline", http.StatusOK, "projects/987654321/zones/us-central1-a\n", "us-central1-a"},
		{"surrounding spaces", http.StatusOK, "  projects/987654321/zones/us-east1-b  ", "us-east1-b"},
		{"zone name", http.StatusOK, "europe-west1-c", "europe-west1-c"},
		{"empty", http.StatusOK, "", ""},
		{"not found", http.StatusNotFound, "projects/987654321/zones/us-central1-a", ""},
		{"server error", http.StatusInternalServerError, "projects/987654321/zones/us-central1-a", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeMetadataServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Metadata-Flavor") != "Google" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			})
			if got := DetectCurrentGCPZone(context.Background()); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDetectCurrentGCPZoneTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"no response", func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}},
		{"no body", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeMetadataServer(t, tc.handler)
			start := time.Now()
			if got := DetectCurrentGCPZone(context.Background()); got != "" {
				t.Fatalf("got %q, want empty", got)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("took %v, want about %v", elapsed, metadataTimeout)
			}
		})
	}
}

func TestDetectCurrentGCPZoneUnreachable(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	for _, url := range []string{closed.URL, "://invalid"} {
		setMetadataZoneURL(t, url)
		if got := DetectCurrentGCPZone(context.Background()); got != "" {
			t.Errorf("metadata server %q: got %q, want empty", url, got)
		}
	}
}
