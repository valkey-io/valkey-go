package valkeygcp

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TLSMode mirrors transit_encryption_mode + server_ca_mode.
type TLSMode int

// TLSMode values. Each mode except TLSAuto matches a transit encryption and server CA mode.
const (
	// TLSAuto, the zero value, is TLSCustomCA in WithTLS: the CA certificates in TLSOptions
	// are trusted whatever the instance's server CA mode.
	TLSAuto          TLSMode = iota
	TLSDisabled              // TRANSIT_ENCRYPTION_DISABLED
	TLSPerInstanceCA         // SERVER_AUTHENTICATION + GOOGLE_MANAGED_PER_INSTANCE_CA
	TLSSharedCA              // SERVER_AUTHENTICATION + GOOGLE_MANAGED_SHARED_CA
	TLSCustomCA              // SERVER_AUTHENTICATION + CUSTOMER_MANAGED_CAS_CA, or any caller PEM
)

func (m TLSMode) String() string {
	switch m {
	case TLSAuto:
		return "TLSAuto"
	case TLSDisabled:
		return "TLSDisabled"
	case TLSPerInstanceCA:
		return "TLSPerInstanceCA"
	case TLSSharedCA:
		return "TLSSharedCA"
	case TLSCustomCA:
		return "TLSCustomCA"
	}
	return "TLSMode(" + strconv.Itoa(int(m)) + ")"
}

// TLSOptions configures in-transit encryption.
type TLSOptions struct {
	Mode       TLSMode
	CAPEM      []byte // server CA certificates (PEM), for any mode
	CAFile     string // alternative to CAPEM
	ServerName string // optional verification override
	// Base is an optional starting point, cloned and never mutated. Only RootCAs,
	// MinVersion (raised to TLS 1.2) and ServerName (when set above) are changed; its other
	// settings are kept as they are, so InsecureSkipVerify still turns verification off.
	Base *tls.Config
}

// buildTLSConfig turns the resolved mode plus CA material into a *tls.Config.
// Precedence: CAPEM > CAFile > discovered PEMs > Base.RootCAs. discovered holds CA
// certificates obtained another way, such as from the Memorystore API; WithTLS passes none.
func buildTLSConfig(opt TLSOptions, mode TLSMode, discovered []string) (*tls.Config, error) {
	if mode == TLSDisabled || mode == TLSAuto {
		return nil, nil
	}
	pemBytes := opt.CAPEM
	if len(pemBytes) == 0 && opt.CAFile != "" {
		b, err := os.ReadFile(opt.CAFile)
		if err != nil {
			return nil, fmt.Errorf("valkeygcp: read TLSOptions.CAFile: %w", err)
		}
		pemBytes = b
	}
	if len(pemBytes) == 0 && len(discovered) > 0 {
		pemBytes = []byte(strings.Join(discovered, "\n"))
	}
	var pool *x509.CertPool
	switch {
	case len(pemBytes) > 0:
		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("valkeygcp: no valid PEM certificate in the CA bundle")
		}
	case opt.Base != nil && opt.Base.RootCAs != nil:
		pool = opt.Base.RootCAs // caller-built pool, kept as is
	default:
		return nil, errNeedsCA(mode)
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12} // Memorystore supports TLS 1.2+
	if opt.Base != nil {
		cfg = opt.Base.Clone()
		if cfg.MinVersion < tls.VersionTLS12 {
			cfg.MinVersion = tls.VersionTLS12
		}
	}
	cfg.RootCAs = pool
	if opt.ServerName != "" {
		cfg.ServerName = opt.ServerName
	}
	// ServerName stays empty by default, so crypto/tls verifies each node's certificate
	// against the IP address valkey-go dials.
	return cfg, nil
}

// errNeedsCA reports a TLS mode without CA material.
func errNeedsCA(mode TLSMode) error {
	return fmt.Errorf("valkeygcp: TLS mode %v needs CA certificates: set TLSOptions.CAPEM or TLSOptions.CAFile to the instance's server CA certificates", mode)
}

// errTLSConfigConflict reports a caller-built TLSConfig next to WithTLS.
var errTLSConfigConflict = errors.New("valkeygcp: ClientOption.TLSConfig conflicts with WithTLS, which builds the TLS configuration; to start from your own settings, set TLSOptions.Base")

// WithTLS turns on in-transit encryption for NewClient and ResolveClientOption. Every node's
// certificate is verified against the CA certificates in opt (CAPEM, CAFile or
// Base.RootCAs), with TLS 1.2 or later. All certificates in the bundle are trusted, so a
// bundle holding more than one CA, as during a server CA rotation, verifies nodes signed by
// any of them. The bundle is read once, when NewClient or ResolveClientOption runs.
//
// opt.Mode names the instance's server CA mode and appears in errors; TLSAuto is
// TLSCustomCA. Without CA certificates NewClient fails instead of connecting unverified.
// TLSDisabled leaves TLS off. A ClientOption.TLSConfig set next to WithTLS is an error.
func WithTLS(opt TLSOptions) Option {
	return func(c *config) { c.tls = &opt }
}

// newTLSConfig checks opt.Mode and builds the TLS configuration WithTLS asks for; nil for
// TLSDisabled.
func newTLSConfig(opt TLSOptions) (*tls.Config, error) {
	mode := opt.Mode
	switch {
	case mode == TLSAuto:
		mode = TLSCustomCA
	case mode < TLSAuto || mode > TLSCustomCA:
		return nil, fmt.Errorf("valkeygcp: unknown TLS mode %v", mode)
	}
	return buildTLSConfig(opt, mode, nil)
}
