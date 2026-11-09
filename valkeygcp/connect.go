package valkeygcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// connect builds the valkey client. valkey-go dials each InitAddress once and returns an
// ERR_IAM_* reply to HELLO as an error without retrying, so connect retries those with
// exponential backoff, as Google recommends for these errors. ctx bounds the retries, which
// also stop after setupTimeout when ctx has no earlier deadline.
func connect(ctx context.Context, opt valkey.ClientOption) (valkey.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				return nil, err // valkey.NewClient takes no context
			}
			return nil, fmt.Errorf("valkeygcp: %w (gave up after %d attempts: %w)", lastErr, attempt, err)
		}
		client, err := valkey.NewClient(opt)
		if err == nil {
			return client, nil
		}
		if !isIAMBackendError(err) {
			return nil, explainHandshakeError(err)
		}
		lastErr = err
		t := time.NewTimer(iamBackoff(attempt))
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}
	}
}

// explainHandshakeError adds a hint when an ACL rule refused CLIENT TRACKING, which
// valkey-go sends on every new connection unless ClientOption.DisableCache is set. The hint
// names no user: Memorystore matches ACL rules to the IAM principal that authenticated,
// which isn't the AUTH username "default", and its NOPERM reply, kept in the error, names
// the rule's user.
func explainHandshakeError(err error) error {
	if msg := err.Error(); errors.Is(err, valkey.ErrNoCache) && strings.Contains(msg, "NOPERM") && strings.Contains(msg, "TRACKING") {
		return fmt.Errorf("valkeygcp: the ACL rule must allow CLIENT TRACKING (+client|tracking), or set ClientOption.DisableCache: %w", err)
	}
	return err
}
