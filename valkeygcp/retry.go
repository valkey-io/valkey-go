package valkeygcp

import (
	"math/rand/v2"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Backoff settings: retryBase and retryCap for the errors valkey-go retries, iamBackoffBase
// and iamBackoffCap for Memorystore IAM backend errors, which Google asks to retry more
// slowly. After maxRetryAttempts attempts, memorystoreRetryDelay stops retrying.
const (
	retryBase        = 50 * time.Millisecond
	retryCap         = 2 * time.Second
	iamBackoffBase   = 250 * time.Millisecond
	iamBackoffCap    = 10 * time.Second
	maxRetryAttempts = 20
)

// memorystoreRetryDelay is exponential backoff with full jitter. Memorystore IAM backend
// errors (ERR_IAM_EXHAUSTED / ERR_IAM_OTHER) back off longer, as Google advises.
// It is a valkey.RetryDelayFn: negative stops retrying, 0 retries now, positive waits.
//
// valkey-go calls it only for errors it retries: network errors and LOADING. An ERR_IAM_*
// reply to a connection's HELLO is not one of them: valkey-go returns that error instead.
func memorystoreRetryDelay(attempts int, _ valkey.Completed, err error) time.Duration {
	if attempts >= maxRetryAttempts {
		return -1
	}
	if isIAMBackendError(err) {
		return iamBackoff(attempts)
	}
	return fullJitter(retryBase, retryCap, attempts)
}

// isIAMBackendError reports Memorystore's IAM backend replies, which Google says to retry
// with exponential backoff: ERR_IAM_EXHAUSTED (quota) and ERR_IAM_OTHER (transient, or an
// invalid username).
func isIAMBackendError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ERR_IAM_EXHAUSTED") || strings.Contains(msg, "ERR_IAM_OTHER")
}

// iamBackoff is the wait after the attempt-th consecutive IAM backend error: full jitter
// over 250ms·2^attempt, capped at 10s.
func iamBackoff(attempt int) time.Duration {
	return fullJitter(iamBackoffBase, iamBackoffCap, attempt)
}

// fullJitter returns a random duration in (0, min(base·2^attempt, limit)].
func fullJitter(base, limit time.Duration, attempt int) time.Duration {
	backoff := base << min(max(attempt, 0), 16)
	if backoff <= 0 || backoff > limit {
		backoff = limit
	}
	return time.Duration(rand.Int64N(int64(backoff)) + 1)
}
