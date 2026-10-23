package engine

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

const (
	defaultMaxAttempts = 3
	maxBackoff         = 30 * time.Second
)

// RetryableError is implemented by errors a step marks as safe to retry
// (transient network/5xx/lock). Unclassified errors default to retryable;
// steps that fail persistently must type their error as definitely
// non-retryable to fail fast.
type RetryableError interface {
	Retryable() bool
}

func IsRetryable(err error) bool {
	var r RetryableError
	if errors.As(err, &r) {
		return r.Retryable()
	}
	return true
}

// RetryPolicy controls per-step retry behavior. Zero-value fields fall back
// to defaults when used via Worker.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     func(attempt int) time.Duration
	Sleep       func(ctx context.Context, d time.Duration) error
	IsRetryable func(err error) bool
}

func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts: defaultMaxAttempts,
		Backoff:     DefaultBackoff,
		Sleep:       SleepWithContext,
		IsRetryable: IsRetryable,
	}
}

// DefaultBackoff returns full jitter drawn from [0, min(2^(attempt-1) * 1s, 30s)].
// attempt is 1-based.
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		attempt = 6
	}
	base := time.Second << uint(attempt-1)
	if base <= 0 || base > maxBackoff {
		base = maxBackoff
	}
	return time.Duration(rand.Int64N(int64(base)) + 1)
}

// SleepWithContext returns ctx.Err() if the context is done before or during
// the wait, otherwise nil.
func SleepWithContext(ctx context.Context, d time.Duration) error {
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (p *RetryPolicy) withDefaults() *RetryPolicy {
	filled := *p
	if filled.MaxAttempts <= 0 {
		filled.MaxAttempts = defaultMaxAttempts
	}
	if filled.Backoff == nil {
		filled.Backoff = DefaultBackoff
	}
	if filled.Sleep == nil {
		filled.Sleep = SleepWithContext
	}
	if filled.IsRetryable == nil {
		filled.IsRetryable = IsRetryable
	}
	return &filled
}
