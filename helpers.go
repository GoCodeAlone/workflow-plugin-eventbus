package eventbus

import (
	"context"
	"time"
)

// retryWithBackoff calls fn repeatedly with exponential backoff
// (50ms → 100ms → 200ms → … capped at 1s) until either:
//   - fn returns nil (success), or
//   - maxDuration has elapsed (returns fn's last error), or
//   - ctx is cancelled (returns ctx.Err()).
//
// The first call to fn happens immediately without any initial delay, so a
// successful fn returns within a single syscall with no sleep.
//
// Used by streamModule.Start and consumerModule.Start to bridge the broker-
// module Start ordering gap: when modular runs Start hooks in module-instance
// order, the broker may not yet be in the LookupRuntime registry when a
// stream/consumer's Start fires. Bounded retry gives the broker a 10-second
// window to come up before we declare the dispatch unrecoverable.
func retryWithBackoff(ctx context.Context, maxDuration time.Duration, fn func() error) error {
	deadline := time.Now().Add(maxDuration)
	delay := 50 * time.Millisecond
	const maxDelay = time.Second
	var lastErr error
	for {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, maxDelay)
	}
}
