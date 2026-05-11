// helpers_test.go — tests for the retryWithBackoff helper. Internal test so we
// can call the unexported function directly without exposing it on the package
// surface.
package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoff_SucceedsFirstTry(t *testing.T) {
	calls := 0
	start := time.Now()
	err := retryWithBackoff(context.Background(), time.Second, func() error {
		calls++
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retries needed)", calls)
	}
	// First-try success should return synchronously with no sleep. Allow a tiny
	// scheduler-noise budget but flag anything that looks like it slept.
	if elapsed > 10*time.Millisecond {
		t.Errorf("first-try success took %v; expected <10ms (no sleep)", elapsed)
	}
}

func TestRetryWithBackoff_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	err := retryWithBackoff(context.Background(), 2*time.Second, func() error {
		calls++
		if calls < 4 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 4 {
		t.Errorf("calls = %d, want 4 (3 failures + 1 success)", calls)
	}
}

func TestRetryWithBackoff_TimesOut(t *testing.T) {
	sentinel := errors.New("always-fail")
	calls := 0
	start := time.Now()
	// maxDuration tight enough that fn cannot succeed; long enough to exercise
	// at least one retry to verify the deadline check actually fires (vs only
	// catching the immediate first-call path).
	err := retryWithBackoff(context.Background(), 120*time.Millisecond, func() error {
		calls++
		return sentinel
	})
	elapsed := time.Since(start)
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if calls < 2 {
		t.Errorf("calls = %d, want >=2 (initial + at least one retry)", calls)
	}
	// Should return shortly after the deadline. The next-scheduled delay can
	// push elapsed up to a bit over maxDuration + maxDelay (1s), but for a
	// 120ms budget we'd never exceed ~1.2s.
	if elapsed > 1500*time.Millisecond {
		t.Errorf("elapsed %v, expected <1.5s for 120ms budget", elapsed)
	}
}

func TestRetryWithBackoff_RespectsCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	// Cancel after the first fn call so we exercise the ctx.Done() branch
	// inside the select, not the first-iteration immediate return.
	err := retryWithBackoff(ctx, 5*time.Second, func() error {
		calls++
		if calls == 1 {
			cancel()
		}
		return errors.New("still failing")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls < 1 {
		t.Errorf("calls = %d, want >=1", calls)
	}
}

func TestRetryWithBackoff_CtxCancelledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	calls := 0
	// fn always errors so the loop must enter the select and observe ctx.Done.
	// The first invocation of fn still runs (we check fn() before ctx) — that's
	// the contract: one attempt always happens.
	err := retryWithBackoff(ctx, 5*time.Second, func() error {
		calls++
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want exactly 1 (one attempt before ctx check)", calls)
	}
}
