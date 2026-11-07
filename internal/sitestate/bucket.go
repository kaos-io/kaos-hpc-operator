// Package sitestate reads the state of a project's site jobs from the Slurm REST API in batches
// (PRD-HPC-1461 D-36, D-45): one jobs/state call per chunk of explicit job IDs, every call through a
// token bucket shared by all HPCSites of one site profile.
package sitestate

import (
	"context"
	"sync"
	"time"
)

// Bucket is a token bucket: Rate tokens per second up to Burst. Wait blocks for one token.
type Bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	// Now and Sleep are the clock; tests replace them.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

// NewBucket returns a full bucket. Rate is tokens per second; both rate and burst are at least 1 token.
func NewBucket(rate float64, burst int) *Bucket {
	b := &Bucket{rate: max(rate, 0.001), burst: float64(max(burst, 1)), Now: time.Now, Sleep: sleepCtx}
	b.tokens, b.last = b.burst, b.Now()
	return b
}

// Wait takes one token, sleeping until one is available. It returns the context's error if ctx ends first.
func (b *Bucket) Wait(ctx context.Context) error {
	for {
		d := b.take()
		if d <= 0 {
			return nil
		}
		if err := b.Sleep(ctx, d); err != nil {
			return err
		}
	}
}

// take removes a token and returns 0, or returns how long until one is available.
func (b *Bucket) take() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.Now()
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return 0
	}
	return time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
