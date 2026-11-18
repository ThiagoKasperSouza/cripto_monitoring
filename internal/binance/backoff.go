// Package binance implements the resilient WebSocket client used by the
// Market Data Gateway (Camada 1 — Aquisição Externa) to ingest the trade
// streams published by the Binance public WebSocket API.
package binance

import (
	"math"
	"math/rand"
	"time"
)

// Backoff implements an exponential backoff with jitter for WebSocket
// reconnections. It exists to avoid the "thundering herd" problem: when the
// Binance API drops N gateways at once, naive immediate retries would
// reconnect all nodes simultaneously and DoS the exchange (and violate its
// rate limits).
//
// Algorithm: equal jitter on top of capped exponential growth.
//
//	cap    = min(Max, Base * Factor^attempt)
//	sleep  = cap/2 + rand[0, cap/2)
//
// The rand source is injectable to keep the calculation deterministic in tests.
type Backoff struct {
	Base    time.Duration
	Max     time.Duration
	Factor  float64
	attempt int
	rand    func() float64
}

// NewBackoff returns a Backoff with sane production defaults:
// 500ms base, doubling on each failure, capped at 30s.
func NewBackoff() *Backoff {
	return &Backoff{
		Base:   500 * time.Millisecond,
		Max:    30 * time.Second,
		Factor: 2.0,
		rand:   rand.Float64,
	}
}

// NewBackoffWithRand is used by tests to inject a deterministic rand in [0, 1).
func NewBackoffWithRand(base, max time.Duration, factor float64, randFn func() float64) *Backoff {
	return &Backoff{Base: base, Max: max, Factor: factor, rand: randFn}
}

// Next returns the delay to wait before the next reconnection attempt and
// increments the internal attempt counter.
func (b *Backoff) Next() time.Duration {
	cap := float64(b.Base) * math.Pow(b.Factor, float64(b.attempt))
	if cap > float64(b.Max) {
		cap = float64(b.Max)
	}
	b.attempt++

	half := cap / 2
	jitter := 0.0
	if b.rand != nil {
		jitter = b.rand() * half
	}
	return time.Duration(half + jitter)
}

// Reset zeroes the attempt counter. It must be called after a successful
// (re)connection so the next failure starts from the base delay again.
func (b *Backoff) Reset() {
	b.attempt = 0
}

// Attempt reports how many consecutive failures have been observed.
func (b *Backoff) Attempt() int {
	return b.attempt
}
