package binance

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentiallyAndCaps(t *testing.T) {
	b := NewBackoffWithRand(100*time.Millisecond, 1*time.Second, 2.0, func() float64 { return 0 })

	// jitter=0 => sleep == cap/2
	cases := []time.Duration{
		50 * time.Millisecond,  // 100/2
		100 * time.Millisecond, // 200/2
		200 * time.Millisecond, // 400/2
		400 * time.Millisecond, // 800/2
		500 * time.Millisecond, // capped at 1000/2
		500 * time.Millisecond,
	}
	for i, want := range cases {
		if got := b.Next(); got != want {
			t.Errorf("attempt %d: got %v, want %v", i, got, want)
		}
	}
}

func TestBackoffJitterStaysWithinBounds(t *testing.T) {
	b := NewBackoffWithRand(200*time.Millisecond, 10*time.Second, 2.0, func() float64 { return 0.999999 })
	for i := 0; i < 20; i++ {
		got := b.Next()
		cap := float64(b.Base) * float64(int(1)<<uint(i))
		if cap > float64(b.Max) {
			cap = float64(b.Max)
		}
		if got < time.Duration(cap/2) || got >= time.Duration(cap) {
			t.Fatalf("attempt %d: sleep %v outside [%v, %v)", i, got, time.Duration(cap/2), time.Duration(cap))
		}
	}
}

func TestBackoffReset(t *testing.T) {
	b := NewBackoffWithRand(100*time.Millisecond, 10*time.Second, 2.0, func() float64 { return 0 })
	b.Next()
	b.Next()
	b.Next()
	if b.Attempt() != 3 {
		t.Fatalf("expected 3 attempts, got %d", b.Attempt())
	}
	b.Reset()
	if b.Attempt() != 0 {
		t.Fatalf("expected 0 attempts after reset, got %d", b.Attempt())
	}
	if got := b.Next(); got != 50*time.Millisecond {
		t.Fatalf("after reset got %v, want 50ms", got)
	}
}
