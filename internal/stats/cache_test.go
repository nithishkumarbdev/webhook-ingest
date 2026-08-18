package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestRecordIsSafeForConcurrentUse guards against the cache losing updates
// (or, with `go test -race`, panicking on a concurrent map write) when
// multiple goroutines call Record for the same account at once — the
// situation concurrent webhook deliveries put it in.
func TestRecordIsSafeForConcurrentUse(t *testing.T) {
	c := stats.NewCache()

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			c.Record("acc_shared", 1)
		}()
	}
	close(start)
	wg.Wait()

	got := c.Get("acc_shared")
	if got.CallCount != n {
		t.Fatalf("got CallCount=%d after %d concurrent Record calls, want %d — updates were lost",
			got.CallCount, n, n)
	}
	if got.TotalDurationSec != n {
		t.Fatalf("got TotalDurationSec=%d, want %d", got.TotalDurationSec, n)
	}
}

func TestSetSeedsAccountTotals(t *testing.T) {
	c := stats.NewCache()
	c.Set("acc_1", stats.AccountStats{CallCount: 7, TotalDurationSec: 210})

	got := c.Get("acc_1")
	if got.CallCount != 7 || got.TotalDurationSec != 210 {
		t.Fatalf("got %+v, want CallCount=7 TotalDurationSec=210", got)
	}
}
