package ingest_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestConcurrentDuplicateDeliveryIngestsExactlyOnce is the concurrency
// counterpart to TestDuplicateDeliveryIsIgnored above. Sequential duplicate
// delivery passing does not prove the check-then-insert pattern this
// replaced is safe: two requests can both observe "event not found" before
// either has inserted. This fires many deliveries of the same event_id at
// once and checks that exactly one durable copy, one call row, and one
// stats increment result.
func TestConcurrentDuplicateDeliveryIngestsExactlyOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				t.Errorf("post: %v", err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("got %d, want 200", resp.StatusCode)
			}
		}()
	}
	close(start) // release every goroutine at once to maximise the race window
	wg.Wait()

	var eventCount int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("stored %d copies of the event under concurrent delivery, want 1", eventCount)
	}

	var callCount int
	row = st.Pool().QueryRow(ctx, `SELECT count(*) FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&callCount); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("stored %d call rows, want 1", callCount)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 {
		t.Fatalf("call_count=%d after %d concurrent duplicate deliveries, want 1 — duplicates were double-counted",
			got.CallCount, n)
	}
	if got.TotalDurationSec != 143 {
		t.Fatalf("total_duration_sec=%d, want 143 (must not be counted more than once)", got.TotalDurationSec)
	}
}

// TestRecordingIsProcessedAfterRequestCompletes exercises the real HTTP
// path end to end. Before the fix, the recording-processing goroutine
// inherited the inbound request's context. net/http cancels that context as
// soon as the handler returns, which happens long before the goroutine's
// simulated work finishes, so recording_processed never became true. This
// polls briefly because processing is asynchronous by design; a passing run
// should see the flag flip within a few tens of milliseconds.
func TestRecordingIsProcessedAfterRequestCompletes(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		var processed bool
		row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recording was never marked processed — the background job appears to have been " +
				"cancelled along with the request")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newDirectService builds a Service without going through HTTP, so tests
// can call Ingest and Wait directly instead of only observing their effects
// through a request/response cycle.
func newDirectService(t *testing.T) (*ingest.Service, *store.Store) {
	t.Helper()
	cfg := config.Load()

	s := testutil.NewStore(t)

	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return ingest.New(s, stats.NewCache(), rdb, log), s
}

// TestWaitBlocksUntilInFlightRecordingProcessingFinishes checks that Wait
// actually waits for outstanding recording-processing goroutines rather
// than returning immediately — which is what makes it safe to call during
// shutdown (see cmd/server/main.go).
func TestWaitBlocksUntilInFlightRecordingProcessingFinishes(t *testing.T) {
	svc, s := newDirectService(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  5,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now(),
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	svc.Wait(ctx)

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording to be processed once Wait returned — Wait must block until in-flight work finishes")
	}
}

// TestRecordingProcessingFailureLogsTheError forces the asynchronous
// recording step to fail, by closing its store connection out from under
// it after the synchronous part of Ingest has already committed, and
// asserts the failure is logged instead of silently discarded (the
// original code's `// TODO: handle`).
func TestRecordingProcessingFailureLogsTheError(t *testing.T) {
	cfg := config.Load()

	// A separate store connection used only for generating/cleaning up
	// test-scoped IDs and for reading back state; it stays open for the
	// whole test, unlike svcStore below.
	cleanupStore := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, cleanupStore)

	svcStore, err := store.New(context.Background(), cfg.PostgresDSN, cfg.DBMaxConns)
	if err != nil {
		t.Fatalf("connect to postgres (is `docker compose up` running?): %v", err)
	}

	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis (is `docker compose up` running?): %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	svc := ingest.New(svcStore, stats.NewCache(), rdb, log)

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  5,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now(),
	}
	if err := svc.Ingest(context.Background(), evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// The event/call/stats writes above are synchronous and already
	// committed through svcStore, so closing it now can only affect the
	// still-sleeping recording-processing goroutine, forcing its
	// MarkRecordingProcessed call to fail.
	svcStore.Close()
	svc.Wait(context.Background())

	if !strings.Contains(logs.String(), "recording processing failed") {
		t.Fatalf("expected a log line for the failed recording processing, got: %q", logs.String())
	}
	if !strings.Contains(logs.String(), callID) {
		t.Fatalf("expected the log line to include the call id %q, got: %q", callID, logs.String())
	}
}
