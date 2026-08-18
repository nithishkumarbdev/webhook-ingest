# Solution Notes

## What was broken and why

**Duplicates / inflated counts.** `Ingest` used `EventExists` followed by `InsertEvent`, so concurrent deliveries of the same `event_id` could both see "not found". The `events.event_id` column also had no unique constraint. This was fixed with migration `002`, a unique constraint, and a transaction gated by `INSERT ... ON CONFLICT (event_id) DO NOTHING`. Call and stats updates occur only when the event is newly inserted.

**Recording processing.** The recording goroutine used the HTTP request context, which can be cancelled when the handler returns. Processing errors were also not logged. This was fixed with a separate bounded `context.WithTimeout` and explicit error logging.

**In-flight work on shutdown.** Recording goroutines were not tracked, so shutdown could finish while background work was still running. A `WaitGroup` now tracks this work and shutdown waits for it. This covers graceful `SIGTERM`; recovery after a hard process kill would require durable job state and is outside this focused fix.

**Stats cache.** Cache updates were not synchronized consistently, causing a concurrent access race. The cache also started empty after restart. This was fixed by protecting updates with the mutex and seeding the cache from durable `account_stats` data at startup.

## Deduplication strategy

PostgreSQL is the durable source of truth. A unique `event_id` constraint combined with `INSERT ... ON CONFLICT DO NOTHING` provides atomic deduplication under concurrent delivery. This was preferred over a Redis `SETNX` lock because the Redis lock and PostgreSQL commit are separate operations and do not provide the same transactional guarantee.

## At 10,000 webhooks/second

Separate webhook acceptance from processing with a durable queue and horizontally scaled workers. Further scaling would require connection-pool tuning, batching, backpressure, partitioning where appropriate, retry handling, and observability.