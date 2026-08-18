-- events.event_id previously had a plain index, which speeds up lookups but
-- does not stop two concurrent deliveries of the same event_id from both
-- inserting. This constraint is the durable, atomic gate that ingestion
-- idempotency relies on: when two requests race to insert the same
-- event_id, Postgres lets exactly one succeed.
ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);

-- Superseded by the unique constraint above, which already maintains its
-- own supporting index.
DROP INDEX IF EXISTS idx_events_event_id;
