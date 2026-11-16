-- Explicit v1 -> v2 migration. Historical rows retain NULL metadata; no time is invented.
ALTER TABLE measurements ADD COLUMN source_metadata TEXT;
CREATE TABLE record_order (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    kind TEXT NOT NULL CHECK(kind IN ('measurement', 'event')),
    row_id INTEGER NOT NULL
) STRICT;
