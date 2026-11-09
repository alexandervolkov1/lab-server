CREATE TABLE sessions (
    id INTEGER PRIMARY KEY,
    started_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER,
    recorder_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(recorder_incomplete IN (0, 1)),
    recorder_dropped_events TEXT NOT NULL DEFAULT '0',
    recorder_dropped_data_lost_events TEXT NOT NULL DEFAULT '0'
) STRICT;
CREATE TABLE measurements (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    port_id TEXT NOT NULL,
    global_key TEXT NOT NULL,
    local_source TEXT NOT NULL,
    completed_at_ns INTEGER NOT NULL,
    unit TEXT NOT NULL,
    value_type TEXT NOT NULL CHECK(value_type IN ('integer', 'float', 'boolean', 'error')),
    integer_value INTEGER,
    real_value REAL,
    boolean_value INTEGER,
    read_error TEXT,
    CHECK (
        (value_type = 'integer' AND integer_value IS NOT NULL AND real_value IS NULL AND boolean_value IS NULL AND read_error IS NULL) OR
        (value_type = 'float' AND integer_value IS NULL AND real_value IS NOT NULL AND boolean_value IS NULL AND read_error IS NULL) OR
        (value_type = 'boolean' AND integer_value IS NULL AND real_value IS NULL AND boolean_value IS NOT NULL AND boolean_value IN (0, 1) AND read_error IS NULL) OR
        (value_type = 'error' AND integer_value IS NULL AND real_value IS NULL AND boolean_value IS NULL AND read_error IS NOT NULL)
    )
) STRICT;
CREATE INDEX measurements_key_time ON measurements(session_id, global_key, completed_at_ns);
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES sessions(id),
    registered_at_ns INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('DataLost', 'PortStopped', 'InvalidObservation', 'SourceClosedWithoutStop', 'RecorderDataLost')),
    origin TEXT NOT NULL CHECK(origin IN ('port_worker', 'aggregator', 'recorder')),
    port_id TEXT,
    details_json TEXT NOT NULL
) STRICT;
CREATE INDEX events_session ON events(session_id, id);
