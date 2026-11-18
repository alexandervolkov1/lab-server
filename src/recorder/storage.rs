use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use rusqlite::{Connection, OpenFlags, Transaction, params};
use serde_json::{Value as Json, json};

use super::{Entry, LossInfo, RecorderError, SCHEMA_VERSION, unix_ns};
use crate::driver::PortState;
use crate::model::{Observation, Value};
use crate::observation_aggregator::{AggregatorEvent, MappingError};
use crate::port_worker::StopReason;

// ASCII "LABR": reject unrelated databases rather than adopting their schema.
const APPLICATION_ID: i32 = 0x4c414252;
const SCHEMA_V1: &str = include_str!("schema.sql");
const MIGRATE_TO_V2: &str = include_str!("schema-v2.sql");
const MIGRATE_TO_V3: &str = include_str!("schema-v3.sql");

const INSERT_SESSION: &str = "INSERT INTO sessions(started_at_ns, audit_enabled, measurements_enabled, audit_incomplete) VALUES (?, ?, ?, ?)";
const INSERT_MEASUREMENT: &str = "INSERT INTO measurements(session_id, port_id, global_key, local_source, completed_at_ns, unit, value_type, integer_value, real_value, boolean_value, read_error, source_metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
const INSERT_EVENT: &str = "INSERT INTO events(session_id, registered_at_ns, event_type, origin, port_id, details_json) VALUES (?, ?, ?, ?, ?, ?)";
const INSERT_RECORD_ORDER: &str =
    "INSERT INTO record_order(session_id, kind, row_id) VALUES (?, ?, ?)";
const INSERT_AUDIT: &str = "INSERT INTO audit_events(session_id, operation_id, sequence, registered_at_ns, kind, context_json, details_json) VALUES (?, ?, ?, ?, ?, ?, ?)";
const UPDATE_AUDIT_LOSSES: &str = "UPDATE sessions SET audit_dropped_events=? WHERE id=?";
const UPDATE_RECORDER_LOSSES: &str = "UPDATE sessions SET recorder_incomplete = ?, recorder_dropped_events = ?, recorder_dropped_data_lost_events = ? WHERE id = ?";
const FINISH_SESSION: &str = "UPDATE sessions SET ended_at_ns = ?, audit_incomplete = CASE WHEN audit_enabled THEN ? ELSE 0 END WHERE id = ?";

pub(super) struct Storage {
    connection: Connection,
    pub session_id: i64,
}

impl Storage {
    pub fn open(
        path: &Path,
        busy_timeout: Duration,
        audit_enabled: bool,
        measurements_enabled: bool,
    ) -> Result<Self, RecorderError> {
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| RecorderError::sqlite("open", error))?;
        connection
            .busy_timeout(busy_timeout)
            .map_err(|error| RecorderError::sqlite("busy timeout", error))?;
        initialize(&mut connection)?;
        let started = unix_ns(SystemTime::now()).map_err(RecorderError::Data)?;
        connection
            .execute(
                INSERT_SESSION,
                params![started, audit_enabled, measurements_enabled, audit_enabled],
            )
            .map_err(|error| RecorderError::sqlite("start session", error))?;
        let session_id = connection.last_insert_rowid();
        Ok(Self {
            connection,
            session_id,
        })
    }

    pub fn write_batch(
        &mut self,
        batch: &[Entry],
        audit_batch: &[crate::audit::Event],
        losses: LossInfo,
        changed: bool,
    ) -> Result<(), RecorderError> {
        let loss_time = if changed {
            Some(unix_ns(SystemTime::now()).map_err(RecorderError::Data)?)
        } else {
            None
        };
        let transaction = self
            .connection
            .transaction()
            .map_err(|error| RecorderError::sqlite("begin batch", error))?;
        for entry in batch {
            insert_entry(&transaction, self.session_id, entry)
                .map_err(|error| RecorderError::sqlite("insert batch", error))?;
        }
        for e in audit_batch {
            transaction
                .prepare_cached(INSERT_AUDIT)
                .and_then(|mut statement| {
                    statement.execute(params![
                        self.session_id,
                        e.operation.to_string(),
                        e.sequence as i64,
                        e.time_ns,
                        e.kind,
                        e.context.as_ref(),
                        e.details
                    ])
                })
                .map_err(|error| RecorderError::sqlite("insert audit", error))?;
        }
        transaction
            .execute(
                UPDATE_AUDIT_LOSSES,
                params![losses.dropped_audit_events.to_string(), self.session_id],
            )
            .map_err(|e| RecorderError::sqlite("audit losses", e))?;
        if let Some(time) = loss_time {
            insert_event(
                &transaction,
                self.session_id,
                time,
                "RecorderDataLost",
                "recorder",
                None,
                json!({"dropped_events": losses.dropped_events.to_string(),
                    "dropped_data_lost_events": losses.dropped_data_lost_events.to_string(),
                    "rejected_invalid_data": losses.rejected_invalid_data.to_string(),
                    "dropped_audit_events": losses.dropped_audit_events.to_string(),
                    "port_worker_continuity_unknown": losses.dropped_data_lost_events != 0,
                    "counts_are_cumulative": true, "gap_position_known": false}),
            )
            .map_err(|error| RecorderError::sqlite("record losses", error))?;
            transaction
                .execute(
                    UPDATE_RECORDER_LOSSES,
                    params![
                        losses.is_incomplete(),
                        losses.dropped_events.to_string(),
                        losses.dropped_data_lost_events.to_string(),
                        self.session_id
                    ],
                )
                .map_err(|error| RecorderError::sqlite("update losses", error))?;
        }
        transaction
            .commit()
            .map_err(|error| RecorderError::sqlite("commit batch", error))
    }

    pub fn finish(self, audit_complete: bool) -> Result<(), RecorderError> {
        let ended = unix_ns(SystemTime::now()).map_err(RecorderError::Data)?;
        self.connection
            .execute(
                FINISH_SESSION,
                params![ended, !audit_complete, self.session_id],
            )
            .map_err(|error| RecorderError::sqlite("finish session", error))?;
        self.connection
            .close()
            .map_err(|(_, error)| RecorderError::sqlite("close", error))
    }
}

fn initialize(connection: &mut Connection) -> Result<(), RecorderError> {
    let inspect = || -> rusqlite::Result<(i32, i32, bool)> {
        Ok((
            connection.pragma_query_value(None, "application_id", |row| row.get(0))?,
            connection.pragma_query_value(None, "user_version", |row| row.get(0))?,
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%')",
                [],
                |row| row.get(0),
            )?,
        ))
    };
    let (application_id, version, has_objects) =
        inspect().map_err(|error| RecorderError::sqlite("inspect schema", error))?;
    let fresh = application_id == 0 && version == 0 && !has_objects;
    if !fresh && (application_id != APPLICATION_ID || !(1..=SCHEMA_VERSION).contains(&version)) {
        return Err(RecorderError::UnsupportedSchema {
            application_id,
            version,
        });
    }
    connection
        .execute_batch("PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL;")
        .map_err(|error| RecorderError::sqlite("configure", error))?;
    let mode: String = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
        .map_err(|error| RecorderError::sqlite("enable WAL", error))?;
    if mode != "wal" {
        return Err(RecorderError::InvalidOptions(
            "recorder requires a file database supporting WAL",
        ));
    }
    if fresh {
        let transaction = connection
            .transaction()
            .map_err(|error| RecorderError::sqlite("begin schema", error))?;
        transaction
            .execute_batch(SCHEMA_V1)
            .map_err(|error| RecorderError::sqlite("create schema", error))?;
        transaction
            .execute_batch(MIGRATE_TO_V2)
            .map_err(|error| RecorderError::sqlite("create schema v2", error))?;
        transaction
            .pragma_update(None, "application_id", APPLICATION_ID)
            .map_err(|error| RecorderError::sqlite("identify schema", error))?;
        transaction
            .pragma_update(None, "user_version", 2)
            .map_err(|error| RecorderError::sqlite("version schema", error))?;
        transaction
            .commit()
            .map_err(|error| RecorderError::sqlite("commit schema", error))?;
    }
    if !fresh && version == 1 {
        let tx = connection
            .transaction()
            .map_err(|e| RecorderError::sqlite("begin migration", e))?;
        tx.execute_batch(MIGRATE_TO_V2)
            .map_err(|e| RecorderError::sqlite("migrate v1 to v2", e))?;
        tx.pragma_update(None, "user_version", 2)
            .map_err(|e| RecorderError::sqlite("version migration", e))?;
        tx.commit()
            .map_err(|e| RecorderError::sqlite("commit migration", e))?;
    }
    if fresh || version < 3 {
        let tx = connection
            .transaction()
            .map_err(|e| RecorderError::sqlite("begin v3", e))?;
        tx.execute_batch(MIGRATE_TO_V3)
            .map_err(|e| RecorderError::sqlite("migrate v2 to v3", e))?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|e| RecorderError::sqlite("version v3", e))?;
        tx.commit()
            .map_err(|e| RecorderError::sqlite("commit v3", e))?;
    }
    validate_schema(connection)
}

fn validate_schema(connection: &Connection) -> Result<(), RecorderError> {
    // Derive the required columns from the migrations themselves so the
    // validator and schema cannot drift independently.
    let reference = Connection::open_in_memory().map_err(schema_sqlite_error)?;
    for sql in [SCHEMA_V1, MIGRATE_TO_V2, MIGRATE_TO_V3] {
        reference.execute_batch(sql).map_err(schema_sqlite_error)?;
    }
    for table in [
        "sessions",
        "measurements",
        "events",
        "record_order",
        "audit_events",
    ] {
        validate_table(connection, &reference, table)?;
    }
    let unique_audit_identity: bool = connection
        .query_row(
            r#"SELECT EXISTS (
            SELECT 1 FROM pragma_index_list('audit_events', 'main') AS idx
            WHERE idx."unique" = 1 AND idx.partial = 0
              AND (SELECT count(*) FROM pragma_index_info(idx.name, 'main')) = 3
              AND (SELECT count(DISTINCT lower(name)) FROM pragma_index_info(idx.name, 'main')
                   WHERE lower(name) IN ('session_id', 'operation_id', 'sequence')) = 3
        )"#,
            [],
            |row| row.get(0),
        )
        .map_err(schema_sqlite_error)?;
    if !unique_audit_identity {
        return Err(schema_error("audit_events requires a non-partial UNIQUE key on (session_id, operation_id, sequence)".into()));
    }
    // Compile the writer's actual statements to also catch e.g. foreign key
    // mismatches. No writes or trigger execution during validation.
    for sql in [
        INSERT_SESSION,
        INSERT_MEASUREMENT,
        INSERT_EVENT,
        INSERT_RECORD_ORDER,
        INSERT_AUDIT,
        UPDATE_AUDIT_LOSSES,
        UPDATE_RECORDER_LOSSES,
        FINISH_SESSION,
    ] {
        connection.prepare(sql).map_err(schema_sqlite_error)?;
    }
    Ok(())
}

fn validate_table(
    connection: &Connection,
    reference: &Connection,
    table: &str,
) -> Result<(), RecorderError> {
    let rowid_table: bool = connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM pragma_table_list
          WHERE schema='main' AND name=? COLLATE NOCASE AND type='table' AND strict=1 AND wr=0)",
            [table],
            |row| row.get(0),
        )
        .map_err(schema_sqlite_error)?;
    if !rowid_table {
        return Err(schema_error(format!(
            "{table}: expected a STRICT table with rowid"
        )));
    }
    // An indexed PRIMARY KEY (including INTEGER PRIMARY KEY DESC) does not
    // alias rowid. Recorder uses last_insert_rowid() as the generated id.
    let indexed_primary_key: bool = connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM pragma_index_list(?, 'main') WHERE origin='pk')",
            [table],
            |row| row.get(0),
        )
        .map_err(schema_sqlite_error)?;
    if indexed_primary_key {
        return Err(schema_error(format!(
            "{table}.id: primary key must alias rowid"
        )));
    }
    let mut actual = table_columns(connection, table).map_err(schema_sqlite_error)?;
    for (name, expected) in table_columns(reference, table).map_err(schema_sqlite_error)? {
        let Some(column) = actual.remove(&name) else {
            return Err(schema_error(format!(
                "missing required column {table}.{name}"
            )));
        };
        if !column.data_type.eq_ignore_ascii_case(&expected.data_type)
            || column.primary_key != expected.primary_key
            || (expected.primary_key == 0 && column.not_null != expected.not_null)
            || column.hidden != 0
            || (expected.default.is_some() && !column.has_compatible_default())
        {
            return Err(schema_error(format!(
                "incompatible column {table}.{name}: expected {expected:?}, found {column:?}"
            )));
        }
    }
    for (name, column) in actual {
        if column.hidden != 0 {
            return Err(schema_error(format!(
                "extra generated column {table}.{name} is not supported"
            )));
        }
        if !column.has_compatible_default() {
            let required = if column.not_null { "required " } else { "" };
            return Err(schema_error(format!(
                "extra {required}column {table}.{name} requires a compatible literal DEFAULT"
            )));
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Column {
    data_type: String,
    not_null: bool,
    default: Option<String>,
    primary_key: u32,
    hidden: u32,
}

impl Column {
    fn has_compatible_default(&self) -> bool {
        let Some(default) = self.default.as_deref() else {
            return !self.not_null;
        };
        let mut literal = default.trim();
        while let Some(inner) = literal.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
            literal = inner.trim();
        }
        if literal.eq_ignore_ascii_case("NULL") {
            return !self.not_null;
        }

        // Classify literals without evaluating SQL from the existing database.
        let literal_type = if is_quoted_literal(literal) {
            "TEXT"
        } else if literal.parse::<i64>().is_ok() {
            "INTEGER"
        } else if literal.parse::<f64>().is_ok_and(f64::is_finite) {
            "REAL"
        } else if literal
            .strip_prefix("X'")
            .or_else(|| literal.strip_prefix("x'"))
            .and_then(|hex| hex.strip_suffix('\''))
            .is_some_and(|hex| {
                hex.len().is_multiple_of(2) && hex.bytes().all(|b| b.is_ascii_hexdigit())
            })
        {
            "BLOB"
        } else {
            return false;
        };
        let column_type = self.data_type.to_ascii_uppercase();
        column_type == literal_type
            || matches!(
                (column_type.as_str(), literal_type),
                ("ANY", _) | ("INT" | "REAL" | "TEXT", "INTEGER") | ("TEXT", "REAL")
            )
    }
}

fn is_quoted_literal(value: &str) -> bool {
    ['\'', '"'].into_iter().any(|quote| {
        let Some(mut text) = value
            .strip_prefix(quote)
            .and_then(|s| s.strip_suffix(quote))
        else {
            return false;
        };
        while let Some((_, after_quote)) = text.split_once(quote) {
            let Some(after_escape) = after_quote.strip_prefix(quote) else {
                return false;
            };
            text = after_escape;
        }
        true
    })
}

fn table_columns(
    connection: &Connection,
    table: &str,
) -> rusqlite::Result<BTreeMap<String, Column>> {
    connection.prepare(r#"SELECT name, type, "notnull", dflt_value, pk, hidden FROM pragma_table_xinfo(?, 'main')"#)?
        .query_map([table], |row| Ok((
            row.get::<_, String>(0)?.to_ascii_lowercase(),
            Column { data_type: row.get(1)?, not_null: row.get(2)?, default: row.get(3)?, primary_key: row.get(4)?, hidden: row.get(5)? },
        )))?.collect()
}

fn schema_error(message: String) -> RecorderError {
    RecorderError::Sqlite {
        operation: "validate schema",
        extended_code: None,
        message,
    }
}

fn schema_sqlite_error(error: rusqlite::Error) -> RecorderError {
    RecorderError::sqlite("validate schema", error)
}

fn insert_entry(tx: &Transaction<'_>, session: i64, entry: &Entry) -> rusqlite::Result<()> {
    let (kind, origin, port, details) = match &entry.event {
        AggregatorEvent::Measurement {
            port_id,
            global_key,
            observation,
        } => {
            let (kind, integer, real, boolean, error) = match &observation.reading {
                Ok(Value::Integer(value)) => ("integer", Some(*value), None, None, None),
                Ok(Value::Float(value)) => ("float", None, Some(*value), None, None),
                Ok(Value::Boolean(value)) => ("boolean", None, None, Some(*value), None),
                Err(error) => ("error", None, None, None, Some(error.as_str())),
            };
            // Admission checked this before accepting the immutable event.
            let time = unix_ns(observation.completed_at).expect("validated timestamp");
            tx.prepare_cached(INSERT_MEASUREMENT)?
                .execute(params![session, port_id, global_key, observation.source, time, observation.unit, kind, integer, real, boolean, error, observation.metadata.as_ref().map(|m| json!({"source_instance":m.source_instance.to_string(), "sequence":m.sequence.to_string(), "generation":m.generation.to_string(), "elapsed_ns":m.elapsed.as_nanos().to_string()}).to_string())])?;
            tx.execute(
                INSERT_RECORD_ORDER,
                params![session, "measurement", tx.last_insert_rowid()],
            )?;
            return Ok(());
        }
        AggregatorEvent::DataLost {
            port_id,
            loss_epoch,
        } => (
            "DataLost",
            "port_worker",
            port_id,
            json!({"loss_epoch": loss_epoch.to_string(), "gap_position_known": false}),
        ),
        AggregatorEvent::PortStopped { port_id, reason } => {
            ("PortStopped", "port_worker", port_id, stop_details(reason))
        }
        AggregatorEvent::SourceClosedWithoutStop { port_id } => {
            ("SourceClosedWithoutStop", "aggregator", port_id, json!({}))
        }
        AggregatorEvent::InvalidObservation {
            port_id,
            observation,
            error,
        } => {
            let mapping = match error {
                MappingError::UnknownDevice => json!({"kind": "UnknownDevice"}),
                MappingError::UnknownParameter => json!({"kind": "UnknownParameter"}),
                MappingError::WrongPort { registered_port_id } => {
                    json!({"kind": "WrongPort", "registered_port_id": registered_port_id})
                }
            };
            (
                "InvalidObservation",
                "aggregator",
                port_id,
                json!({"device_id": observation.device_id,
                "observation": observation_details(&observation.observation), "mapping_error": mapping}),
            )
        }
    };
    insert_event(
        tx,
        session,
        entry.registered_at_ns,
        kind,
        origin,
        Some(port),
        details,
    )
}

fn insert_event(
    tx: &Transaction<'_>,
    session: i64,
    time: i64,
    kind: &str,
    origin: &str,
    port: Option<&str>,
    details: Json,
) -> rusqlite::Result<()> {
    tx.prepare_cached(INSERT_EVENT)?.execute(params![
        session,
        time,
        kind,
        origin,
        port,
        details.to_string()
    ])?;
    tx.execute(
        INSERT_RECORD_ORDER,
        params![session, "event", tx.last_insert_rowid()],
    )?;
    Ok(())
}

fn stop_details(reason: &StopReason) -> Json {
    match reason {
        StopReason::Shutdown => json!({"reason": "Shutdown"}),
        StopReason::NoCommandSenders => json!({"reason": "NoCommandSenders"}),
        StopReason::ObservationReceiverDropped => json!({"reason": "ObservationReceiverDropped"}),
        StopReason::ScheduleOverflow => json!({"reason": "ScheduleOverflow"}),
        StopReason::Driver(error) => json!({"reason": "Driver", "message": error.message,
        "port_state": match error.port_state {
            PortState::Synchronized => "Synchronized",
            PortState::Desynchronized => "Desynchronized",
            PortState::Disconnected => "Disconnected",
        }}),
    }
}

// Invalid mappings retain all diagnostic payload, even timestamps outside the
// measurement column's range or nonfinite floats. No second measurement model.
fn observation_details(observation: &Observation) -> Json {
    let (before_epoch, elapsed) = match observation
        .completed_at
        .duration_since(SystemTime::UNIX_EPOCH)
    {
        Ok(value) => (false, value),
        Err(error) => (true, error.duration()),
    };
    let reading = match &observation.reading {
        Ok(Value::Integer(value)) => json!({"type": "integer", "value": value.to_string()}),
        Ok(Value::Float(value)) => {
            json!({"type": "float", "bits_hex": format!("{:016x}", value.to_bits())})
        }
        Ok(Value::Boolean(value)) => json!({"type": "boolean", "value": value}),
        Err(error) => json!({"type": "error", "error": error}),
    };
    json!({"source": observation.source, "unit": observation.unit, "reading": reading,
        "completed_at": {"before_epoch": before_epoch, "seconds": elapsed.as_secs().to_string(), "nanoseconds": elapsed.subsec_nanos()}})
}
