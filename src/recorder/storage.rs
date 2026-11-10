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

pub(super) struct Storage {
    connection: Connection,
    pub session_id: i64,
}

impl Storage {
    pub fn open(path: &Path, busy_timeout: Duration) -> Result<Self, RecorderError> {
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
            .execute("INSERT INTO sessions(started_at_ns) VALUES (?)", [started])
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
                    "port_worker_continuity_unknown": losses.dropped_data_lost_events != 0,
                    "counts_are_cumulative": true, "gap_position_known": false}),
            )
            .map_err(|error| RecorderError::sqlite("record losses", error))?;
            transaction.execute("UPDATE sessions SET recorder_incomplete = ?, recorder_dropped_events = ?, recorder_dropped_data_lost_events = ? WHERE id = ?",
                params![losses.is_incomplete(), losses.dropped_events.to_string(), losses.dropped_data_lost_events.to_string(), self.session_id])
                .map_err(|error| RecorderError::sqlite("update losses", error))?;
        }
        transaction
            .commit()
            .map_err(|error| RecorderError::sqlite("commit batch", error))
    }

    pub fn finish(self) -> Result<(), RecorderError> {
        let ended = unix_ns(SystemTime::now()).map_err(RecorderError::Data)?;
        self.connection
            .execute(
                "UPDATE sessions SET ended_at_ns = ? WHERE id = ?",
                params![ended, self.session_id],
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
    if !fresh && (application_id != APPLICATION_ID || version != SCHEMA_VERSION) {
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
            .execute_batch(include_str!("schema.sql"))
            .map_err(|error| RecorderError::sqlite("create schema", error))?;
        transaction
            .pragma_update(None, "application_id", APPLICATION_ID)
            .map_err(|error| RecorderError::sqlite("identify schema", error))?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|error| RecorderError::sqlite("version schema", error))?;
        transaction
            .commit()
            .map_err(|error| RecorderError::sqlite("commit schema", error))?;
    }
    // Fail during construction for missing/incompatible required columns, even
    // when a damaged file still advertises our application id and version.
    for sql in [
        "SELECT id, started_at_ns, ended_at_ns, recorder_incomplete, recorder_dropped_events, recorder_dropped_data_lost_events FROM sessions LIMIT 0",
        "SELECT session_id, port_id, global_key, local_source, completed_at_ns, unit, value_type, integer_value, real_value, boolean_value, read_error FROM measurements LIMIT 0",
        "SELECT session_id, registered_at_ns, event_type, origin, port_id, details_json FROM events LIMIT 0",
    ] {
        connection
            .prepare(sql)
            .map_err(|error| RecorderError::sqlite("validate schema", error))?;
    }
    Ok(())
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
            tx.prepare_cached("INSERT INTO measurements(session_id, port_id, global_key, local_source, completed_at_ns, unit, value_type, integer_value, real_value, boolean_value, read_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?
                .execute(params![session, port_id, global_key, observation.source, time, observation.unit, kind, integer, real, boolean, error])?;
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
    tx.prepare_cached("INSERT INTO events(session_id, registered_at_ns, event_type, origin, port_id, details_json) VALUES (?, ?, ?, ?, ?, ?)")?
        .execute(params![session, time, kind, origin, port, details.to_string()])?;
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
