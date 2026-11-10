use super::*;
use crate::driver::{DriverError, PortState};
use crate::observation_aggregator::MappingError;
use crate::port_worker::{DeviceObservation, StopReason};
use rusqlite::{Connection, params};
use serde_json::Value as Json;
use std::path::PathBuf;
use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};

const WAIT: Duration = Duration::from_secs(10);
static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        // Cargo tests inherit the explicitly configured single build directory.
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        assert!(root.is_dir());
        let name = format!(
            "recorder-test-{}-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        );
        let path = root.join(name);
        assert!(!path.exists());
        Self(path)
    }
    fn connect(&self) -> Connection {
        Connection::open(&self.0).unwrap()
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        // Only these unique files owned by this test; no recursive deletion.
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.0.as_os_str().to_owned();
            path.push(suffix);
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("cannot remove owned recorder test file: {error}"),
            }
        }
    }
}

fn observation(value: i64) -> Observation {
    Observation {
        source: "value".into(),
        unit: "unit".into(),
        completed_at: SystemTime::UNIX_EPOCH + Duration::from_nanos(123_456_700),
        reading: Ok(Value::Integer(value)),
    }
}
fn event(value: i64) -> AggregatorEvent {
    AggregatorEvent::Measurement {
        port_id: "bus".into(),
        global_key: "device/value".into(),
        observation: observation(value),
    }
}
fn scalar(connection: &Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}
fn options(capacity: usize, batch: usize) -> RecorderOptions {
    RecorderOptions {
        queue_capacity: capacity,
        batch_size: batch,
        ..RecorderOptions::default()
    }
}

type Gate = (
    Hooks,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
    mpsc::Receiver<()>,
);
fn first_flush_gate() -> Gate {
    let (entered, wait_entered) = mpsc::channel();
    let (release, wait_release) = mpsc::channel();
    let (flushed, wait_flushed) = mpsc::channel();
    let mut first = true;
    let hooks = Hooks {
        before_flush: Some(Box::new(move || {
            if first {
                first = false;
                entered.send(()).unwrap();
                wait_release.recv_timeout(WAIT).unwrap();
            }
        })),
        after_flush: Some(Box::new(move || {
            let _ = flushed.send(());
        })),
        ..Hooks::default()
    };
    (hooks, wait_entered, release, wait_flushed)
}

#[test]
fn creates_versioned_schema_wal_and_completed_session() {
    let db = Database::new();
    let before = unix_ns(SystemTime::now()).unwrap();
    let recorder = Recorder::open(&db.0, 8).unwrap();
    let handle = recorder.handle();
    assert_eq!(handle.state(), RecorderState::Running);
    let connection = db.connect();
    assert_eq!(
        scalar(&connection, "PRAGMA user_version"),
        i64::from(SCHEMA_VERSION)
    );
    assert_eq!(
        connection
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
    drop(connection);
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.accepted_records, 0);
    assert_eq!(summary.session_id, handle.session_id());
    assert_eq!(handle.state(), RecorderState::Stopped);
    assert_eq!(handle.try_record(event(1)), Err(SubmitError::Closed));
    let connection = db.connect();
    let (start, end): (i64, i64) = connection
        .query_row(
            "SELECT started_at_ns, ended_at_ns FROM sessions",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(start >= before && end >= start);
    assert_eq!(
        connection
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn reopening_appends_independent_sessions_and_never_duplicates() {
    let db = Database::new();
    let mut ids = Vec::new();
    for session in 0..3 {
        let recorder = Recorder::open(&db.0, 32).unwrap();
        for number in 0..17 {
            recorder
                .handle()
                .try_record(event(session * 100 + number))
                .unwrap();
        }
        let summary = recorder.shutdown().unwrap();
        ids.push(summary.session_id);
        assert_eq!(summary.accepted_records, 17);
    }
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    let connection = db.connect();
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        3
    );
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 51);
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(DISTINCT integer_value) FROM measurements"
        ),
        51
    );
    for id in ids {
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM measurements WHERE session_id = ?",
                    [id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            17
        );
    }
}

#[test]
fn preserves_ports_devices_types_i64_extremes_units_time_and_read_errors() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 32).unwrap();
    let values = [
        Ok(Value::Integer(i64::MIN)),
        Ok(Value::Integer(i64::MAX)),
        Ok(Value::Integer(42)),
        Ok(Value::Float(1.25)),
        Ok(Value::Boolean(false)),
        Ok(Value::Boolean(true)),
        Err("read timeout: sensor α".into()),
    ];
    let times = [
        i64::MIN + 8,
        i64::MAX - 7,
        -100,
        0,
        123_456_700,
        2_000_000_000,
        3_000_000_000,
    ];
    for (index, reading) in values.iter().enumerate() {
        let mut obs = observation(0);
        obs.reading = reading.clone();
        // Windows SystemTime has 100ns precision. The extremes themselves are
        // tested separately by platform-independent checked conversion below.
        let nanos = times[index] / 100 * 100;
        obs.completed_at = if nanos < 0 {
            SystemTime::UNIX_EPOCH - Duration::from_nanos(nanos.unsigned_abs())
        } else {
            SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos as u64)
        };
        obs.unit = "°C / unit".into();
        let port = if index < 4 { "shared" } else { "other" };
        recorder
            .handle()
            .try_observation(port, format!("device{index}/value"), obs)
            .unwrap();
    }
    recorder.shutdown().unwrap();
    let connection = db.connect();
    let mut stmt = connection.prepare("SELECT port_id, global_key, local_source, completed_at_ns, unit, value_type, integer_value, real_value, boolean_value, read_error FROM measurements ORDER BY id").unwrap();
    let mut rows = stmt.query([]).unwrap();
    for (index, value) in values.iter().enumerate() {
        let row = rows.next().unwrap().unwrap();
        assert_eq!(
            row.get::<_, String>(0).unwrap(),
            if index < 4 { "shared" } else { "other" }
        );
        assert_eq!(
            row.get::<_, String>(1).unwrap(),
            format!("device{index}/value")
        );
        assert_eq!(row.get::<_, String>(2).unwrap(), "value");
        assert_eq!(row.get::<_, i64>(3).unwrap(), times[index] / 100 * 100);
        assert_eq!(row.get::<_, String>(4).unwrap(), "°C / unit");
        let kind: String = row.get(5).unwrap();
        match value {
            Ok(Value::Integer(value)) => {
                assert_eq!(kind, "integer");
                assert_eq!(row.get::<_, i64>(6).unwrap(), *value);
            }
            Ok(Value::Float(value)) => {
                assert_eq!(kind, "float");
                assert_eq!(row.get::<_, f64>(7).unwrap(), *value);
            }
            Ok(Value::Boolean(value)) => {
                assert_eq!(kind, "boolean");
                assert_eq!(row.get::<_, bool>(8).unwrap(), *value);
            }
            Err(error) => {
                assert_eq!(kind, "error");
                assert_eq!(row.get::<_, String>(9).unwrap(), *error);
            }
        }
    }
    assert!(rows.next().unwrap().is_none());
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM measurements WHERE value_type = 'integer' AND typeof(integer_value) = 'integer'"
        ),
        3
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM measurements WHERE value_type = 'float' AND typeof(real_value) = 'real'"
        ),
        1
    );
}

#[test]
fn stores_all_event_kinds_stop_reasons_and_full_invalid_diagnostics() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 32).unwrap();
    let handle = recorder.handle();
    let before = unix_ns(SystemTime::now()).unwrap();
    handle
        .try_record(AggregatorEvent::DataLost {
            port_id: "bus".into(),
            loss_epoch: u64::MAX,
        })
        .unwrap();
    for reason in [
        StopReason::Shutdown,
        StopReason::NoCommandSenders,
        StopReason::ObservationReceiverDropped,
        StopReason::ScheduleOverflow,
        StopReason::Driver(DriverError::new("cable unplugged", PortState::Disconnected)),
        StopReason::Driver(DriverError::new("framing", PortState::Desynchronized)),
        StopReason::Driver(DriverError::new("read rejected", PortState::Synchronized)),
    ] {
        handle
            .try_record(AggregatorEvent::PortStopped {
                port_id: "bus".into(),
                reason,
            })
            .unwrap();
    }
    for error in [
        MappingError::UnknownDevice,
        MappingError::UnknownParameter,
        MappingError::WrongPort {
            registered_port_id: "expected".into(),
        },
    ] {
        handle
            .try_record(AggregatorEvent::InvalidObservation {
                port_id: "wrong".into(),
                observation: DeviceObservation {
                    device_id: "gauge".into(),
                    observation: observation(i64::MAX),
                },
                error,
            })
            .unwrap();
    }
    handle
        .try_record(AggregatorEvent::SourceClosedWithoutStop {
            port_id: "bus".into(),
        })
        .unwrap();
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.accepted_records, 12);
    let connection = db.connect();
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 0);
    assert_eq!(scalar(&connection, "SELECT count(*) FROM events"), 12);
    let lost: String = connection
        .query_row(
            "SELECT details_json FROM events WHERE event_type='DataLost' AND origin='port_worker'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let lost: Json = serde_json::from_str(&lost).unwrap();
    assert_eq!(lost["loss_epoch"], u64::MAX.to_string());
    assert_eq!(lost["gap_position_known"], false);
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM events WHERE event_type='PortStopped'"
        ),
        7
    );
    let invalid: String = connection.query_row("SELECT details_json FROM events WHERE event_type='InvalidObservation' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    let invalid: Json = serde_json::from_str(&invalid).unwrap();
    assert_eq!(invalid["device_id"], "gauge");
    assert_eq!(invalid["mapping_error"]["registered_port_id"], "expected");
    assert_eq!(
        invalid["observation"]["reading"]["value"],
        i64::MAX.to_string()
    );
    assert_eq!(
        invalid["observation"]["completed_at"]["nanoseconds"],
        123_456_700
    );
    assert!(scalar(&connection, "SELECT min(registered_at_ns) FROM events") >= before);
}

#[test]
fn rejects_unrepresentable_time_and_nonfinite_values_before_acceptance() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 8).unwrap();
    let handle = recorder.handle();
    for time in [
        SystemTime::UNIX_EPOCH + Duration::from_secs(10_000_000_000),
        SystemTime::UNIX_EPOCH - Duration::from_secs(10_000_000_000),
    ] {
        let mut obs = observation(1);
        obs.completed_at = time;
        assert_eq!(
            handle.try_observation("bus", "d/v", obs),
            Err(SubmitError::InvalidData(DataError::TimestampOutOfRange))
        );
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut obs = observation(1);
        obs.reading = Ok(Value::Float(value));
        assert_eq!(
            handle.try_observation("bus", "d/v", obs),
            Err(SubmitError::InvalidData(DataError::NonFiniteFloat))
        );
    }
    assert!(handle.losses().is_incomplete());
    assert_eq!(handle.losses().rejected_invalid_data, 5);
    handle.try_record(event(i64::MAX)).unwrap();
    assert_eq!(recorder.shutdown().unwrap().accepted_records, 1);
    let connection = db.connect();
    assert_eq!(
        scalar(&connection, "SELECT recorder_incomplete FROM sessions"),
        1
    );
    assert_eq!(
        scalar(&connection, "SELECT integer_value FROM measurements"),
        i64::MAX
    );
    let details: String = connection.query_row("SELECT details_json FROM events WHERE event_type='RecorderDataLost' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    let details: Json = serde_json::from_str(&details).unwrap();
    assert_eq!(details["rejected_invalid_data"], "5");
    assert_eq!(details["dropped_events"], "0");
}

#[test]
fn concurrent_overflow_never_waits_for_writer_and_lost_datalost_remains_visible() {
    let db = Database::new();
    let (hooks, entered, release, flushed) = first_flush_gate();
    let recorder = Recorder::start(&db.0, options(2, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(0)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    handle.try_record(event(1)).unwrap();
    handle.try_record(event(2)).unwrap();
    let producer_handle = handle.clone();
    let (finished, wait_finished) = mpsc::channel();
    let producer = thread::spawn(move || {
        for _ in 0..1000 {
            assert_eq!(producer_handle.try_record(event(3)), Err(SubmitError::Full));
        }
        assert_eq!(
            producer_handle.try_record(AggregatorEvent::DataLost {
                port_id: "bus".into(),
                loss_epoch: 27
            }),
            Err(SubmitError::Full)
        );
        finished.send(()).unwrap();
    });
    // Writer cannot proceed until we release it, so completion proves admission
    // does not wait for queue capacity or SQLite progress (not a timing guess).
    let completed = wait_finished.recv_timeout(WAIT);
    let losses = handle.losses();
    release.send(()).unwrap();
    completed.unwrap();
    producer.join().unwrap();
    flushed.recv_timeout(WAIT).unwrap();
    let summary = recorder.shutdown().unwrap();
    assert_eq!(
        losses,
        LossInfo {
            dropped_events: 1001,
            dropped_data_lost_events: 1,
            rejected_invalid_data: 0,
        }
    );
    assert_eq!(handle.losses(), losses);
    assert_eq!(summary.losses, losses);
    assert_eq!(summary.accepted_records, 3);
    let connection = db.connect();
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 3);
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(DISTINCT integer_value) FROM measurements"
        ),
        3
    );
    assert_eq!(
        scalar(&connection, "SELECT recorder_incomplete FROM sessions"),
        1
    );
    let counts: (String, String) = connection
        .query_row(
            "SELECT recorder_dropped_events, recorder_dropped_data_lost_events FROM sessions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, ("1001".into(), "1".into()));
    let json: String = connection.query_row("SELECT details_json FROM events WHERE event_type='RecorderDataLost' AND origin='recorder' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    let json: Json = serde_json::from_str(&json).unwrap();
    assert_eq!(json["port_worker_continuity_unknown"], true);
    assert_eq!(json["dropped_data_lost_events"], "1");
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM events WHERE event_type='DataLost'"
        ),
        0
    );
}

#[test]
fn saturated_loss_is_sticky_and_persisted_as_exact_decimal_text() {
    let db = Database::new();
    let (hooks, entered, release, _) = first_flush_gate();
    let recorder = Recorder::start(&db.0, options(1, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(0)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    handle.try_record(event(1)).unwrap();
    control(&handle.shared).losses = LossInfo {
        dropped_events: u64::MAX - 1,
        dropped_data_lost_events: u64::MAX - 1,
        rejected_invalid_data: 0,
    };
    for _ in 0..3 {
        assert_eq!(
            handle.try_record(AggregatorEvent::DataLost {
                port_id: "bus".into(),
                loss_epoch: u64::MAX
            }),
            Err(SubmitError::Full)
        );
    }
    release.send(()).unwrap();
    recorder.shutdown().unwrap();
    assert_eq!(
        handle.losses(),
        LossInfo {
            dropped_events: u64::MAX,
            dropped_data_lost_events: u64::MAX,
            rejected_invalid_data: 0,
        }
    );
    assert!(handle.losses().is_incomplete());
    let connection = db.connect();
    assert_eq!(
        connection
            .query_row("SELECT recorder_dropped_events FROM sessions", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
        u64::MAX.to_string()
    );
    assert_eq!(
        scalar(&connection, "SELECT recorder_incomplete FROM sessions"),
        1
    );
}

#[test]
fn low_rate_flushes_before_shutdown_and_shutdown_drains_partial_batch() {
    let db = Database::new();
    let (flushed, wait_flushed) = mpsc::channel();
    let hooks = Hooks {
        before_flush: None,
        after_flush: Some(Box::new(move || {
            let _ = flushed.send(());
        })),
        ..Hooks::default()
    };
    let recorder = Recorder::start(&db.0, options(32, 128), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    wait_flushed.recv_timeout(WAIT).unwrap();
    assert_eq!(
        scalar(&db.connect(), "SELECT count(*) FROM measurements"),
        1
    );
    for value in 2..18 {
        handle.try_record(event(value)).unwrap();
    }
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.accepted_records, 17);
    assert_eq!(
        scalar(&db.connect(), "SELECT count(*) FROM measurements"),
        17
    );
    assert_eq!(
        scalar(
            &db.connect(),
            "SELECT count(DISTINCT integer_value) FROM measurements"
        ),
        17
    );
}

#[test]
fn open_errors_and_unknown_or_unrelated_schema_are_returned_without_adoption() {
    let db = Database::new();
    assert!(matches!(
        Recorder::open(db.0.join("missing.sqlite"), 8),
        Err(RecorderError::Sqlite {
            operation: "open",
            ..
        })
    ));
    let connection = db.connect();
    connection
        .execute_batch("CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES ('keep');")
        .unwrap();
    assert!(matches!(
        Recorder::open(&db.0, 8),
        Err(RecorderError::UnsupportedSchema { .. })
    ));
    assert_eq!(scalar(&connection, "SELECT count(*) FROM unrelated"), 1);
    assert_eq!(scalar(&connection, "PRAGMA user_version"), 0);
    drop(connection);
    let other = Database::new();
    Recorder::open(&other.0, 8).unwrap().shutdown().unwrap();
    let connection = other.connect();
    connection.pragma_update(None, "user_version", 99).unwrap();
    assert!(matches!(
        Recorder::open(&other.0, 8),
        Err(RecorderError::UnsupportedSchema { version: 99, .. })
    ));
    assert_eq!(scalar(&connection, "SELECT count(*) FROM sessions"), 1);
}

#[test]
fn invalid_options_fail_before_creating_database() {
    let db = Database::new();
    for settings in [
        options(0, 1),
        options(1, 0),
        RecorderOptions {
            flush_interval: Duration::ZERO,
            ..options(1, 1)
        },
        RecorderOptions {
            busy_timeout: Duration::from_secs(u64::MAX),
            ..options(1, 1)
        },
    ] {
        assert!(matches!(
            Recorder::with_options(&db.0, settings),
            Err(RecorderError::InvalidOptions(_))
        ));
        assert!(!db.0.exists());
    }
}

#[test]
fn write_failure_rolls_back_entire_batch_rejects_new_data_and_keeps_session_unfinished() {
    let db = Database::new();
    let (hooks, entered, release, _) = first_flush_gate();
    let recorder = Recorder::start(&db.0, options(8, 2), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    handle.try_record(event(2)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let connection = db.connect();
    connection.execute_batch("CREATE TRIGGER fail_second BEFORE INSERT ON measurements WHEN NEW.integer_value = 2 BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;").unwrap();
    release.send(()).unwrap();
    let error = recorder.shutdown().unwrap_err();
    assert!(
        matches!(&error, RecorderError::Sqlite { operation: "insert batch", message, .. } if message.contains("injected write failure"))
    );
    assert_eq!(handle.state(), RecorderState::Failed(error.clone()));
    assert_eq!(handle.try_record(event(3)), Err(SubmitError::Failed(error)));
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 0);
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
    connection
        .execute_batch("DROP TRIGGER fail_second")
        .unwrap();
    drop(connection);
    Recorder::open(&db.0, 4).unwrap().shutdown().unwrap();
    let connection = db.connect();
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        1
    );
}

#[test]
fn sqlite_lock_failure_is_finite_and_does_not_retry() {
    let db = Database::new();
    let (hooks, entered, release, _) = first_flush_gate();
    let recorder = Recorder::start(
        &db.0,
        RecorderOptions {
            busy_timeout: Duration::ZERO,
            ..options(4, 1)
        },
        hooks,
    )
    .unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let connection = db.connect();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    release.send(()).unwrap();
    let error = recorder.shutdown().unwrap_err();
    assert!(matches!(
        error,
        RecorderError::Sqlite {
            extended_code: Some(5),
            ..
        }
    ));
    connection.execute_batch("ROLLBACK").unwrap();
    assert!(matches!(
        handle.try_record(event(2)),
        Err(SubmitError::Failed(_))
    ));
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 0);
}

#[test]
fn shutdown_reports_finalization_failure_instead_of_success() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 8).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    let connection = db.connect();
    connection.execute_batch("CREATE TRIGGER fail_finish BEFORE UPDATE OF ended_at_ns ON sessions BEGIN SELECT RAISE(ABORT, 'finish failed'); END;").unwrap();
    let error = recorder.shutdown().unwrap_err();
    assert!(matches!(
        &error,
        RecorderError::Sqlite {
            operation: "finish session",
            ..
        }
    ));
    assert_eq!(handle.state(), RecorderState::Failed(error));
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 1);
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
}

#[test]
fn concurrent_producers_preserve_every_accepted_record_once() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 1024).unwrap();
    let gate = Arc::new(Barrier::new(5));
    let mut threads = Vec::new();
    for producer in 0..4 {
        let handle = recorder.handle();
        let gate = gate.clone();
        threads.push(thread::spawn(move || {
            gate.wait();
            for value in 0..200 {
                handle.try_record(event(producer * 1000 + value)).unwrap();
            }
        }));
    }
    gate.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.accepted_records, 800);
    assert!(!summary.losses.is_incomplete());
    let connection = db.connect();
    assert_eq!(
        scalar(&connection, "SELECT count(*) FROM measurements"),
        800
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(DISTINCT integer_value) FROM measurements"
        ),
        800
    );
}

#[test]
fn concurrent_shutdown_linearizes_admission_and_drains_accepted_records() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 1024).unwrap();
    let handle = recorder.handle();
    let gate = Arc::new(Barrier::new(2));
    let producer_gate = gate.clone();
    let producer = thread::spawn(move || {
        let mut accepted = Vec::new();
        for value in 0..10 {
            handle.try_record(event(value)).unwrap();
            accepted.push(value);
        }
        producer_gate.wait();
        for value in 10..510 {
            match handle.try_record(event(value)) {
                Ok(()) => accepted.push(value),
                Err(SubmitError::Closed) => {}
                other => panic!("unexpected admission: {other:?}"),
            }
        }
        accepted
    });
    gate.wait();
    let summary = recorder.shutdown().unwrap();
    let accepted = producer.join().unwrap();
    assert_eq!(summary.accepted_records, accepted.len() as u64);
    let connection = db.connect();
    let actual: Vec<i64> = connection
        .prepare("SELECT integer_value FROM measurements ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(actual, accepted);
}

#[test]
fn dropping_owner_drains_even_when_handles_remain() {
    let db = Database::new();
    let recorder = Recorder::with_options(
        &db.0,
        RecorderOptions {
            flush_interval: Duration::from_secs(60),
            ..options(8, 128)
        },
    )
    .unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    drop(recorder);
    assert_eq!(handle.state(), RecorderState::Stopped);
    assert_eq!(
        scalar(&db.connect(), "SELECT count(*) FROM measurements"),
        1
    );
}

#[test]
fn writer_panic_is_reported_without_unwinding_the_caller() {
    let db = Database::new();
    let hooks = Hooks {
        before_flush: Some(Box::new(|| panic!("injected writer panic"))),
        after_flush: None,
        ..Hooks::default()
    };
    let recorder = Recorder::start(&db.0, options(8, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    assert_eq!(recorder.shutdown(), Err(RecorderError::WriterPanicked));
    assert_eq!(
        handle.state(),
        RecorderState::Failed(RecorderError::WriterPanicked)
    );
    assert_eq!(
        scalar(
            &db.connect(),
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
}

#[test]
fn schema_constraints_reject_mixed_value_columns() {
    let db = Database::new();
    Recorder::open(&db.0, 8).unwrap().shutdown().unwrap();
    let connection = db.connect();
    assert!(connection.execute("INSERT INTO measurements(session_id, port_id, global_key, local_source, completed_at_ns, unit, value_type, integer_value, real_value) VALUES (1, 'b', 'd/v', 'v', 0, '', 'integer', ?, ?)", params![3, 3.0]).is_err());
}

#[test]
fn nanosecond_conversion_checks_both_exact_i64_boundaries() {
    assert_eq!(
        duration_ns(Duration::from_nanos(i64::MAX as u64), false),
        Ok(i64::MAX)
    );
    assert_eq!(
        duration_ns(Duration::from_nanos(i64::MAX as u64 + 1), true),
        Ok(i64::MIN)
    );
    assert_eq!(
        duration_ns(Duration::from_nanos(i64::MAX as u64 + 1), false),
        Err(DataError::TimestampOutOfRange)
    );
    assert_eq!(
        duration_ns(Duration::from_nanos(i64::MAX as u64 + 2), true),
        Err(DataError::TimestampOutOfRange)
    );
    assert_eq!(duration_ns(Duration::from_nanos(1), true), Ok(-1));
    assert_eq!(
        duration_ns(Duration::MAX, false),
        Err(DataError::TimestampOutOfRange)
    );
}

#[test]
fn concurrent_loss_is_persisted_before_shutdown_and_survives_further_success() {
    let db = Database::new();
    let (hooks, entered, release, flushed) = first_flush_gate();
    let recorder = Recorder::start(&db.0, options(1, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    handle.try_record(event(2)).unwrap();
    assert_eq!(handle.try_record(event(3)), Err(SubmitError::Full));
    assert_eq!(
        handle.try_record(AggregatorEvent::DataLost {
            port_id: "bus".into(),
            loss_epoch: 42
        }),
        Err(SubmitError::Full)
    );
    release.send(()).unwrap();
    flushed.recv_timeout(WAIT).unwrap();
    flushed.recv_timeout(WAIT).unwrap();
    assert_eq!(handle.state(), RecorderState::Running);
    let connection = db.connect();
    assert_eq!(
        scalar(&connection, "SELECT recorder_incomplete FROM sessions"),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT recorder_dropped_data_lost_events FROM sessions",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "1"
    );
    let losses = handle.losses();
    handle.try_record(event(4)).unwrap();
    flushed.recv_timeout(WAIT).unwrap();
    assert_eq!(handle.losses(), losses);
    assert_eq!(scalar(&connection, "SELECT count(*) FROM measurements"), 3);
    assert_eq!(
        scalar(&connection, "SELECT recorder_incomplete FROM sessions"),
        1
    );
    assert_eq!(recorder.shutdown().unwrap().losses, losses);
}

#[test]
fn runtime_failure_is_published_without_waiting_for_shutdown() {
    let db = Database::new();
    let (mut hooks, entered, release, _) = first_flush_gate();
    let (stopped, wait_stopped) = mpsc::channel();
    hooks.stopped = Some(stopped);
    let recorder = Recorder::start(&db.0, options(4, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(1)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    assert_eq!(handle.state(), RecorderState::Running);
    let connection = db.connect();
    connection.execute_batch("CREATE TRIGGER fail_write BEFORE INSERT ON measurements BEGIN SELECT RAISE(ABORT, 'runtime failure'); END;").unwrap();
    release.send(()).unwrap();
    wait_stopped.recv_timeout(WAIT).unwrap();
    let RecorderState::Failed(error) = handle.state() else {
        panic!("failure not published")
    };
    assert!(
        matches!(&error, RecorderError::Sqlite { message, .. } if message.contains("runtime failure"))
    );
    for _ in 0..3 {
        assert_eq!(
            handle.try_record(event(2)),
            Err(SubmitError::Failed(error.clone()))
        );
    }
    assert_eq!(recorder.shutdown(), Err(error));
}

#[test]
fn invalid_observation_keeps_nonfinite_float_bits_and_out_of_range_time() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 4).unwrap();
    let mut obs = observation(0);
    obs.completed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000_000_000);
    obs.reading = Ok(Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)));
    recorder
        .handle()
        .try_record(AggregatorEvent::InvalidObservation {
            port_id: "bus".into(),
            observation: DeviceObservation {
                device_id: "unregistered".into(),
                observation: obs,
            },
            error: MappingError::UnknownDevice,
        })
        .unwrap();
    recorder.shutdown().unwrap();
    let connection = db.connect();
    let details: String = connection
        .query_row("SELECT details_json FROM events", [], |r| r.get(0))
        .unwrap();
    let details: Json = serde_json::from_str(&details).unwrap();
    assert_eq!(
        details["observation"]["reading"]["bits_hex"],
        "7ff8000000000001"
    );
    assert_eq!(
        details["observation"]["completed_at"]["seconds"],
        "10000000000"
    );
}

fn invalid_event() -> AggregatorEvent {
    let mut obs = observation(0);
    obs.reading = Ok(Value::Float(f64::NAN));
    AggregatorEvent::Measurement {
        port_id: "bus".into(),
        global_key: "device/value".into(),
        observation: obs,
    }
}

#[test]
fn concurrent_invalid_and_overflow_keep_distinct_counts_after_reopen() {
    let db = Database::new();
    let (hooks, entered, release, _) = first_flush_gate();
    let recorder = Recorder::start(&db.0, options(1, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(0)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    handle.try_record(event(1)).unwrap();
    assert_eq!(handle.try_record(event(2)), Err(SubmitError::Full));
    for _ in 0..3 {
        assert!(matches!(
            handle.try_record(invalid_event()),
            Err(SubmitError::InvalidData(_))
        ));
    }
    release.send(()).unwrap();
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.losses.dropped_events, 1);
    assert_eq!(summary.losses.rejected_invalid_data, 3);
    let second = Recorder::open(&db.0, 8).unwrap();
    second.handle().try_record(event(3)).unwrap();
    second.shutdown().unwrap();
    let connection = db.connect();
    assert_eq!(scalar(&connection, "PRAGMA user_version"), 1);
    assert_eq!(
        scalar(
            &connection,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        2
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT recorder_incomplete FROM sessions WHERE id=1"
        ),
        1
    );
    assert_eq!(
        scalar(
            &connection,
            "SELECT recorder_incomplete FROM sessions WHERE id=2"
        ),
        0
    );
    let details: String = connection.query_row("SELECT details_json FROM events WHERE session_id=1 AND event_type='RecorderDataLost' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    let details: Json = serde_json::from_str(&details).unwrap();
    assert_eq!(details["dropped_events"], "1");
    assert_eq!(details["rejected_invalid_data"], "3");
}

#[test]
fn concurrent_invalid_data_and_shutdown_preserve_final_snapshot() {
    let db = Database::new();
    let (hooks, entered, release, _) = first_flush_gate();
    let recorder = Recorder::start(&db.0, options(8, 1), hooks).unwrap();
    let handle = recorder.handle();
    handle.try_record(event(0)).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        handle.try_record(invalid_event()),
        Err(SubmitError::InvalidData(_))
    ));
    let barrier = Arc::new(Barrier::new(2));
    let producer_barrier = barrier.clone();
    let producer_handle = handle.clone();
    let producer = thread::spawn(move || {
        producer_barrier.wait();
        let mut rejected = 1;
        for _ in 0..1000 {
            match producer_handle.try_record(invalid_event()) {
                Err(SubmitError::InvalidData(_)) => rejected += 1,
                Err(SubmitError::Closed) => {}
                result => panic!("unexpected {result:?}"),
            }
        }
        rejected
    });
    let closer = thread::spawn(move || {
        barrier.wait();
        recorder.shutdown().unwrap()
    });
    let rejected = producer.join().unwrap();
    release.send(()).unwrap();
    let summary = closer.join().unwrap();
    assert_eq!(summary.losses.rejected_invalid_data, rejected);
    assert_eq!(handle.losses(), summary.losses);
    let connection = db.connect();
    let value: String = connection.query_row("SELECT json_extract(details_json, '$.rejected_invalid_data') FROM events WHERE event_type='RecorderDataLost' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    assert_eq!(value, rejected.to_string());
    assert_eq!(
        scalar(&connection, "SELECT recorder_incomplete FROM sessions"),
        1
    );
}

#[test]
fn invalid_data_counter_saturates_independently() {
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 8).unwrap();
    let handle = recorder.handle();
    control(&handle.shared).losses.rejected_invalid_data = u64::MAX - 1;
    for _ in 0..3 {
        assert!(matches!(
            handle.try_record(invalid_event()),
            Err(SubmitError::InvalidData(_))
        ));
    }
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.losses.rejected_invalid_data, u64::MAX);
    assert_eq!(summary.losses.dropped_events, 0);
    let connection = db.connect();
    let value: String = connection.query_row("SELECT json_extract(details_json, '$.rejected_invalid_data') FROM events WHERE event_type='RecorderDataLost' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    assert_eq!(value, u64::MAX.to_string());
}
