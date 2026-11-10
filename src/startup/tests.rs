use super::*;
use crate::driver::DriverError;
use crate::model::{Observation, ParameterDescription, Spec, Value};
use crate::recorder::{Hooks, RecorderError};
use std::io::{BufRead, Cursor, Write};
use std::sync::atomic::AtomicU64;
use std::time::{Instant, SystemTime};
#[path = "../../tests/support/recording_db.rs"]
mod db;
use db::{Database, scalar};
const WAIT: Duration = Duration::from_secs(10);

fn wait_for(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() {
        assert!(Instant::now() < deadline, "condition not reached");
        thread::yield_now();
    }
}
struct CounterDriver(Arc<AtomicU64>);
impl Driver for CounterDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "value".into(),
            unit: "unit".into(),
            writable: false,
            spec: Spec::Integer {
                min: 0,
                max: i64::MAX,
            },
            default_poll_period: Some(Duration::from_micros(10)),
        }]
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        Ok(Value::Integer(self.0.fetch_add(1, Ordering::SeqCst) as i64))
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        unreachable!()
    }
}
fn server() -> (StartedServer, Arc<AtomicU64>) {
    let config = ServerConfig::from_toml("[tcp]\nbind='127.0.0.1:0'\n[[ports]]\nid='bus'\nname='FAKE'\nbaud=9600\ndata_bits=8\nparity='none'\nstop_bits=1\nflow_control='none'\ntimeout_ms=100\n[[devices]]\nid='device'\nport='bus'\ndriver='test'").unwrap();
    let calls = Arc::new(AtomicU64::new(0));
    let server = start_with(
        &config,
        |_| {
            Ok(DriverSetup {
                driver: Box::new(CounterDriver(calls.clone())),
                post_open_delay: Duration::ZERO,
            })
        },
        |_, _| Ok(BufReader::new(Cursor::new(Vec::<u8>::new()))),
    )
    .unwrap();
    (server, calls)
}
fn event(value: Value) -> AggregatorEvent {
    AggregatorEvent::Measurement {
        port_id: "bus".into(),
        global_key: "device/value".into(),
        observation: Observation {
            source: "value".into(),
            unit: "unit".into(),
            completed_at: SystemTime::now(),
            reading: Ok(value),
        },
    }
}
fn attach(server: &mut StartedServer, recorder: Recorder) -> RecorderHandle {
    let handle = recorder.handle();
    server.recording = Recording {
        recorder: Some(recorder),
        handle: Some(handle.clone()),
        ..Recording::default()
    };
    handle
}
fn hello(addr: SocketAddr) {
    let stream = TcpStream::connect_timeout(&addr, WAIT).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut stream = BufReader::new(stream);
    writeln!(stream.get_mut(), "{{\"op\":\"hello\",\"id\":1}}").unwrap();
    let mut line = String::new();
    stream.read_line(&mut line).unwrap();
    let reply: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["version"], 1);
}
fn spawn(server: StartedServer) -> (Arc<AtomicBool>, JoinHandle<io::Result<()>>) {
    let stop = Arc::new(AtomicBool::new(false));
    let signal = stop.clone();
    (stop, thread::spawn(move || server.run_until(&signal)))
}

#[test]
fn concurrent_recorder_overflow_keeps_tcp_and_polling_and_final_counts() {
    let db = Database::new();
    let (entered, waiting) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut first = true;
    let recorder = Recorder::start(
        &db.0,
        RecorderOptions {
            queue_capacity: 2,
            batch_size: 1,
            ..RecorderOptions::default()
        },
        Hooks {
            before_flush: Some(Box::new(move || {
                if first {
                    first = false;
                    entered.send(()).unwrap();
                    gate.recv_timeout(WAIT).unwrap();
                }
            })),
            ..Hooks::default()
        },
    )
    .unwrap();
    recorder
        .handle()
        .try_record(event(Value::Integer(0)))
        .unwrap();
    waiting.recv_timeout(WAIT).unwrap();
    let (mut server, calls) = server();
    let handle = attach(&mut server, recorder);
    let port = server
        .backend()
        .registry()
        .port("bus")
        .unwrap()
        .handle()
        .unwrap()
        .clone();
    wait_for(|| port.loss_epoch() > 0);
    let addr = server.local_addr().unwrap();
    let (stop, join) = spawn(server);
    wait_for(|| handle.losses().dropped_events > 0);
    let before = calls.load(Ordering::SeqCst);
    hello(addr);
    wait_for(|| calls.load(Ordering::SeqCst) > before);
    stop.store(true, Ordering::Release);
    wait_for(|| handle.state() == RecorderState::Closing);
    let final_losses = handle.losses();
    release.send(()).unwrap();
    join.join().unwrap().unwrap();
    let c = db.connect();
    let stored: String = c
        .query_row("SELECT recorder_dropped_events FROM sessions", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(stored, final_losses.dropped_events.to_string());
    assert!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='DataLost' AND origin='port_worker'"
        ) > 0
    );
    assert!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='RecorderDataLost' AND origin='recorder'"
        ) > 0
    );
    assert_eq!(scalar(&c, "SELECT recorder_incomplete FROM sessions"), 1);
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        1
    );
}

#[test]
fn concurrent_sqlite_failure_keeps_tcp_and_worker_and_returns_original_error() {
    let db = Database::new();
    let (stopped, wait_stopped) = mpsc::channel();
    let recorder = Recorder::start(
        &db.0,
        RecorderOptions {
            batch_size: 1,
            ..RecorderOptions::default()
        },
        Hooks {
            stopped: Some(stopped),
            ..Hooks::default()
        },
    )
    .unwrap();
    db.connect().execute_batch("CREATE TRIGGER fail_insert BEFORE INSERT ON measurements BEGIN SELECT RAISE(FAIL, 'injected write failure'); END;").unwrap();
    let (mut server, calls) = server();
    let handle = attach(&mut server, recorder);
    let addr = server.local_addr().unwrap();
    let (stop, join) = spawn(server);
    wait_stopped.recv_timeout(WAIT).unwrap();
    assert!(matches!(handle.state(), RecorderState::Failed(_)));
    let before = calls.load(Ordering::SeqCst);
    hello(addr);
    wait_for(|| calls.load(Ordering::SeqCst) > before);
    stop.store(true, Ordering::Release);
    let error = join.join().unwrap().unwrap_err();
    assert!(
        error.to_string().contains("injected write failure"),
        "{error}"
    );
    TcpListener::bind(addr).unwrap();
    assert_eq!(
        scalar(
            &db.connect(),
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
}

#[test]
fn concurrent_shutdown_drains_worker_queue_and_final_port_loss_before_terminal_event() {
    let db = Database::new();
    let (mut server, _) = server();
    let port = server
        .backend()
        .registry()
        .port("bus")
        .unwrap()
        .handle()
        .unwrap()
        .clone();
    attach(&mut server, Recorder::open(&db.0, 1024).unwrap());
    // No aggregator loop yet: force real PortWorker queue loss deterministically.
    wait_for(|| port.loss_epoch() > 0);
    server.run_until(&AtomicBool::new(true)).unwrap();
    let final_epoch = port.loss_epoch();
    let c = db.connect();
    assert_eq!(
        scalar(&c, "SELECT count(*) FROM measurements"),
        OBSERVATION_QUEUE_CAPACITY as i64
    );
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='PortStopped' AND json_extract(details_json,'$.reason')='Shutdown'"
        ),
        1
    );
    let lost: String = c.query_row("SELECT json_extract(details_json,'$.loss_epoch') FROM events WHERE event_type='DataLost' AND origin='port_worker' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    assert_eq!(lost, final_epoch.to_string());
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='RecorderDataLost'"
        ),
        0
    );
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        1
    );
}

#[test]
fn invalid_data_from_aggregator_marks_incomplete_and_shutdown_preserves_both_errors() {
    for fail_finish in [false, true] {
        let db = Database::new();
        let (mut server, _) = server();
        // Stop genuine workers first; supply controlled boundary payloads to
        // exercise data validation that today's drivers normally preclude.
        server.workers.shutdown().unwrap();
        let (send, receive) = mpsc::channel();
        server.sources = vec![("bus".into(), receive)];
        for value in [Value::Float(f64::NAN), Value::Integer(123)] {
            let AggregatorEvent::Measurement { observation, .. } = event(value) else {
                unreachable!()
            };
            send.send(DeviceObservation {
                device_id: "device".into(),
                observation,
            })
            .unwrap();
        }
        drop(send);
        attach(&mut server, Recorder::open(&db.0, 8).unwrap());
        if fail_finish {
            db.connect().execute_batch("CREATE TRIGGER fail_finish BEFORE UPDATE OF ended_at_ns ON sessions BEGIN SELECT RAISE(FAIL, 'injected finish failure'); END;").unwrap();
        }
        let error = server.run_until(&AtomicBool::new(true)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("measurement float must be finite"),
            "{error}"
        );
        assert_eq!(
            error.to_string().contains("injected finish failure"),
            fail_finish
        );
        let c = db.connect();
        assert_eq!(scalar(&c, "SELECT recorder_incomplete FROM sessions"), 1);
        assert_eq!(scalar(&c, "SELECT integer_value FROM measurements"), 123);
        assert_eq!(
            scalar(
                &c,
                "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
            ),
            i64::from(!fail_finish)
        );
    }
}

#[test]
fn dispatch_records_all_aggregator_variants_and_closed_admission_is_an_error() {
    let db = Database::new();
    let (mut server, _) = server();
    server.workers.shutdown().unwrap();
    let (send, receive) = mpsc::channel();
    let AggregatorEvent::Measurement { observation, .. } = event(Value::Integer(1)) else {
        unreachable!()
    };
    send.send(DeviceObservation {
        device_id: "unknown".into(),
        observation,
    })
    .unwrap();
    drop(send);
    attach(&mut server, Recorder::open(&db.0, 8).unwrap());
    let mut aggregator =
        ObservationAggregator::new(server.tcp.registry(), [("bus".into(), receive)]).unwrap();
    dispatch(&server.tcp, &mut aggregator, &mut server.recording).unwrap();
    // Explicitly exercise unexplained EOF (normal startup must never fabricate it).
    server
        .recording
        .record(&AggregatorEvent::SourceClosedWithoutStop {
            port_id: "bus".into(),
        });
    server.recording.record(&AggregatorEvent::DataLost {
        port_id: "bus".into(),
        loss_epoch: 9,
    });
    dispatch(&server.tcp, &mut aggregator, &mut server.recording).unwrap();
    let mut errors = ServerErrors::default();
    server.recording.finish(&mut errors);
    errors.result().unwrap();
    let c = db.connect();
    for kind in [
        "InvalidObservation",
        "SourceClosedWithoutStop",
        "DataLost",
        "PortStopped",
    ] {
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM events WHERE event_type=?",
                [kind],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }
    server.recording.record(&event(Value::Integer(2)));
    let mut errors = ServerErrors::default();
    server.recording.finish(&mut errors);
    assert!(
        errors
            .result()
            .unwrap_err()
            .to_string()
            .contains("closing or stopped")
    );
}

#[test]
fn recording_keeps_first_diagnostics_without_unbounded_error_growth() {
    let mut recording = Recording::default();
    for _ in 0..1000 {
        recording.submitted(Err(SubmitError::InvalidData(DataError::NonFiniteFloat)));
        recording.submitted(Err(SubmitError::Failed(RecorderError::WriterDisconnected)));
    }
    let mut errors = ServerErrors(vec![Box::new(io::Error::other("original loop failure"))]);
    recording.finish(&mut errors);
    assert_eq!(errors.0.len(), 3);
    let text = errors.result().unwrap_err().to_string();
    assert!(text.starts_with("original loop failure"));
    assert!(text.contains("measurement float must be finite"));
    assert!(text.contains("disconnected unexpectedly"));
}
