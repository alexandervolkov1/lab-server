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
    let (mut server, calls) = server();
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
        (calls.load(Ordering::SeqCst) - final_epoch) as i64
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
        server.workers.request_shutdown();
        server.workers.join().unwrap();
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
    server.workers.request_shutdown();
    server.workers.join().unwrap();
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

struct FinishingRead {
    queued: usize,
    next: usize,
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
impl Driver for FinishingRead {
    fn parameters(&self) -> Vec<ParameterDescription> {
        let mut parameters = CounterDriver(Arc::new(AtomicU64::new(0))).parameters();
        // Fill the queue without relying on OS timer resolution. The last read
        // is controlled solely by the channels below.
        parameters[0].default_poll_period = Some(Duration::from_nanos(1));
        parameters
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        let value = self.next;
        self.next += 1;
        if value == self.queued {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(WAIT).unwrap();
        }
        Ok(Value::Integer(value as i64))
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        unreachable!()
    }
}

struct ShutdownFixture {
    server: StartedServer,
    ports: Vec<PortHandle>,
    releases: Vec<mpsc::Sender<()>>,
    entered: mpsc::Receiver<()>,
}
fn finishing_reads(queued: &[usize]) -> ShutdownFixture {
    let (entered, waiting) = mpsc::channel();
    let mut registry = DeviceRegistry::new();
    let mut sources = Vec::new();
    let mut workers = Workers::default();
    let mut ports = Vec::new();
    let mut releases = Vec::new();
    for (index, &queued) in queued.iter().enumerate() {
        let (release, gate) = mpsc::channel();
        let device = DeviceInstance::new(
            format!("d{index}"),
            FinishingRead {
                queued,
                next: 0,
                entered: entered.clone(),
                release: gate,
            },
        )
        .unwrap();
        let (worker, channels) = PortWorker::new(
            BufReader::new(Cursor::new(Vec::<u8>::new())),
            vec![device],
            COMMAND_QUEUE_CAPACITY,
            OBSERVATION_QUEUE_CAPACITY,
        )
        .unwrap();
        let port_id = format!("p{index}");
        registry
            .register_port(&port_id, channels.commands.clone())
            .unwrap();
        registry
            .register_device(&port_id, &worker.devices()[0])
            .unwrap();
        sources.push((port_id, channels.observations));
        ports.push(channels.commands.clone());
        releases.push(release);
        workers
            .0
            .push((channels.commands, thread::spawn(move || worker.run())));
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    ShutdownFixture {
        server: StartedServer {
            workers,
            listener,
            tcp: Arc::new(MultiDeviceTcp::new(
                CommandRouter::new(registry),
                OBSERVATION_QUEUE_CAPACITY,
            )),
            sources,
            recording: Recording::default(),
        },
        ports,
        releases,
        entered: waiting,
    }
}

#[test]
fn concurrent_shutdown_drains_before_join_preserving_last_reads_without_new_loss() {
    // Near-full and completely full queues, including independent physical ports.
    // The full case would lose the last reading with join-before-drain.
    for queued in [
        vec![OBSERVATION_QUEUE_CAPACITY - 1],
        vec![OBSERVATION_QUEUE_CAPACITY],
        vec![OBSERVATION_QUEUE_CAPACITY - 1, OBSERVATION_QUEUE_CAPACITY],
    ] {
        for recording_enabled in [true, false] {
            let db = Database::new();
            let ShutdownFixture {
                mut server,
                ports,
                releases,
                entered,
            } = finishing_reads(&queued);
            for _ in &ports {
                entered.recv_timeout(WAIT).unwrap();
            }
            assert!(ports.iter().all(|port| port.loss_epoch() == 0));
            let prefix_count = queued.iter().sum::<usize>();
            let (flushed, committed) = mpsc::channel();
            if recording_enabled {
                let recorder = Recorder::start(
                    &db.0,
                    RecorderOptions {
                        batch_size: prefix_count,
                        ..RecorderOptions::default()
                    },
                    Hooks {
                        after_flush: Some(Box::new(move || {
                            let _ = flushed.send(());
                        })),
                        ..Hooks::default()
                    },
                )
                .unwrap();
                attach(&mut server, recorder);
            }
            // The publication clock observes the actual shared dispatch path,
            // even with SQLite disabled and no active TCP subscriptions.
            let observer = server.backend().register_client().unwrap();
            let addr = server.local_addr().unwrap();
            let join = thread::spawn(move || server.run_until(&AtomicBool::new(true)));
            let deadline = Instant::now() + WAIT / 2;
            while observer.clock.load(Ordering::SeqCst) != prefix_count as u64
                && Instant::now() < deadline
            {
                thread::yield_now();
            }
            let drained_before_release =
                observer.clock.load(Ordering::SeqCst) == prefix_count as u64;
            let waiting_for_drivers = !join.is_finished();
            if recording_enabled && drained_before_release {
                let c = db.connect();
                // Flushes may be partial; wait on actual commits, not a delay.
                while scalar(&c, "SELECT count(*) FROM measurements") != prefix_count as i64 {
                    committed.recv_timeout(WAIT).unwrap();
                }
                assert_eq!(
                    scalar(
                        &c,
                        "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
                    ),
                    1
                );
            }
            for release in releases {
                release.send(()).unwrap();
            }
            join.join().unwrap().unwrap();
            // Clean up the owned threads before asserting a failure, so a
            // regression cannot strand drivers or leave SQLite open on unwind.
            assert!(
                drained_before_release,
                "aggregator stopped draining before the last driver read completed"
            );
            assert!(waiting_for_drivers, "drivers still own their last read");
            TcpListener::bind(addr).unwrap();
            assert!(ports.iter().all(|port| port.loss_epoch() == 0));
            assert_eq!(
                observer.clock.load(Ordering::SeqCst),
                (prefix_count + ports.len()) as u64
            );
            if recording_enabled {
                let c = db.connect();
                for (index, &count) in queued.iter().enumerate() {
                    let values: Vec<i64> = c
                        .prepare(
                            "SELECT integer_value FROM measurements WHERE port_id=? ORDER BY id",
                        )
                        .unwrap()
                        .query_map([format!("p{index}")], |r| r.get(0))
                        .unwrap()
                        .map(Result::unwrap)
                        .collect();
                    assert_eq!(values, (0..=count as i64).collect::<Vec<_>>());
                }
                assert_eq!(
                    scalar(
                        &c,
                        "SELECT count(*) FROM events WHERE event_type='PortStopped' AND json_extract(details_json,'$.reason')='Shutdown'"
                    ),
                    ports.len() as i64
                );
                assert_eq!(
                    scalar(
                        &c,
                        "SELECT count(*) FROM events WHERE event_type IN ('DataLost','RecorderDataLost','SourceClosedWithoutStop')"
                    ),
                    0
                );
                assert_eq!(
                    scalar(
                        &c,
                        "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL AND recorder_incomplete=0"
                    ),
                    1
                );
            } else {
                assert!(!db.0.exists());
            }
        }
    }
}

#[test]
fn concurrent_worker_panic_and_recorder_close_error_are_both_retained_after_drain() {
    let db = Database::new();
    let ShutdownFixture {
        mut server,
        ports,
        releases,
        entered,
    } = finishing_reads(&[1, 1]);
    for _ in &ports {
        entered.recv_timeout(WAIT).unwrap();
    }
    // Wrap one genuine worker join in a panicking owner, without changing the
    // PortWorker or its stop reason/receiver lifecycle.
    let (handle, worker) = server.workers.0.remove(0);
    server.workers.0.push((
        handle,
        thread::spawn(move || {
            worker.join().unwrap();
            panic!("test worker owner panic");
        }),
    ));
    attach(&mut server, Recorder::open(&db.0, 8).unwrap());
    db.connect().execute_batch("CREATE TRIGGER fail_finish BEFORE UPDATE OF ended_at_ns ON sessions BEGIN SELECT RAISE(FAIL, 'injected finish failure'); END;").unwrap();
    let observer = server.backend().register_client().unwrap();
    let join = thread::spawn(move || server.run_until(&AtomicBool::new(true)));
    wait_for(|| observer.clock.load(Ordering::SeqCst) == 2);
    for release in releases {
        release.send(()).unwrap();
    }
    let error = join.join().unwrap().unwrap_err().to_string();
    assert!(error.contains("port worker thread panicked"), "{error}");
    assert!(error.contains("injected finish failure"), "{error}");
    let c = db.connect();
    assert_eq!(scalar(&c, "SELECT count(*) FROM measurements"), 4);
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='PortStopped'"
        ),
        2
    );
}

#[test]
fn concurrent_drop_requests_all_workers_before_joining_them() {
    let ShutdownFixture {
        server,
        ports,
        releases,
        entered,
    } = finishing_reads(&[0, 0]);
    for _ in &ports {
        entered.recv_timeout(WAIT).unwrap();
    }
    let join = thread::spawn(move || drop(server));
    wait_for(|| {
        ports
            .iter()
            .all(|port| port.stop_reason() == Some(StopReason::Shutdown))
    });
    assert!(!join.is_finished());
    for release in releases {
        release.send(()).unwrap();
    }
    join.join().unwrap();
    assert!(
        ports
            .iter()
            .all(|port| port.stop_reason() == Some(StopReason::Shutdown))
    );
}

#[test]
fn concurrent_full_warning_is_once_and_summary_uses_recorder_final_counts() {
    for invalid_count in [0, 3] {
        let db = Database::new();
        let (entered, waiting) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let mut first = true;
        let recorder = Recorder::start(
            &db.0,
            RecorderOptions {
                queue_capacity: 1,
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
        let handle = recorder.handle();
        let mut recording = Recording {
            recorder: Some(recorder),
            handle: Some(handle.clone()),
            ..Recording::default()
        };
        recording.record(&event(Value::Integer(1)));
        waiting.recv_timeout(WAIT).unwrap();
        recording.record(&event(Value::Integer(2)));
        for _ in 0..7 {
            recording.record(&event(Value::Integer(3)));
        }
        recording.record(&AggregatorEvent::DataLost {
            port_id: "bus".into(),
            loss_epoch: 5,
        });
        for _ in 0..invalid_count {
            recording.record(&event(Value::Float(f64::NAN)));
        }
        assert_eq!(
            recording
                .diagnostics
                .iter()
                .filter(|s| s.starts_with("Warning:"))
                .count(),
            1
        );
        assert_eq!(
            recording.diagnostics[0],
            "Warning: recorder queue is full; recording is incomplete; polling and TCP continue"
        );
        release.send(()).unwrap();
        let mut errors = ServerErrors::default();
        recording.finish(&mut errors);
        let result = errors.result();
        if invalid_count == 0 {
            result.unwrap();
        } else {
            assert!(result.unwrap_err().to_string().contains("invalid data"));
        }
        let c = db.connect();
        let accepted = scalar(&c, "SELECT count(*) FROM measurements");
        let dropped: String = c
            .query_row("SELECT recorder_dropped_events FROM sessions", [], |r| {
                r.get(0)
            })
            .unwrap();
        let lost: String = c
            .query_row(
                "SELECT recorder_dropped_data_lost_events FROM sessions",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let invalid: String = c.query_row("SELECT json_extract(details_json,'$.rejected_invalid_data') FROM events WHERE event_type='RecorderDataLost' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
        assert_eq!(accepted, 2);
        assert_eq!(dropped, "8");
        assert_eq!(lost, "1");
        assert_eq!(invalid, invalid_count.to_string());
        assert_eq!(
            scalar(
                &c,
                "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL AND recorder_incomplete=1"
            ),
            1
        );
        assert_eq!(
            recording.diagnostics.last().unwrap(),
            &format!(
                "Recording summary: session_id={} accepted_records={accepted} dropped_events={dropped} dropped_data_lost_events={lost} rejected_invalid_data={invalid} incomplete=true",
                handle.session_id()
            )
        );
        let count = recording.diagnostics.len();
        recording.finish(&mut ServerErrors::default());
        assert_eq!(recording.diagnostics.len(), count);
    }
}

#[test]
fn successful_complete_summary_and_disabled_recording_diagnostics() {
    let mut disabled = Recording::default();
    disabled.record(&event(Value::Integer(1)));
    let mut errors = ServerErrors::default();
    disabled.finish(&mut errors);
    errors.result().unwrap();
    assert!(disabled.diagnostics.is_empty());
    let db = Database::new();
    let recorder = Recorder::open(&db.0, 8).unwrap();
    let mut recording = Recording {
        handle: Some(recorder.handle()),
        recorder: Some(recorder),
        ..Recording::default()
    };
    recording.record(&event(Value::Integer(1)));
    let mut errors = ServerErrors::default();
    recording.finish(&mut errors);
    errors.result().unwrap();
    assert_eq!(
        recording.diagnostics,
        [
            "Recording summary: session_id=1 accepted_records=1 dropped_events=0 dropped_data_lost_events=0 rejected_invalid_data=0 incomplete=false"
        ]
    );
    assert_eq!(
        scalar(&db.connect(), "SELECT count(*) FROM measurements"),
        1
    );
}
