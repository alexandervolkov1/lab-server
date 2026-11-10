use super::*;
use lab_server::config::RecordingConfig;
use lab_server::driver::PortState;
use std::sync::mpsc;
#[path = "../support/recording_db.rs"]
mod db;
use db::{Database, scalar};

fn enable(config: &mut ServerConfig, db: &Database) {
    config.recording = Some(RecordingConfig {
        enabled: true,
        path: Some(db.0.clone()),
        queue_capacity: None,
    });
}
fn wait_for(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() {
        assert!(Instant::now() < deadline, "condition not reached");
        thread::yield_now();
    }
}

#[test]
fn sqlite_open_failure_precedes_hardware_and_releases_listener() {
    let db = Database::new();
    let mut config = config(1, &[("a", "fake_a", 0)]);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    config.tcp.bind = listener.local_addr().unwrap();
    drop(listener);
    enable(&mut config, &db);
    config.recording.as_mut().unwrap().path = Some(db.0.join("missing/measurements.sqlite"));
    let mut opens = 0;
    let result = start_with(&config, factory, |_, _| -> io::Result<FakeIo> {
        opens += 1;
        unreachable!()
    });
    let error = result.err().expect("opening must fail");
    assert!(error.to_string().contains("SQLite open"), "{error}");
    assert_eq!(opens, 0);
    TcpListener::bind(config.tcp.bind).unwrap();
    assert!(!db.0.exists());
}

#[test]
fn disabled_recording_does_not_open_even_an_invalid_path() {
    let db = Database::new();
    let mut config = config(1, &[("a", "fake_a", 0)]);
    enable(&mut config, &db);
    let recording = config.recording.as_mut().unwrap();
    recording.enabled = false;
    recording.path = Some(db.0.join("missing/file.sqlite"));
    let (running, _) = launch(&config, &[0]);
    assert_eq!(
        Client::new(running.addr).request(json!({"op":"hello"}))["version"],
        1
    );
    drop(running);
    assert!(!db.0.exists());
}

#[test]
fn unavailable_ports_keep_tcp_and_database_and_restart_adds_session() {
    let db = Database::new();
    let mut config = config(1, &[("a", "fake_a", 0)]);
    enable(&mut config, &db);
    for _ in 0..2 {
        let (running, _) = launch(&config, &[0]);
        let addr = running.addr;
        let mut client = Client::new(addr);
        assert_eq!(client.request(json!({"op":"hello"}))["version"], 1);
        assert_eq!(
            client.request(json!({"op":"get","key":"a/value"}))["error"],
            "runtime_unavailable"
        );
        assert!(db.0.exists());
        drop(running);
        TcpListener::bind(addr).unwrap();
    }
    let c = db.connect();
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        2
    );
    assert_eq!(scalar(&c, "SELECT count(*) FROM measurements"), 0);
}

// Each actual driver operation has a channel rendezvous. Tests decide when it
// completes; no sleeps are used to guess worker progress or subscription races.
struct Operation {
    device: String,
    key: String,
    reply: mpsc::Sender<Result<Value, DriverError>>,
}
struct ControlledDriver {
    id: String,
    operations: mpsc::Sender<Operation>,
    typed: bool,
}
impl Driver for ControlledDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        let keys: &[&str] = if self.typed {
            &["integer", "float", "boolean", "error"]
        } else {
            &["sample"]
        };
        keys.iter()
            .map(|key| ParameterDescription {
                id: (*key).into(),
                unit: "unit".into(),
                writable: false,
                spec: match *key {
                    "float" => Spec::Float {
                        min: -10.0,
                        max: 10.0,
                    },
                    "boolean" => Spec::Boolean,
                    _ => Spec::Integer {
                        min: i64::MIN,
                        max: i64::MAX,
                    },
                },
                default_poll_period: Some(Duration::from_millis(1)),
            })
            .collect()
    }
    fn read(&mut self, key: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        let (reply, response) = mpsc::channel();
        self.operations
            .send(Operation {
                device: self.id.clone(),
                key: key.into(),
                reply,
            })
            .unwrap();
        response.recv_timeout(WAIT).unwrap()
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        unreachable!()
    }
}
fn controlled(config: &ServerConfig, typed: bool) -> (StartedServer, mpsc::Receiver<Operation>) {
    let (send, receive) = mpsc::channel();
    let server = start_with(
        config,
        |device| {
            Ok(DriverSetup {
                driver: Box::new(ControlledDriver {
                    id: device.id.clone(),
                    operations: send.clone(),
                    typed,
                }),
                post_open_delay: Duration::ZERO,
            })
        },
        |_, _| Ok(BufReader::new(Cursor::new(Vec::<u8>::new()))),
    )
    .unwrap();
    (server, receive)
}
fn reply_value(op: Operation) -> (String, String) {
    let value = match op.key.as_str() {
        "float" => Ok(Value::Float(1.25)),
        "boolean" => Ok(Value::Boolean(true)),
        "error" => Err(DriverError::new("sensor error", PortState::Synchronized)),
        _ => Ok(Value::Integer(i64::MAX)),
    };
    op.reply.send(value).unwrap();
    (op.device, op.key)
}

#[test]
fn concurrent_typed_measurements_without_clients_for_single_shared_and_independent_ports() {
    for (ports, devices) in [
        (1, vec![("a", "fake_a", 0)]),
        (1, vec![("a", "fake_a", 0), ("b", "fake_b", 0)]),
        (2, vec![("a", "fake_a", 0), ("b", "fake_b", 1)]),
    ] {
        let db = Database::new();
        let mut config = config(ports, &devices);
        enable(&mut config, &db);
        let (server, operations) = controlled(&config, true);
        let backend = server.backend().clone();
        let running = Running::new(server);
        let mut seen = std::collections::HashSet::new();
        while seen.len() < devices.len() * 4 {
            seen.insert(reply_value(operations.recv_timeout(WAIT).unwrap()));
        }
        running.stop.store(true, Ordering::Release);
        // Wait for each worker's shutdown request, then release its current call.
        wait_for(|| {
            config.ports.iter().all(|p| {
                backend
                    .registry()
                    .port(&p.id)
                    .unwrap()
                    .handle()
                    .unwrap()
                    .stop_reason()
                    .is_some()
            })
        });
        loop {
            match operations.recv_timeout(WAIT) {
                Ok(op) => {
                    reply_value(op);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(error) => panic!("workers did not finish: {error}"),
            }
        }
        drop(running);
        let c = db.connect();
        assert_eq!(
            scalar(&c, "SELECT count(DISTINCT global_key) FROM measurements"),
            (devices.len() * 4) as i64
        );
        assert_eq!(
            scalar(
                &c,
                "SELECT count(*) FROM measurements WHERE global_key != substr(global_key,1,instr(global_key,'/')) || local_source"
            ),
            0
        );
        assert_eq!(
            scalar(
                &c,
                "SELECT min(integer_value) FROM measurements WHERE value_type='integer'"
            ),
            i64::MAX
        );
        assert_eq!(
            c.query_row(
                "SELECT min(real_value) FROM measurements WHERE value_type='float'",
                [],
                |r| r.get::<_, f64>(0)
            )
            .unwrap(),
            1.25
        );
        assert_eq!(
            scalar(
                &c,
                "SELECT min(boolean_value) FROM measurements WHERE value_type='boolean'"
            ),
            1
        );
        assert_eq!(
            scalar(
                &c,
                "SELECT count(DISTINCT global_key) FROM measurements WHERE read_error='sensor error'"
            ),
            devices.len() as i64
        );
        assert_eq!(
            scalar(
                &c,
                "SELECT count(*) FROM events WHERE event_type='PortStopped' AND json_extract(details_json,'$.reason')='Shutdown'"
            ),
            ports as i64
        );
        assert_eq!(
            scalar(
                &c,
                "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
            ),
            1
        );
    }
}

#[test]
fn concurrent_tcp_subscription_unsubscribe_and_last_inflight_observation_reach_sqlite() {
    let db = Database::new();
    let mut config = config(1, &[("a", "fake_a", 0)]);
    enable(&mut config, &db);
    let (server, operations) = controlled(&config, false);
    let backend = server.backend().clone();
    let running = Running::new(server);
    let mut client = Client::new(running.addr);
    assert_eq!(
        client.request(json!({"op":"subscribe","key":"a/sample"}))["status"],
        "ok"
    );
    let first = operations.recv_timeout(WAIT).unwrap();
    first.reply.send(Ok(Value::Integer(42))).unwrap();
    assert_eq!(client.event("a/sample")["value"], 42);
    let last = operations.recv_timeout(WAIT).unwrap();
    assert_eq!(
        client.request(json!({"op":"unsubscribe","keys":["a/sample"]}))["status"],
        "ok"
    );
    running.stop.store(true, Ordering::Release);
    wait_for(|| {
        backend
            .registry()
            .port("p0")
            .unwrap()
            .handle()
            .unwrap()
            .stop_reason()
            .is_some()
    });
    last.reply.send(Ok(Value::Integer(43))).unwrap();
    let addr = running.addr;
    drop(running);
    TcpListener::bind(addr).unwrap();
    let c = db.connect();
    let values: Vec<i64> = c
        .prepare("SELECT integer_value FROM measurements ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(values, [42, 43]);
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='PortStopped' AND json_extract(details_json,'$.reason')='Shutdown'"
        ),
        1
    );
}

#[test]
fn concurrent_port_failure_does_not_stop_neighbor_recording() {
    let db = Database::new();
    let mut config = config(2, &[("a", "fake_a", 0), ("b", "fake_b", 1)]);
    enable(&mut config, &db);
    let (server, operations) = controlled(&config, false);
    let backend = server.backend().clone();
    let running = Running::new(server);
    let mut initial = vec![
        operations.recv_timeout(WAIT).unwrap(),
        operations.recv_timeout(WAIT).unwrap(),
    ];
    let bad = initial.remove(initial.iter().position(|op| op.device == "a").unwrap());
    bad.reply
        .send(Err(DriverError::new("unplugged", PortState::Disconnected)))
        .unwrap();
    wait_for(|| {
        backend
            .registry()
            .port("p0")
            .unwrap()
            .handle()
            .unwrap()
            .stop_reason()
            .is_some()
    });
    initial
        .pop()
        .unwrap()
        .reply
        .send(Ok(Value::Integer(77)))
        .unwrap();
    let last = operations.recv_timeout(WAIT).unwrap();
    assert_eq!(last.device, "b");
    running.stop.store(true, Ordering::Release);
    wait_for(|| {
        backend
            .registry()
            .port("p1")
            .unwrap()
            .handle()
            .unwrap()
            .stop_reason()
            .is_some()
    });
    last.reply.send(Ok(Value::Integer(78))).unwrap();
    drop(running);
    let c = db.connect();
    assert_eq!(
        scalar(&c, "SELECT count(*) FROM measurements WHERE port_id='p1'"),
        2
    );
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM measurements WHERE read_error='unplugged'"
        ),
        1
    );
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM events WHERE event_type='PortStopped'"
        ),
        2
    );
}
