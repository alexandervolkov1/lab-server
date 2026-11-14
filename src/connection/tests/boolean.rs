use super::*;
use crate::config::ServerConfig;
use crate::driver::{DeviceIo, Driver, DriverError, PortState};
use crate::model::Spec;
use crate::startup::{DriverSetup, start_with};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

const POLL_PERIOD: Duration = Duration::from_millis(10);

#[derive(Default)]
struct DriverState {
    writes: Mutex<Vec<(String, Value)>>,
    fail_write: AtomicBool,
}

struct BooleanDriver {
    state: Arc<DriverState>,
    enabled: bool,
    fault_reads: usize,
}

impl Driver for BooleanDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![
            ParameterDescription {
                id: "enabled".into(),
                spec: Spec::Boolean,
                unit: String::new(),
                writable: true,
                default_poll_period: None,
            },
            ParameterDescription {
                id: "fault".into(),
                spec: Spec::Boolean,
                unit: String::new(),
                writable: false,
                default_poll_period: Some(POLL_PERIOD),
            },
        ]
    }

    fn read(&mut self, parameter: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        match parameter {
            "enabled" => Ok(Value::Boolean(self.enabled)),
            "fault" => {
                let reading = match self.fault_reads % 3 {
                    0 => Ok(Value::Boolean(true)),
                    1 => Ok(Value::Boolean(false)),
                    _ => Err(DriverError::new(
                        "simulated read failure",
                        PortState::Synchronized,
                    )),
                };
                self.fault_reads += 1;
                reading
            }
            _ => Err("unknown parameter".into()),
        }
    }

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        _: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        self.state
            .writes
            .lock()
            .unwrap()
            .push((parameter.into(), value.clone()));
        match (parameter, value) {
            ("enabled", Value::Boolean(value)) => self.enabled = *value,
            _ => return Err("invalid write reached driver".into()),
        }
        if self.state.fail_write.load(Ordering::SeqCst) {
            // The write took effect, but its acknowledgement was lost.
            Err(DriverError::new(
                "simulated write reply loss",
                PortState::Synchronized,
            ))
        } else {
            Ok(())
        }
    }
}

// Use the production startup, aggregator and TCP loop with in-memory device I/O.
struct Fixture {
    client: TcpStream,
    decoder: FrameDecoder,
    messages: VecDeque<serde_json::Value>,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<io::Result<()>>>,
    state: Arc<DriverState>,
    next_id: u64,
}

impl Fixture {
    fn new() -> Self {
        let state = Arc::new(DriverState::default());
        let config = ServerConfig::from_toml(
            "[tcp]\nbind='127.0.0.1:0'\n[[ports]]\nid='p'\nname='MOCK'\nbaud=9600\ndata_bits=8\nparity='none'\nstop_bits=1\nflow_control='none'\ntimeout_ms=100\n[[devices]]\nid='switch'\ndriver='boolean'\nport='p'\n",
        ).unwrap();
        let server = start_with(
            &config,
            |_| {
                Ok(DriverSetup {
                    driver: Box::new(BooleanDriver {
                        state: state.clone(),
                        enabled: false,
                        fault_reads: 0,
                    }),
                    post_open_delay: Duration::ZERO,
                })
            },
            |_, _| Ok(BufReader::new(Cursor::new(Vec::<u8>::new()))),
        )
        .unwrap();
        let address = server.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let server = Some(thread::spawn(move || server.run_until(&flag)));
        let client = TcpStream::connect(address).unwrap();
        client.set_nonblocking(true).unwrap();
        Self {
            client,
            decoder: FrameDecoder::default(),
            messages: VecDeque::new(),
            stop,
            server,
            state,
            next_id: 1,
        }
    }
    fn send(&mut self, mut request: serde_json::Value) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        request["id"] = json!(id);
        writeln!(self.client, "{request}").unwrap();
        id
    }

    fn next_message(&mut self) -> serde_json::Value {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(message) = self.messages.pop_front() {
                return message;
            }
            let mut buffer = [0; 4096];
            match self.client.read(&mut buffer) {
                Ok(0) => panic!("unexpected TCP EOF"),
                Ok(count) => {
                    for frame in self.decoder.push(&buffer[..count]).unwrap() {
                        self.messages
                            .push_back(serde_json::from_str(&frame).unwrap());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("TCP read failed: {error}"),
            }
            assert!(Instant::now() < deadline, "Boolean TCP test timed out");
            thread::yield_now();
        }
    }

    fn request(&mut self, request: serde_json::Value) -> serde_json::Value {
        let id = self.send(request);
        let response = self.next_message();
        assert_eq!(response["id"], id);
        response
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.server.take().unwrap().join().unwrap().unwrap();
    }
}

#[test]
fn boolean_tcp_describe_set_and_get_preserve_types() {
    let mut fixture = Fixture::new();
    assert_eq!(
        fixture.request(json!({"op":"hello"})),
        json!({
            "id":1, "status":"ok", "protocol":"lab-server", "version":1,
        })
    );
    assert_eq!(
        fixture.request(json!({"op":"describe"})),
        json!({
            "id":2, "status":"ok", "actions":[], "parameters":[
                {"key":"switch/enabled", "type":"boolean", "min":null, "max":null, "unit":"", "writable":true, "poll_period_ns":null},
                {"key":"switch/fault", "type":"boolean", "min":null, "max":null, "unit":"", "writable":false, "poll_period_ns":10_000_000},
            ],
        })
    );

    for value in [true, false] {
        let response = fixture.request(json!({"op":"set", "key":"switch/enabled", "value":value}));
        assert_eq!(response, json!({"id":response["id"], "status":"ok"}));
        let mut response = fixture.request(json!({"op":"get", "key":"switch/enabled"}));
        assert!(response["completed_at_unix_ms"].as_u64().is_some());
        response
            .as_object_mut()
            .unwrap()
            .remove("completed_at_unix_ms");
        assert_eq!(
            response,
            json!({"id":response["id"], "status":"ok", "key":"switch/enabled", "value":value, "unit":""})
        );
    }
    assert_eq!(
        *fixture.state.writes.lock().unwrap(),
        vec![
            ("enabled".into(), Value::Boolean(true)),
            ("enabled".into(), Value::Boolean(false)),
        ]
    );
}

#[test]
fn boolean_tcp_rejects_invalid_writes_before_driver_and_consumes_ids() {
    let mut fixture = Fixture::new();
    for value in [
        json!(1),
        json!(0),
        json!(1.0),
        json!(0.0),
        json!("true"),
        json!("false"),
        json!(null),
    ] {
        let expected_error = if value.is_number() {
            "Value does not match parameter spec"
        } else {
            "invalid_value"
        };
        let response = fixture.request(json!({"op":"set", "key":"switch/enabled", "value":value}));
        assert_eq!(
            response,
            json!({"id":response["id"], "status":"error", "error":expected_error})
        );
        writeln!(
            fixture.client,
            "{}",
            json!({"id":response["id"], "op":"set", "key":"switch/enabled", "value":true})
        )
        .unwrap();
        assert_eq!(
            fixture.next_message(),
            json!({"status":"error", "error":"duplicate_id"})
        );
        assert!(fixture.state.writes.lock().unwrap().is_empty());
    }
    let response = fixture.request(json!({"op":"set", "key":"switch/fault", "value":true}));
    assert_eq!(response["error"], "Parameter is read-only");
    assert!(fixture.state.writes.lock().unwrap().is_empty());
}

#[test]
fn boolean_tcp_subscription_ack_precedes_polled_values_and_read_errors() {
    let mut fixture = Fixture::new();
    let id = fixture.send(json!({"op":"subscribe", "keys":["switch/fault"]}));
    assert_eq!(
        fixture.next_message(),
        json!({"id":id, "status":"ok", "keys":["switch/fault"]})
    );
    let mut seen = [false; 3];
    for _ in 0..3 {
        let mut event = fixture.next_message();
        assert!(event["completed_at_unix_ms"].as_u64().is_some());
        event
            .as_object_mut()
            .unwrap()
            .remove("completed_at_unix_ms");
        if let Some(value) = event["value"].as_bool() {
            seen[usize::from(value)] = true;
            assert_eq!(
                event,
                json!({"event":"measurement", "status":"ok", "key":"switch/fault", "value":value, "unit":""})
            );
        } else {
            seen[2] = true;
            assert_eq!(
                event,
                json!({"event":"measurement", "status":"error", "key":"switch/fault", "error":"simulated read failure"})
            );
        }
    }
    assert_eq!(seen, [true; 3]);

    // Cached get uses the same Boolean/error representation as the polled stream.
    let id = fixture.send(json!({"op":"get", "key":"switch/fault"}));
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < deadline, "cached Boolean get timed out");
        let response = fixture.next_message();
        if response.get("id") == Some(&json!(id)) {
            assert_eq!(response["key"], "switch/fault");
            assert!(response["completed_at_unix_ms"].as_u64().is_some());
            if response["status"] == "ok" {
                assert!(response["value"].is_boolean());
                assert_eq!(response["unit"], "");
            } else {
                assert_eq!(response["status"], "error");
                assert_eq!(response["error"], "simulated read failure");
                assert!(response.get("value").is_none());
                assert!(response.get("unit").is_none());
            }
            break;
        }
        assert_eq!(response["event"], "measurement");
    }
}

#[test]
fn boolean_tcp_write_failure_reports_unknown_outcome() {
    let mut fixture = Fixture::new();
    fixture.state.fail_write.store(true, Ordering::SeqCst);
    assert_eq!(
        fixture.request(json!({"op":"set", "key":"switch/enabled", "value":true})),
        json!({
            "id":1, "status":"error", "error":"outcome_unknown",
        })
    );
    assert_eq!(
        *fixture.state.writes.lock().unwrap(),
        vec![("enabled".into(), Value::Boolean(true))]
    );
    assert_eq!(
        fixture.request(json!({"op":"get", "key":"switch/enabled"}))["value"],
        true
    );
    // A subsequent request has its own reply, without another reply to the Set.
    assert_eq!(
        fixture.request(json!({"op":"ping"})),
        json!({"id":3, "status":"ok"})
    );
}
