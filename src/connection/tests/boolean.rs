use super::*;
use crate::driver::{DeviceIo, Driver, DriverError, PortState};
use crate::fanout::{ObservationFanout, ObservationReceiver};
use crate::model::Spec;
use crate::runtime::{self, DeviceCommand, ObservationOutput, PollTask};
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
            Err("simulated write reply loss".into())
        } else {
            Ok(())
        }
    }
}

// Exercise the real runtime, fanout and TCP connection with in-memory device I/O.
// The test thread forwards observations exactly as main does, then pumps TCP.
struct Fixture {
    connection: TcpConnection,
    client: TcpStream,
    decoder: FrameDecoder,
    messages: VecDeque<serde_json::Value>,
    commands: mpsc::SyncSender<DeviceCommand>,
    catalog: HashMap<String, ParameterDescription>,
    latest: Mutex<HashMap<String, Observation>>,
    fanout: ObservationFanout,
    observations: ObservationReceiver,
    runtime_observations: Option<mpsc::Receiver<Observation>>,
    worker: Option<JoinHandle<()>>,
    state: Arc<DriverState>,
    next_id: u64,
}

impl Fixture {
    fn new() -> Self {
        let state = Arc::new(DriverState::default());
        let driver = BooleanDriver {
            state: state.clone(),
            enabled: false,
            fault_reads: 0,
        };
        let parameters = driver.parameters();
        let catalog = parameters
            .iter()
            .map(|p| (p.id.clone(), p.clone()))
            .collect();
        let tasks = parameters
            .into_iter()
            .filter_map(|description| {
                Some(PollTask {
                    period: description.default_poll_period?,
                    source: description.id.clone(),
                    description,
                })
            })
            .collect();
        let fanout = ObservationFanout::new(128);
        let observations = fanout.register().unwrap();
        let (tx, rx) = mpsc::sync_channel(runtime::OBSERVATION_QUEUE_CAPACITY);
        let output = ObservationOutput::new(tx, fanout.loss_epoch());
        let (commands, command_rx) = mpsc::sync_channel(runtime::COMMAND_QUEUE_CAPACITY);
        let worker = thread::spawn(move || {
            runtime::run_polling(
                driver,
                BufReader::new(Cursor::new(Vec::<u8>::new())),
                tasks,
                output,
                command_rx,
            );
        });
        let (mut connection, client) = loopback_pair();
        connection.session.outbound = OutboundBuffer::new(64 * 1024);
        client.set_nonblocking(true).unwrap();
        Self {
            connection,
            client,
            decoder: FrameDecoder::default(),
            messages: VecDeque::new(),
            commands,
            catalog,
            latest: Mutex::new(HashMap::new()),
            fanout,
            observations,
            runtime_observations: Some(rx),
            worker: Some(worker),
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
            for observation in self.runtime_observations.as_ref().unwrap().try_iter() {
                self.latest
                    .lock()
                    .unwrap()
                    .insert(observation.source.clone(), observation.clone());
                self.fanout.publish(&observation).unwrap();
            }
            assert!(
                !self
                    .connection
                    .drive_once_with_observations(
                        &self.commands,
                        &self.catalog,
                        &self.latest,
                        &self.observations,
                    )
                    .unwrap()
            );
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
            thread::sleep(Duration::from_millis(1));
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
        // Closing the observation receiver stops polling on its next publication.
        drop(self.runtime_observations.take());
        self.worker.take().unwrap().join().unwrap();
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
                {"key":"enabled", "type":"boolean", "min":null, "max":null, "unit":"", "writable":true, "poll_period_ns":null},
                {"key":"fault", "type":"boolean", "min":null, "max":null, "unit":"", "writable":false, "poll_period_ns":10_000_000},
            ],
        })
    );

    for value in [true, false] {
        let response = fixture.request(json!({"op":"set", "key":"enabled", "value":value}));
        assert_eq!(response, json!({"id":response["id"], "status":"ok"}));
        let mut response = fixture.request(json!({"op":"get", "key":"enabled"}));
        assert!(response["completed_at_unix_ms"].as_u64().is_some());
        response
            .as_object_mut()
            .unwrap()
            .remove("completed_at_unix_ms");
        assert_eq!(
            response,
            json!({"id":response["id"], "status":"ok", "key":"enabled", "value":value, "unit":""})
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
        let response = fixture.request(json!({"op":"set", "key":"enabled", "value":value}));
        assert_eq!(
            response,
            json!({"id":response["id"], "status":"error", "error":expected_error})
        );
        writeln!(
            fixture.client,
            "{}",
            json!({"id":response["id"], "op":"set", "key":"enabled", "value":true})
        )
        .unwrap();
        assert_eq!(
            fixture.next_message(),
            json!({"status":"error", "error":"duplicate_id"})
        );
        assert!(fixture.state.writes.lock().unwrap().is_empty());
    }
    let response = fixture.request(json!({"op":"set", "key":"fault", "value":true}));
    assert_eq!(response["error"], "Parameter is read-only");
    assert!(fixture.state.writes.lock().unwrap().is_empty());
}

#[test]
fn boolean_tcp_subscription_ack_precedes_polled_values_and_read_errors() {
    let mut fixture = Fixture::new();
    let id = fixture.send(json!({"op":"subscribe", "keys":["fault"]}));
    assert_eq!(
        fixture.next_message(),
        json!({"id":id, "status":"ok", "keys":["fault"]})
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
                json!({"event":"measurement", "status":"ok", "key":"fault", "value":value, "unit":""})
            );
        } else {
            seen[2] = true;
            assert_eq!(
                event,
                json!({"event":"measurement", "status":"error", "key":"fault", "error":"simulated read failure"})
            );
        }
    }
    assert_eq!(seen, [true; 3]);

    // Cached get uses the same Boolean/error representation as the polled stream.
    let id = fixture.send(json!({"op":"get", "key":"fault"}));
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < deadline, "cached Boolean get timed out");
        let response = fixture.next_message();
        if response.get("id") == Some(&json!(id)) {
            assert_eq!(response["key"], "fault");
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
        fixture.request(json!({"op":"set", "key":"enabled", "value":true})),
        json!({
            "id":1, "status":"error", "error":"outcome_unknown",
        })
    );
    assert_eq!(
        *fixture.state.writes.lock().unwrap(),
        vec![("enabled".into(), Value::Boolean(true))]
    );
    assert_eq!(
        fixture.request(json!({"op":"get", "key":"enabled"}))["value"],
        true
    );
    // A subsequent request has its own reply, without another reply to the Set.
    assert_eq!(
        fixture.request(json!({"op":"ping"})),
        json!({"id":3, "status":"ok"})
    );
}
