//! Public shared service/session boundary, without hardware or socket timing.
use std::collections::HashMap;
use std::io::{self, BufReader, Cursor, Write};
use std::sync::{Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, UNIX_EPOCH};

use lab_server::command_router::CommandRouter;
use lab_server::connection::ClientSession;
use lab_server::device_registry::DeviceRegistry;
use lab_server::driver::{DeviceIo, Driver, DriverError};
use lab_server::model::{Observation, ParameterDescription, Spec, Value};
use lab_server::observation_aggregator::AggregatorEvent;
use lab_server::outbound::{OutboundBuffer, OutputError};
use lab_server::pending::{Completion, Pending, Poll};
use lab_server::port_worker::{
    DeviceInstance, DeviceObservation, PortHandle, PortWorker, StopReason,
};
use lab_server::service::DeviceService;
use lab_server::session::Session;
use serde_json::{Value as Json, json};

const WAIT: Duration = Duration::from_secs(10);

struct ControlledDriver {
    entered: mpsc::Sender<Value>,
    release: mpsc::Receiver<()>,
}
impl Driver for ControlledDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        [
            ("power", Spec::Integer { min: 0, max: 100 }, true, None),
            (
                "temperature",
                Spec::Float {
                    min: -100.0,
                    max: 500.0,
                },
                false,
                Some(Duration::from_secs(3600)),
            ),
        ]
        .into_iter()
        .map(
            |(id, spec, writable, default_poll_period)| ParameterDescription {
                id: id.into(),
                spec,
                writable,
                default_poll_period,
                unit: "".into(),
            },
        )
        .collect()
    }
    fn read(&mut self, parameter: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        match parameter {
            "power" => Ok(Value::Integer(30)),
            "temperature" => Ok(Value::Float(20.0)),
            _ => panic!("driver received a nonlocal key"),
        }
    }
    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        _: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        assert_eq!(parameter, "power");
        self.entered.send(value.clone()).unwrap();
        self.release.recv_timeout(WAIT).unwrap();
        Ok(())
    }
}

struct Context {
    service: DeviceService,
    handle: PortHandle,
    worker: Option<JoinHandle<StopReason>>,
    // Keep the worker's initial poll alive; stream tests inject aggregator events explicitly.
    _observations: mpsc::Receiver<DeviceObservation>,
    entered: mpsc::Receiver<Value>,
    release: mpsc::Sender<()>,
    // Only the raw TCP framing adapter needs these explicit dispatch arguments.
    catalog: HashMap<String, ParameterDescription>,
    latest: Mutex<HashMap<String, Observation>>,
}
impl Context {
    fn new() -> Self {
        Self::with_capacity(8)
    }
    fn with_capacity(capacity: usize) -> Self {
        let (entered_tx, entered) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let device = DeviceInstance::new(
            "furnace1",
            ControlledDriver {
                entered: entered_tx,
                release: release_rx,
            },
        )
        .unwrap();
        let (worker, channels) = PortWorker::new(
            BufReader::new(Cursor::new(Vec::<u8>::new())),
            vec![device],
            8,
            8,
        )
        .unwrap();
        let mut registry = DeviceRegistry::new();
        registry
            .register_port("p", channels.commands.clone())
            .unwrap();
        registry.register_device("p", &worker.devices()[0]).unwrap();
        let catalog = registry
            .parameters()
            .map(|p| {
                let mut description = p.description.clone();
                description.id = p.global_key.clone();
                (description.id.clone(), description)
            })
            .collect();
        let service = DeviceService::new(CommandRouter::new(registry), capacity);
        Self {
            service,
            handle: channels.commands,
            worker: Some(thread::spawn(move || worker.run())),
            _observations: channels.observations,
            entered,
            release,
            catalog,
            latest: Mutex::new(HashMap::new()),
        }
    }
    fn send(&self, session: &mut Session, text: &str, now: Instant) {
        self.service.receive_message(session, text, now).unwrap();
    }
    fn started(&self) {
        assert_eq!(self.entered.recv_timeout(WAIT).unwrap(), Value::Integer(30));
    }
    fn complete(&self) {
        self.release.send(()).unwrap();
        // The following read can finish only after the preceding Set reply was sent.
        self.barrier();
    }
    fn barrier(&self) {
        let pending = self
            .handle
            .read(99, "furnace1", "power", Instant::now() + WAIT)
            .unwrap();
        assert!(received(pending).is_ok());
    }
    fn publish(&self, value: f64) {
        self.service
            .apply_event(AggregatorEvent::Measurement {
                port_id: "p".into(),
                global_key: "furnace1/temperature".into(),
                observation: Observation {
                    source: "temperature".into(),
                    unit: "".into(),
                    metadata: None,
                    completed_at: UNIX_EPOCH,
                    reading: Ok(Value::Float(value)),
                },
            })
            .unwrap();
    }
    fn lose(&self) {
        self.service
            .apply_event(AggregatorEvent::DataLost {
                port_id: "p".into(),
                loss_epoch: 1,
            })
            .unwrap();
    }
    fn close(&mut self) {
        self.handle.shutdown();
        let reason = self.worker.take().unwrap().join().unwrap();
        assert_eq!(reason, StopReason::Shutdown);
        self.service
            .apply_event(AggregatorEvent::PortStopped {
                port_id: "p".into(),
                reason,
            })
            .unwrap();
    }
}
impl Drop for Context {
    fn drop(&mut self) {
        self.handle.shutdown();
        let _ = self.release.send(());
        if let Some(worker) = self.worker.take() {
            assert_eq!(worker.join().unwrap(), StopReason::Shutdown);
        }
    }
}
fn received<T>(mut pending: Pending<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        match pending.poll(Instant::now()) {
            Poll::Waiting(next) => pending = next,
            Poll::Finished(Completion::Received { value, .. }) => return value,
            _ => panic!("worker failed to finish barrier"),
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
}

fn messages(session: &mut Session) -> Vec<Json> {
    let mut result = Vec::new();
    while let Some(text) = session.next_message().unwrap() {
        assert!(!text.contains('\n'));
        result.push(serde_json::from_str(&text).unwrap());
    }
    result
}

#[test]
fn complete_json_text_has_no_ndjson_dependency_and_uses_the_existing_parser() {
    let context = Context::new();
    let mut session = Session::new(64, 64 * 1024);
    let mut tcp = ClientSession::new(64, 64 * 1024);
    let now = Instant::now();
    for text in [
        r#"{"id":1,"op":"hello"}"#,
        r#"{"id":2,"op":"describe"}"#,
        "not-json",
        "[]",
        r#"{"id":0,"op":"ping"}"#,
        r#"{"id":3,"op":"get"}"#,
        r#"{"id":3,"op":"ping"}"#,
        r#"{"id":4,"id":5,"op":"ping"}"#,
        r#"{"id":4,"op":"ping"} {}"#,
        r#"{"id":4,"op":"get","key":"температура"}"#,
        r#"{"id":5,"op":"get","key":"furnace1/temperature"}"#,
    ] {
        context.send(&mut session, text, now);
        for incoming in tcp.receive(format!("{text}\r\n").as_bytes()) {
            tcp.dispatch_incoming(
                incoming,
                &context.service,
                &context.catalog,
                &context.latest,
                now,
            )
            .unwrap();
        }
        let direct = session.next_message().unwrap().unwrap();
        let mut wire = Vec::new();
        tcp.outbound.flush_to(&mut wire).unwrap();
        assert_eq!(wire, format!("{direct}\n").as_bytes());
        assert!(session.next_message().unwrap().is_none());
    }
    context.send(&mut session, "{\n \"id\":6,\n \"op\":\"ping\"\n}", now);
    assert_eq!(messages(&mut session), [json!({"id":6,"status":"ok"})]);
    assert!(context.entered.try_recv().is_err());
}

#[test]
fn one_session_uses_the_current_catalog_after_component_creation_and_deletion() {
    let context = Context::new();
    let mut session = Session::new(8, 64 * 1024);
    let now = Instant::now();
    let mut request = |request: Json| {
        context.send(&mut session, &request.to_string(), now);
        let [reply]: [Json; 1] = messages(&mut session).try_into().unwrap();
        reply
    };
    let key = "processing/smooth/value";
    let create = json!({
        "action":"create",
        "expected_revision":0,
        "configuration":{
            "name":"smooth","type_id":"ema",
            "inputs":{"input":"furnace1/temperature"},
            "parameters":{},"max_age_ms":1000
        }
    });
    let delete = json!({"action":"delete","name":"smooth","expected_revision":1});

    for (id, command, present) in [(1, create, true), (5, delete, false)] {
        assert_eq!(
            request(json!({"id":id,"op":"components","command":command}))["status"],
            "ok"
        );
        let catalog = request(json!({"id":id+1,"op":"describe"}));
        assert_eq!(
            catalog["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["key"] == key),
            present
        );
        let get = request(json!({"id":id+2,"op":"get","key":key}));
        let subscribe = request(json!({"id":id+3,"op":"subscribe","keys":[key]}));
        if present {
            assert_eq!(get, json!({"id":id+2,"status":"no_data","key":key}));
            assert_eq!(subscribe, json!({"id":id+3,"status":"ok","keys":[key]}));
        } else {
            assert_eq!(
                get,
                json!({"id":id+2,"status":"error","error":"unknown_key"})
            );
            assert_eq!(
                subscribe,
                json!({"id":id+3,"status":"error","error":"unknown_key","key":key})
            );
        }
    }
}

#[test]
fn get_and_describe_follow_rewire_and_source_shutdown() {
    use lab_server::model::SourceMetadata;

    let mut context = Context::new();
    let request = |service: &DeviceService, text: &str| {
        let mut session = Session::new(1, 16 * 1024);
        service
            .receive_message(&mut session, text, Instant::now())
            .unwrap();
        let [reply]: [Json; 1] = messages(&mut session).try_into().unwrap();
        reply
    };
    let physical_get = r#"{"id":1,"op":"get","key":"furnace1/temperature"}"#;
    let derived_get = r#"{"id":1,"op":"get","key":"processing/smooth/value"}"#;
    assert_eq!(
        request(&context.service, r#"{"id":1,"op":"ping"}"#),
        json!({"id":1,"status":"ok"})
    );
    assert_eq!(
        request(&context.service, r#"{"id":1,"op":"hello"}"#)["status"],
        "ok"
    );
    assert_eq!(request(&context.service, "{")["error"], "invalid_json");
    assert_eq!(request(&context.service, physical_get)["status"], "no_data");

    let configure = |service: &DeviceService, action, name, input, revision| {
        service.manage(serde_json::from_value(json!({
            "action":action,"expected_revision":revision,
            "configuration":{"name":name,"type_id":"ema","inputs":{"input":input},"max_age_ms":10000}
        })).unwrap()).unwrap();
    };
    configure(
        &context.service,
        "create",
        "smooth",
        "furnace1/temperature",
        0,
    );
    configure(
        &context.service,
        "create",
        "bridge",
        "furnace1/temperature",
        1,
    );
    assert_eq!(request(&context.service, derived_get)["status"], "no_data");
    let start = Instant::now();
    let sample = |context: &Context, sequence, acquired_at, value| {
        context
            .service
            .apply_event(AggregatorEvent::Measurement {
                port_id: "p".into(),
                global_key: "furnace1/temperature".into(),
                observation: Observation {
                    source: "temperature".into(),
                    unit: "".into(),
                    completed_at: UNIX_EPOCH + Duration::from_secs(sequence),
                    metadata: Some(SourceMetadata {
                        source_instance: context.handle.source_instance(),
                        sequence,
                        generation: 0,
                        acquired_at,
                        elapsed: acquired_at.duration_since(start),
                    }),
                    reading: Ok(Value::Float(value)),
                },
            })
            .unwrap();
    };
    sample(&context, 1, start, 12.0);
    assert_eq!(request(&context.service, physical_get)["value"], 12.0);
    assert_eq!(request(&context.service, derived_get)["value"], 12.0);
    configure(
        &context.service,
        "configure",
        "smooth",
        "processing/bridge/value",
        2,
    );
    assert_eq!(request(&context.service, derived_get)["status"], "no_data");
    sample(&context, 2, start + Duration::from_secs(1), 27.0);
    let current = request(&context.service, derived_get);
    assert!(current["value"].as_f64().unwrap() > 12.0);
    assert_eq!(current["completed_at_unix_ms"], 2000);

    let description = request(&context.service, r#"{"id":1,"op":"describe"}"#);
    let keys: Vec<_> = description["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|parameter| parameter["key"].as_str().unwrap())
        .collect();
    assert_eq!(
        keys,
        [
            "furnace1/power",
            "furnace1/temperature",
            "processing/bridge/value",
            "processing/smooth/value"
        ]
    );

    context.close();
    assert_eq!(
        request(&context.service, physical_get)["error"],
        "runtime_unavailable"
    );
    assert_eq!(request(&context.service, derived_get)["status"], "error");
    context
        .service
        .manage(
            serde_json::from_value(json!({
                "action":"delete","name":"smooth","expected_revision":3
            }))
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        request(&context.service, derived_get)["error"],
        "unknown_key"
    );
}

#[test]
fn cached_get_calls_custom_availability_with_latest_locked_and_uses_the_base_catalog() {
    use lab_server::session::{CommandBackend, EnqueueError, PendingRequests};

    struct InspectLatest<'a>(&'a Mutex<HashMap<String, Observation>>);
    impl CommandBackend for InspectLatest<'_> {
        fn available(&self, key: &str) -> bool {
            assert_eq!(key, "furnace1/temperature");
            assert!(matches!(
                self.0.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            true
        }
        fn enqueue_read(
            &self,
            _: &mut PendingRequests,
            _: u64,
            _: Instant,
            _: ParameterDescription,
        ) -> Result<(), EnqueueError> {
            panic!("cached get must not enqueue a read")
        }
        fn enqueue_set(
            &self,
            _: &mut PendingRequests,
            _: u64,
            _: Instant,
            _: String,
            _: Value,
        ) -> Result<(), EnqueueError> {
            panic!("get must not enqueue a write")
        }
    }

    let context = Context::new();
    let backend = InspectLatest(&context.latest);
    let mut session = Session::new(1, 4096);
    session
        .receive_message(
            r#"{"id":18446744073709551615,"op":"get","key":"furnace1/temperature"}"#,
            &backend,
            &context.catalog,
            &context.latest,
            Instant::now(),
        )
        .unwrap();
    assert_eq!(
        messages(&mut session),
        [json!({"id":u64::MAX,"status":"no_data","key":"furnace1/temperature"})]
    );
    assert!(context.latest.try_lock().is_ok());
}

#[test]
fn message_queue_keeps_utf8_boundaries_and_the_exact_64k_lf_budget() {
    let mut queue = OutboundBuffer::new(64 * 1024);
    let text = format!("\"{}\"", "я".repeat(32766)); // 65534 UTF-8 bytes
    assert_eq!(text.len(), 65534);
    queue.enqueue_message(&text).unwrap();
    assert_eq!(queue.queued_bytes(), 65535);
    assert_eq!(queue.enqueue_message("0"), Err(OutputError::Full));
    assert_eq!(queue.pop_message().unwrap(), Some(text));
    assert_eq!(queue.queued_bytes(), 0);
    queue.enqueue_message(&"x".repeat(65535)).unwrap();
    assert_eq!(queue.queued_bytes(), 65536);
    assert_eq!(queue.enqueue_message("0"), Err(OutputError::Full));
    assert_eq!(queue.pop_message().unwrap().unwrap().len(), 65535);
    assert!(queue.pop_message().unwrap().is_none());
    assert_eq!(
        queue.enqueue_message(&"x".repeat(65536)),
        Err(OutputError::Full)
    );
    assert!(queue.is_empty());
}

#[test]
fn tcp_compatibility_framing_keeps_valid_prefix_before_terminal_error() {
    let context = Context::new();
    let now = Instant::now();
    for (tail, code) in [
        (b"\n".to_vec(), "empty_frame"),
        (vec![0xff, b'\n'], "invalid_utf8"),
        (vec![b'x'; 16 * 1024 + 1], "frame_too_long"),
    ] {
        let prefix = "{\"id\":1,\"op\":\"get\",\"key\":\"я\"}\r\n{\"id\":2,\"op\":\"ping\"}\n";
        let mut input = prefix.as_bytes().to_vec();
        input.extend(tail);
        let mut outputs = Vec::new();
        for one_byte_reads in [false, true] {
            let mut session = ClientSession::new(64, 4096);
            let mut events = Vec::new();
            if one_byte_reads {
                for byte in &input {
                    events.extend(session.receive(&[*byte]));
                }
            } else {
                events.extend(session.receive(&input));
            }
            for event in events {
                session
                    .dispatch_incoming(
                        event,
                        &context.service,
                        &context.catalog,
                        &context.latest,
                        now,
                    )
                    .unwrap();
            }
            assert!(session.is_terminal());
            assert!(session.receive(b"{\"id\":3,\"op\":\"ping\"}\n").is_empty());
            let mut wire = Vec::new();
            session.outbound.flush_to(&mut wire).unwrap();
            outputs.push(String::from_utf8(wire).unwrap());
        }
        assert_eq!(outputs[0], outputs[1]);
        let replies: Vec<Json> = outputs[0]
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(replies.len(), 3);
        assert_eq!(replies[0]["error"], "unknown_key");
        assert_eq!(replies[1], json!({"id":2,"status":"ok"}));
        assert_eq!(replies[2], json!({"status":"error","error":code}));
    }
}

struct BudgetWriter {
    budget: usize,
    bytes: Vec<u8>,
}

impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.budget == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = self.budget.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..n]);
        self.budget -= n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn tcp_partial_writes_release_only_written_bytes_and_cannot_replay_a_prefix() {
    let mut queue = OutboundBuffer::new(8);
    queue.enqueue_message("\"я\"").unwrap(); // 4 payload bytes + LF
    let mut writer = BudgetWriter {
        budget: 2,
        bytes: Vec::new(),
    };
    queue.flush_to(&mut writer).unwrap(); // cut inside UTF-8, not a message boundary
    assert_eq!(queue.queued_bytes(), 3);
    assert_eq!(
        queue.pop_message().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    queue.enqueue_message("null").unwrap(); // capacity reclaimed by partial write
    assert_eq!(queue.queued_bytes(), 8);
    assert_eq!(queue.enqueue_message("0"), Err(OutputError::Full));
    writer.budget = 2;
    queue.flush_to(&mut writer).unwrap(); // body complete, LF still pending
    assert_eq!(queue.queued_bytes(), 6);
    assert!(queue.pop_message().is_err());
    writer.budget = 1;
    queue.flush_to(&mut writer).unwrap();
    assert_eq!(writer.bytes, "\"я\"\n".as_bytes());
    assert_eq!(queue.queued_bytes(), 5);
    writer.budget = 5;
    queue.flush_to(&mut writer).unwrap();
    assert_eq!(writer.bytes, "\"я\"\nnull\n".as_bytes());
    assert!(queue.is_empty());
    assert_eq!(queue.queued_bytes(), 0);
    assert!(queue.pop_message().unwrap().is_none());
}

#[test]
fn output_overflow_preserves_queued_prefix_and_terminates_message_session() {
    let context = Context::new();
    let mut session = Session::new(64, 23); // exactly one ping reply + LF
    context.send(&mut session, r#"{"id":1,"op":"ping"}"#, Instant::now());
    let error = session
        .receive_message(
            r#"{"id":2,"op":"ping"}"#,
            &context.service,
            &context.catalog,
            &context.latest,
            Instant::now(),
        )
        .unwrap_err();
    assert_eq!(
        error.get_ref().unwrap().downcast_ref::<OutputError>(),
        Some(&OutputError::Full)
    );
    assert!(session.is_terminal());
    assert!(!session.is_finished());
    context.send(&mut session, r#"{"id":3,"op":"ping"}"#, Instant::now());
    assert_eq!(messages(&mut session), [json!({"id":1,"status":"ok"})]);
    assert!(session.is_finished());
}

#[test]
fn concurrent_message_session_orders_subscription_ack_completion_and_measurement() {
    let context = Context::new();
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"set","key":"furnace1/power","value":30}"#,
        now,
    );
    context.started();
    context.publish(-1.0); // before activation, excluded
    context.send(
        &mut session,
        r#"{"id":2,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.send(&mut session, r#"{"id":3,"op":"ping"}"#, now);
    context.publish(25.0);
    context.complete(); // worker barrier proves completion is ready
    session.collect_ready(now).unwrap();
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 4);
    assert_eq!(
        output[0],
        json!({"id":2,"status":"ok","keys":["furnace1/temperature"]})
    );
    assert_eq!(output[1], json!({"id":3,"status":"ok"}));
    assert_eq!(output[2], json!({"id":1,"status":"ok"}));
    assert_eq!(output[3]["event"], "measurement");
    assert_eq!(output[3]["value"], 25.0);
    session.collect_ready(now).unwrap();
    assert!(messages(&mut session).is_empty());
}

#[test]
fn unsubscribe_and_reactivation_exclude_queued_old_publications() {
    let context = Context::new();
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.publish(1.0);
    context.send(
        &mut session,
        r#"{"id":2,"op":"unsubscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    session.collect_observations(&receiver).unwrap();
    context.publish(2.0);
    context.send(
        &mut session,
        r#"{"id":3,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.publish(3.0);
    // Idempotent subscribe must not move the retained key's boundary past 3.0.
    context.send(
        &mut session,
        r#"{"id":4,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 5);
    assert_eq!(output[4]["value"], 3.0);
    assert!(
        output[..4]
            .iter()
            .all(|reply| reply["status"] == "ok" && reply.get("event").is_none())
    );
}

#[test]
fn message_receiver_overflow_is_sticky_across_resubscription() {
    let context = Context::with_capacity(1);
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.publish(1.0);
    context.publish(2.0);
    context.send(
        &mut session,
        r#"{"id":2,"op":"unsubscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.send(
        &mut session,
        r#"{"id":3,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 4);
    assert_eq!(
        output[3],
        json!({"status":"error","error":"observation_data_lost"})
    );
    assert!(session.is_finished());
    session.collect_observations(&receiver).unwrap();
    assert!(messages(&mut session).is_empty());
}

#[test]
fn orderly_observation_close_drains_prefix_and_differs_from_data_loss() {
    let mut context = Context::new();
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        Instant::now(),
    );
    context.publish(25.0);
    context.close();
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 3);
    assert_eq!(output[1]["value"], 25.0);
    assert_eq!(output[2]["error"], "observation_stream_closed");
    assert!(session.is_finished());
}

#[test]
fn observation_service_is_bounded_to_one_turn() {
    let context = Context::with_capacity(64);
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 64 * 1024);
    session.bind_observations(&receiver);
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        Instant::now(),
    );
    messages(&mut session);
    for n in 0..40 {
        context.publish(f64::from(n));
    }
    session.collect_observations(&receiver).unwrap();
    let first = messages(&mut session);
    assert_eq!(first.len(), 32);
    session.collect_observations(&receiver).unwrap();
    let tail = messages(&mut session);
    assert_eq!(tail.len(), 8);
    assert_eq!(first[31]["value"], 31.0);
    assert_eq!(tail[0]["value"], 32.0);
}

#[test]
fn concurrent_message_eof_preserves_queued_cancellation_and_started_outcome_unknown() {
    let context = Context::new();
    let mut session = Session::new(8, 4096);
    let now = Instant::now();
    for id in 1..=2 {
        context.send(
            &mut session,
            &json!({"id":id,"op":"set","key":"furnace1/power","value":30}).to_string(),
            now,
        );
    }
    context.started(); // FIFO: request 1 has started; request 2 is queued.
    session.close_input();
    assert!(!session.is_finished());
    context.send(&mut session, r#"{"id":3,"op":"ping"}"#, now);
    session
        .collect_ready(now + Duration::from_secs(11))
        .unwrap();
    assert_eq!(
        messages(&mut session),
        [
            json!({"id":1,"status":"error","error":"outcome_unknown"}),
            json!({"id":2,"status":"error","error":"runtime_timeout"}),
        ]
    );
    assert!(session.is_finished());
    context.complete();
    assert!(
        context.entered.try_recv().is_err(),
        "cancelled request must not reach driver"
    );
    session
        .collect_ready(now + Duration::from_secs(12))
        .unwrap();
    assert!(messages(&mut session).is_empty());
}

#[test]
fn clean_message_eof_keeps_final_ack_and_stops_observation_admission() {
    let context = Context::new();
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.send(
        &mut session,
        r#"{"id":2,"op":"set","key":"furnace1/power","value":30}"#,
        now,
    );
    context.started();
    session.close_input();
    context.publish(25.0);
    session.collect_observations(&receiver).unwrap();
    context.complete();
    session.collect_ready(now).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 2);
    assert_eq!(output[1], json!({"id":2,"status":"ok"}));
    assert!(session.is_finished());
}

#[test]
fn terminal_loss_does_not_fabricate_pending_ack_or_block_worker_after_drop() {
    let context = Context::with_capacity(1);
    let receiver = context.service.register_client().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"set","key":"furnace1/power","value":30}"#,
        now,
    );
    context.started();
    context.send(
        &mut session,
        r#"{"id":2,"op":"subscribe","keys":["furnace1/temperature"]}"#,
        now,
    );
    context.lose();
    session.collect_observations(&receiver).unwrap();
    session
        .collect_ready(now + Duration::from_secs(11))
        .unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 2);
    assert_eq!(output[1]["error"], "observation_data_lost");
    assert!(session.is_finished());
    drop(session);
    context.complete(); // abandoned reply cannot block subsequent commands
}
