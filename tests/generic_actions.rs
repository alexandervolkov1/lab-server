//! Actions use only memory I/O; gates make timeout/start ordering deterministic.
use std::io::{self, BufRead, Cursor, Read};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use lab_server::command_router::{CommandRouter, RouteError};
use lab_server::device_registry::DeviceRegistry;
use lab_server::driver::{DeviceIo, Driver, DriverError, PortState};
use lab_server::model::{ActionDescription, ParameterDescription, Spec, Value};
use lab_server::pending::{Completion, Pending, Poll};
use lab_server::port_worker::{CommandError, DeviceInstance, PortChannels, PortWorker, StopReason};
use lab_server::service::DeviceService;
use lab_server::session::Session;
use serde_json::{Value as Json, json};

const WAIT: Duration = Duration::from_secs(10);
type Trace = Arc<Mutex<Vec<(String, String, usize)>>>;

struct MemoryIo(Cursor<Vec<u8>>);
impl Read for MemoryIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}
impl BufRead for MemoryIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.0.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.0.consume(n);
    }
}
impl DeviceIo for MemoryIo {
    fn send(&mut self, _: &[u8]) -> io::Result<()> {
        assert_eq!(
            self.0.position(),
            self.0.get_ref().len() as u64,
            "interleaved I/O"
        );
        self.0 = Cursor::new(b"ok\n".to_vec());
        Ok(())
    }
}
struct Gate {
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
fn gate() -> (Gate, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, rx) = mpsc::channel();
    let (tx, resume) = mpsc::channel();
    (Gate { entered, resume }, rx, tx)
}
struct MockDriver {
    name: String,
    trace: Trace,
    names: Vec<String>,
    gate: Option<Gate>,
    failure: Option<DriverError>,
    panic: bool,
    value: bool,
}
impl MockDriver {
    fn new(name: &str, trace: &Trace) -> Self {
        Self {
            name: name.into(),
            trace: trace.clone(),
            names: vec!["calibrate".into(), "nested/run".into(), "value".into()],
            gate: None,
            failure: None,
            panic: false,
            value: false,
        }
    }
    fn transaction(&self, name: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.trace.lock().unwrap().push((
            self.name.clone(),
            name.into(),
            io as *mut dyn DeviceIo as *mut () as usize,
        ));
        io.send(name.as_bytes())?;
        if let Some(gate) = &self.gate {
            gate.entered.send(()).unwrap();
            gate.resume.recv_timeout(WAIT).unwrap();
        }
        assert!(!self.panic, "test driver panic");
        let mut reply = String::new();
        io.read_line(&mut reply)?;
        assert_eq!(reply, "ok\n");
        self.failure.clone().map_or(Ok(()), Err)
    }
}
impl Driver for MockDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "value".into(),
            spec: Spec::Boolean,
            unit: String::new(),
            writable: true,
            default_poll_period: None,
        }]
    }
    fn actions(&self) -> Vec<ActionDescription> {
        self.names
            .iter()
            .map(|name| ActionDescription {
                name: name.clone(),
                description: format!("Perform {name}"),
            })
            .collect()
    }
    fn invoke_action(&mut self, action: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.transaction(action, io)
    }
    fn read(&mut self, _: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.transaction("read", io)?;
        Ok(Value::Boolean(self.value))
    }
    fn write(&mut self, _: &str, value: &Value, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.transaction("write", io)?;
        let Value::Boolean(value) = value else {
            panic!("wrong type")
        };
        self.value = *value;
        Ok(())
    }
}
fn build(
    drivers: Vec<MockDriver>,
    capacity: usize,
) -> (PortWorker<MemoryIo>, PortChannels, CommandRouter) {
    let devices = drivers
        .into_iter()
        .map(|d| DeviceInstance::new(d.name.clone(), d).unwrap())
        .collect();
    let (worker, channels) =
        PortWorker::new(MemoryIo(Cursor::new(vec![])), devices, capacity, 8).unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("p", channels.commands.clone())
        .unwrap();
    for device in worker.devices() {
        registry.register_device("p", device).unwrap();
    }
    (worker, channels, CommandRouter::new(registry))
}
fn finish<T>(mut pending: Pending<T>) -> Completion<T> {
    let end = Instant::now() + WAIT;
    loop {
        match pending.poll(Instant::now()) {
            Poll::Finished(result) => return result,
            Poll::Waiting(next) => pending = next,
        }
        assert!(Instant::now() < end);
        thread::yield_now();
    }
}
fn ok(pending: Pending<Result<(), CommandError>>) {
    assert!(matches!(
        finish(pending),
        Completion::Received { value: Ok(()), .. }
    ));
}
fn send(service: &DeviceService, session: &mut Session, value: Json, now: Instant) {
    service
        .receive_message(session, &value.to_string(), now)
        .unwrap();
}
fn receive(session: &mut Session) -> Json {
    let end = Instant::now() + WAIT;
    loop {
        session.collect_ready(Instant::now()).unwrap();
        if let Some(message) = session.next_message().unwrap() {
            return serde_json::from_str(&message).unwrap();
        }
        assert!(Instant::now() < end);
        thread::yield_now();
    }
}

#[test]
fn catalog_routes_local_names_and_keeps_parameters_separate() {
    let trace = Trace::default();
    let (worker, channels, router) = build(
        vec![MockDriver::new("a", &trace), MockDriver::new("b", &trace)],
        8,
    );
    assert_eq!(router.registry().device("a").unwrap().actions.len(), 3);
    assert_eq!(router.registry().actions().count(), 6);
    assert_eq!(
        router
            .action_description("a/nested/run")
            .unwrap()
            .description
            .name,
        "nested/run"
    );
    assert!(router.registry().parameter("a/value").is_some());
    assert!(matches!(
        router.invoke_action(1, "missing/calibrate", Instant::now() + WAIT),
        Err(RouteError::UnknownDevice(_))
    ));
    assert!(matches!(
        router.invoke_action(1, "a/missing", Instant::now() + WAIT),
        Err(RouteError::UnknownAction(_))
    ));
    let join = thread::spawn(move || worker.run());
    ok(router
        .invoke_action(1, "a/nested/run", Instant::now() + WAIT)
        .unwrap());
    ok(router
        .set(2, "b/value", Value::Boolean(true), Instant::now() + WAIT)
        .unwrap());
    ok(router
        .invoke_action(3, "b/calibrate", Instant::now() + WAIT)
        .unwrap());
    assert!(
        matches!(finish(router.read(4, "b/value", Instant::now()+WAIT).unwrap()), Completion::Received { value: Ok(value), .. } if value.observation.reading == Ok(Value::Boolean(true)))
    );
    // Direct worker users get validation too; no driver calls for unknown names.
    for (device, action, expected) in [
        ("missing", "calibrate", CommandError::UnknownDevice),
        ("a", "missing", CommandError::UnknownAction),
    ] {
        assert!(
            matches!(finish(channels.commands.invoke_action(5, device, action, Instant::now()+WAIT).unwrap()), Completion::Received { value: Err(error), .. } if error == expected)
        );
    }
    channels.commands.shutdown();
    join.join().unwrap();
    let trace = trace.lock().unwrap();
    assert_eq!(
        trace
            .iter()
            .map(|(d, n, _)| (d.as_str(), n.as_str()))
            .collect::<Vec<_>>(),
        [
            ("a", "nested/run"),
            ("b", "write"),
            ("b", "calibrate"),
            ("b", "read")
        ]
    );
    assert!(trace.iter().all(|call| call.2 == trace[0].2));
}

#[test]
fn invalid_action_catalogs_fail_before_io_and_unavailable_catalog_is_retained() {
    let trace = Trace::default();
    for names in [vec![""], vec![" "], vec!["x", "x"]] {
        let mut driver = MockDriver::new("a", &trace);
        driver.names = names.into_iter().map(String::from).collect();
        assert!(DeviceInstance::new("a", driver).is_err());
    }
    let mut registry = DeviceRegistry::new();
    registry
        .register_unavailable_port("offline", "test")
        .unwrap();
    let device = DeviceInstance::new("a", MockDriver::new("a", &trace)).unwrap();
    registry.register_device("offline", &device).unwrap();
    let router = CommandRouter::new(registry);
    assert_eq!(router.registry().actions().count(), 3);
    assert!(matches!(
        router.invoke_action(1, "a/calibrate", Instant::now() + WAIT),
        Err(RouteError::Unavailable { .. })
    ));
    assert!(trace.lock().unwrap().is_empty());
}

#[test]
fn queued_timeout_queue_limit_and_shutdown_never_execute_an_action() {
    let trace = Trace::default();
    let (worker, channels, router) = build(vec![MockDriver::new("a", &trace)], 1);
    let deadline = Instant::now() + WAIT;
    let pending = router.invoke_action(1, "a/calibrate", deadline).unwrap();
    assert!(matches!(
        router.invoke_action(2, "a/calibrate", deadline),
        Err(RouteError::QueueFull { .. })
    ));
    assert!(matches!(
        pending.poll(deadline),
        Poll::Finished(Completion::Timeout { id: 1 })
    ));
    let join = thread::spawn(move || worker.run());
    // A read behind the cancelled action proves the queue was processed.
    let read = loop {
        match router.read(3, "a/value", deadline) {
            Ok(p) => break p,
            Err(RouteError::QueueFull { .. }) => {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
            Err(e) => panic!("{e}"),
        }
    };
    assert!(matches!(
        finish(read),
        Completion::Received { value: Ok(_), .. }
    ));
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(trace.lock().unwrap().len(), 1);
    assert_eq!(trace.lock().unwrap()[0].1, "read");

    let (worker, _channels, router) = build(vec![MockDriver::new("b", &trace)], 2);
    let pending = router
        .invoke_action(4, "b/calibrate", Instant::now() + WAIT)
        .unwrap();
    drop(worker);
    assert!(matches!(
        finish(pending),
        Completion::Received {
            value: Err(CommandError::PortStopped(StopReason::Shutdown)),
            ..
        }
    ));
    assert!(matches!(
        router.invoke_action(5, "b/calibrate", Instant::now() + WAIT),
        Err(RouteError::Stopped { .. })
    ));
    assert_eq!(trace.lock().unwrap().len(), 1);
}

#[test]
fn expired_action_is_skipped_even_when_waiter_is_dropped_without_polling() {
    let trace = Trace::default();
    let (worker, channels, router) = build(vec![MockDriver::new("a", &trace)], 4);
    drop(
        router
            .invoke_action(1, "a/calibrate", Instant::now())
            .unwrap(),
    );
    let read = router.read(2, "a/value", Instant::now() + WAIT).unwrap();
    let join = thread::spawn(move || worker.run());
    assert!(matches!(
        finish(read),
        Completion::Received { value: Ok(_), .. }
    ));
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(trace.lock().unwrap().len(), 1);
    assert_eq!(trace.lock().unwrap()[0].1, "read");
}

#[test]
fn driver_errors_preserve_port_state_and_never_retry() {
    for state in [
        PortState::Synchronized,
        PortState::Desynchronized,
        PortState::Disconnected,
    ] {
        for panic in [false, true] {
            let trace = Trace::default();
            let mut driver = MockDriver::new("a", &trace);
            driver.failure = Some(DriverError::new("test failure", state));
            driver.panic = panic;
            let (worker, channels, router) = build(vec![driver], 4);
            let first = router
                .invoke_action(1, "a/calibrate", Instant::now() + WAIT)
                .unwrap();
            let second = router
                .invoke_action(2, "a/calibrate", Instant::now() + WAIT)
                .unwrap();
            let join = thread::spawn(move || worker.run());
            assert!(matches!(
                finish(first),
                Completion::Received {
                    value: Err(CommandError::OutcomeUnknown),
                    ..
                }
            ));
            let second = finish(second);
            if state == PortState::Synchronized && !panic {
                assert!(matches!(
                    second,
                    Completion::Received {
                        value: Err(CommandError::OutcomeUnknown),
                        ..
                    }
                ));
                assert!(channels.commands.stop_reason().is_none());
                assert_eq!(trace.lock().unwrap().len(), 2);
                channels.commands.shutdown();
            } else {
                assert!(matches!(
                    second,
                    Completion::Received {
                        value: Err(CommandError::PortStopped(_)),
                        ..
                    }
                ));
                assert_eq!(
                    channels.commands.port_state(),
                    if panic {
                        PortState::Desynchronized
                    } else {
                        state
                    }
                );
                assert_eq!(trace.lock().unwrap().len(), 1);
            }
            join.join().unwrap();
        }
    }
}

#[test]
fn sessions_share_pending_capacity_and_timeout_has_one_final_response() {
    let trace = Trace::default();
    let (gate, entered, resume) = gate();
    let mut driver = MockDriver::new("a", &trace);
    driver.gate = Some(gate);
    let (worker, channels, router) = build(vec![driver], 4);
    let service = DeviceService::new(router, 8);
    let mut session = Session::new(1, 65536);
    let now = Instant::now();
    send(&service, &mut session, json!({"id":1,"op":"describe"}), now);
    let catalog = receive(&mut session);
    assert_eq!(
        catalog["actions"],
        json!([
            {"key":"a/calibrate","description":"Perform calibrate"},
            {"key":"a/nested/run","description":"Perform nested/run"},
            {"key":"a/value","description":"Perform value"}
        ])
    );
    assert_eq!(catalog["parameters"].as_array().unwrap().len(), 1);
    send(
        &service,
        &mut session,
        json!({"id":2,"op":"invoke_action","key":"a/calibrate"}),
        now,
    );
    for request in [
        json!({"id":3,"op":"set","key":"a/value","value":true}),
        json!({"id":4,"op":"invoke_action","key":"a/calibrate"}),
    ] {
        send(&service, &mut session, request, now);
        assert_eq!(receive(&mut session)["error"], "too_many_pending");
    }
    for id in [2, 1] {
        send(
            &service,
            &mut session,
            json!({"id":id,"op":"invoke_action","key":"a/calibrate"}),
            now,
        );
        assert_eq!(
            receive(&mut session),
            json!({"status":"error","error":"duplicate_id"})
        );
    }
    let join = thread::spawn(move || worker.run());
    entered.recv_timeout(WAIT).unwrap();
    session.collect_ready(now + WAIT).unwrap();
    assert_eq!(
        receive(&mut session),
        json!({"id":2,"status":"error","error":"outcome_unknown"})
    );
    resume.send(()).unwrap();
    // FIFO action on a second session is a barrier for the first late completion.
    let mut second = Session::new(2, 65536);
    send(
        &service,
        &mut second,
        json!({"id":1,"op":"invoke_action","key":"a/nested/run"}),
        Instant::now(),
    );
    entered.recv_timeout(WAIT).unwrap();
    resume.send(()).unwrap();
    assert_eq!(receive(&mut second), json!({"id":1,"status":"ok"}));
    session.collect_ready(Instant::now() + WAIT).unwrap();
    assert!(session.next_message().unwrap().is_none());
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(trace.lock().unwrap().len(), 2);
}

#[test]
fn disconnected_waiter_does_not_cancel_or_repeat_action_and_other_port_progresses() {
    for started in [false, true] {
        let trace = Trace::default();
        let (gate, entered, resume) = gate();
        let mut driver = MockDriver::new("a", &trace);
        driver.gate = Some(gate);
        let (worker, channels, router) = build(vec![driver], 4);
        let pending = router
            .invoke_action(1, "a/calibrate", Instant::now() + WAIT)
            .unwrap();
        let join;
        if started {
            join = thread::spawn(move || worker.run());
            entered.recv_timeout(WAIT).unwrap();
            drop(pending);
        } else {
            drop(pending);
            join = thread::spawn(move || worker.run());
            entered.recv_timeout(WAIT).unwrap();
        }
        let (other, other_channels, other_router) = build(vec![MockDriver::new("b", &trace)], 4);
        let other_join = thread::spawn(move || other.run());
        ok(other_router
            .invoke_action(1, "b/calibrate", Instant::now() + WAIT)
            .unwrap());
        other_channels.commands.shutdown();
        other_join.join().unwrap();
        resume.send(()).unwrap();
        // Shutdown waits for the in-flight action, so join proves completion.
        channels.commands.shutdown();
        join.join().unwrap();
        let trace = trace.lock().unwrap();
        assert_eq!(trace.iter().filter(|call| call.0 == "a").count(), 1);
        assert_eq!(trace.iter().filter(|call| call.0 == "b").count(), 1);
    }
}

#[test]
fn session_timeout_before_worker_start_is_definitive_and_cannot_replay() {
    let trace = Trace::default();
    let (worker, channels, router) = build(vec![MockDriver::new("a", &trace)], 4);
    let service = DeviceService::new(router, 8);
    let mut session = Session::new(2, 65536);
    let now = Instant::now();
    send(
        &service,
        &mut session,
        json!({"id":1,"op":"invoke_action","key":"a/calibrate"}),
        now,
    );
    session.collect_ready(now + WAIT).unwrap();
    assert_eq!(
        receive(&mut session),
        json!({"id":1,"status":"error","error":"runtime_timeout"})
    );
    send(
        &service,
        &mut session,
        json!({"id":1,"op":"invoke_action","key":"a/calibrate"}),
        now,
    );
    assert_eq!(
        receive(&mut session),
        json!({"status":"error","error":"duplicate_id"})
    );
    let join = thread::spawn(move || worker.run());
    send(
        &service,
        &mut session,
        json!({"id":2,"op":"get","key":"a/value"}),
        Instant::now(),
    );
    assert_eq!(receive(&mut session)["value"], false);
    session.collect_ready(Instant::now() + WAIT).unwrap();
    assert!(session.next_message().unwrap().is_none());
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(trace.lock().unwrap().len(), 1);
    assert_eq!(trace.lock().unwrap()[0].1, "read");
}

#[test]
fn existing_driver_implementations_default_to_no_actions() {
    struct Legacy;
    impl Driver for Legacy {
        fn parameters(&self) -> Vec<ParameterDescription> {
            Vec::new()
        }
        fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
            unreachable!()
        }
        fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
            unreachable!()
        }
    }
    assert!(Legacy.actions().is_empty());
    let device = DeviceInstance::new("legacy", Legacy).unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_unavailable_port("p", "no hardware")
        .unwrap();
    registry.register_device("p", &device).unwrap();
    let service = DeviceService::new(CommandRouter::new(registry), 4);
    let mut session = Session::new(2, 65536);
    send(
        &service,
        &mut session,
        json!({"id":1,"op":"describe"}),
        Instant::now(),
    );
    assert_eq!(
        receive(&mut session),
        json!({"id":1,"status":"ok","parameters":[],"actions":[]})
    );
    send(
        &service,
        &mut session,
        json!({"id":2,"op":"invoke_action","key":"legacy/calibrate"}),
        Instant::now(),
    );
    assert_eq!(receive(&mut session)["error"], "unknown_key");
}
