use std::io::{self, BufRead, Cursor, Read};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use lab_server::command_router::{CommandRouter, RouteError};
use lab_server::device_registry::{Availability, DeviceRegistry, RegistryError};
use lab_server::driver::{DeviceIo, Driver, DriverError, PortState};
use lab_server::model::{ParameterDescription, Spec, Value};
use lab_server::pending::{Completion, Pending, Poll};
use lab_server::port_worker::{CommandError, DeviceInstance, PortChannels, PortWorker, StopReason};

const WAIT: Duration = Duration::from_secs(10);
type Trace = Arc<Mutex<Vec<String>>>;

struct FakeIo {
    trace: Trace,
    response: Cursor<Vec<u8>>,
}

impl Read for FakeIo {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.response.read(out)
    }
}
impl BufRead for FakeIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.response.fill_buf()
    }
    fn consume(&mut self, size: usize) {
        self.response.consume(size);
    }
}
impl DeviceIo for FakeIo {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        assert!(
            self.response.fill_buf()?.is_empty(),
            "overlapping transactions"
        );
        self.trace
            .lock()
            .unwrap()
            .push(String::from_utf8(request.to_vec()).unwrap());
        self.response = Cursor::new(b"ok\n".to_vec());
        Ok(())
    }
}

struct Gate {
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}

fn gate() -> (Gate, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, calls) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    (Gate { entered, resume }, calls, release)
}

// Distinct Rust driver types and wire prefixes; each instance owns its state.
struct FakeDriver<const PROTOCOL: u8> {
    count: i64,
    enabled: bool,
    level: f64,
    gate: Option<Gate>,
    failure: Option<DriverError>,
}

impl<const PROTOCOL: u8> FakeDriver<PROTOCOL> {
    fn new(count: i64) -> Self {
        Self {
            count,
            enabled: false,
            level: 1.25,
            gate: None,
            failure: None,
        }
    }

    fn transaction(
        &mut self,
        parameter: &str,
        write: bool,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        assert!(matches!(
            parameter,
            "count" | "level" | "enabled" | "readonly" | "nested/value"
        ));
        io.send(format!("{PROTOCOL}:{parameter}:{write}").as_bytes())?;
        if let Some(gate) = self.gate.take() {
            gate.entered.send(()).unwrap();
            gate.resume.recv_timeout(WAIT).unwrap();
        }
        let failure = self.failure.take();
        if let Some(error) = &failure
            && error.port_state != PortState::Synchronized
        {
            return Err(error.clone());
        }
        let mut response = String::new();
        io.read_line(&mut response)?;
        assert_eq!(response, "ok\n");
        failure.map_or(Ok(()), Err)
    }
}

impl<const PROTOCOL: u8> Driver for FakeDriver<PROTOCOL> {
    fn parameters(&self) -> Vec<ParameterDescription> {
        [
            (
                "count",
                Spec::Integer {
                    min: -10,
                    max: 10_000,
                },
                "count",
                true,
            ),
            (
                "level",
                Spec::Float {
                    min: -5.0,
                    max: 5.0,
                },
                "V",
                true,
            ),
            ("enabled", Spec::Boolean, "", true),
            ("readonly", Spec::Boolean, "", false),
            (
                "nested/value",
                Spec::Integer {
                    min: -10,
                    max: 10_000,
                },
                "count",
                false,
            ),
        ]
        .into_iter()
        .map(|(id, spec, unit, writable)| ParameterDescription {
            id: id.into(),
            spec,
            unit: unit.into(),
            writable,
            default_poll_period: None,
        })
        .collect()
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.transaction(parameter, false, io)?;
        Ok(match parameter {
            "enabled" | "readonly" => Value::Boolean(self.enabled),
            "level" => Value::Float(self.level),
            _ => Value::Integer(self.count),
        })
    }

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        self.transaction(parameter, true, io)?;
        match (parameter, value) {
            ("count", Value::Integer(value)) => self.count = *value,
            ("level", Value::Float(value)) => self.level = *value,
            ("enabled", Value::Boolean(value)) => self.enabled = *value,
            _ => panic!("invalid write reached driver"),
        }
        Ok(())
    }
}

fn device(id: &str, count: i64) -> DeviceInstance {
    DeviceInstance::new(id, FakeDriver::<1>::new(count)).unwrap()
}

struct RunningPort {
    channels: PortChannels,
    trace: Trace,
    join: Option<JoinHandle<StopReason>>,
}

impl RunningPort {
    fn join(&mut self) -> StopReason {
        self.join.take().unwrap().join().unwrap()
    }
}
impl Drop for RunningPort {
    fn drop(&mut self) {
        self.channels.commands.shutdown();
        if let Some(join) = self.join.take() {
            join.join().unwrap();
        }
    }
}

fn start(
    registry: &mut DeviceRegistry,
    port_id: &str,
    devices: Vec<DeviceInstance>,
    capacity: usize,
) -> RunningPort {
    let trace = Trace::default();
    let io = FakeIo {
        trace: trace.clone(),
        response: Cursor::new(vec![]),
    };
    let (worker, channels) = PortWorker::new(io, devices, capacity, 1).unwrap();
    registry
        .register_port(port_id, channels.commands.clone())
        .unwrap();
    for device in worker.devices() {
        registry.register_device(port_id, device).unwrap();
    }
    let join = Some(thread::spawn(move || worker.run()));
    RunningPort {
        channels,
        trace,
        join,
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

fn received<T>(mut pending: Pending<T>) -> T {
    let limit = Instant::now() + WAIT;
    loop {
        match pending.poll(Instant::now()) {
            Poll::Waiting(next) => pending = next,
            Poll::Finished(Completion::Received { value, .. }) => return value,
            _ => panic!("expected explicit completion"),
        }
        assert!(Instant::now() < limit, "reply timed out");
        thread::sleep(Duration::from_millis(1));
    }
}

fn value(router: &CommandRouter, key: &str) -> Value {
    received(router.read(1, key, deadline()).unwrap())
        .unwrap()
        .observation
        .reading
        .unwrap()
}

#[test]
fn single_device_catalog_preserves_model_and_polling_overrides() {
    let mut instance = device("furnace", 4);
    instance
        .set_poll_period("count", Some(Duration::from_secs(3600)))
        .unwrap();
    let mut registry = DeviceRegistry::new();
    let _port = start(&mut registry, "link", vec![instance], 8);
    let router = CommandRouter::new(registry);
    assert_eq!(router.availability("furnace"), Ok(Availability::Available));
    assert_eq!(router.registry().devices().count(), 1);
    assert_eq!(router.registry().ports().next().unwrap().id(), "link");
    assert_eq!(router.registry().parameters().count(), 5);
    let count = router.description("furnace/count").unwrap();
    assert_eq!(
        (&*count.device_id, &*count.port_id, &*count.description.id),
        ("furnace", "link", "count")
    );
    assert!(matches!(
        count.description.spec,
        Spec::Integer {
            min: -10,
            max: 10_000
        }
    ));
    assert_eq!(count.description.unit, "count");
    assert_eq!(
        count.description.default_poll_period,
        Some(Duration::from_secs(3600))
    );
    let level = router.description("furnace/level").unwrap();
    assert!(matches!(
        level.description.spec,
        Spec::Float {
            min: -5.0,
            max: 5.0
        }
    ));
    assert_eq!(level.description.unit, "V");
    let enabled = router.description("furnace/enabled").unwrap();
    assert!(matches!(enabled.description.spec, Spec::Boolean));
    assert!(enabled.description.writable);
    assert_eq!(enabled.description.unit, "");
    assert_eq!(enabled.description.default_poll_period, None);
    assert!(
        !router
            .description("furnace/readonly")
            .unwrap()
            .description
            .writable
    );
    assert_eq!(value(&router, "furnace/count"), Value::Integer(4));
    assert_eq!(value(&router, "furnace/level"), Value::Float(1.25));
    for enabled in [true, false] {
        assert_eq!(
            received(
                router
                    .set(2, "furnace/enabled", Value::Boolean(enabled), deadline())
                    .unwrap()
            ),
            Ok(())
        );
        assert_eq!(value(&router, "furnace/enabled"), Value::Boolean(enabled));
    }
}

#[test]
fn different_drivers_share_handle_with_local_names_and_independent_state() {
    let mut registry = DeviceRegistry::new();
    let port = start(
        &mut registry,
        "shared",
        vec![
            device("a", 1),
            DeviceInstance::new("b", FakeDriver::<2>::new(2)).unwrap(),
            device("c", 3),
        ],
        8,
    );
    let router = CommandRouter::new(registry);
    assert_eq!(router.registry().ports().count(), 1);
    assert_eq!(
        received(
            router
                .set(1, "a/count", Value::Integer(42), deadline())
                .unwrap()
        ),
        Ok(())
    );
    for (id, expected) in [("a", 42), ("b", 2), ("c", 3)] {
        assert_eq!(
            value(&router, &format!("{id}/count")),
            Value::Integer(expected)
        );
        assert_eq!(router.registry().device(id).unwrap().port_id, "shared");
    }
    assert_eq!(
        *port.trace.lock().unwrap(),
        [
            "1:count:true",
            "1:count:false",
            "2:count:false",
            "1:count:false"
        ]
    );
}

#[test]
fn different_ports_route_to_the_correct_handle() {
    let mut registry = DeviceRegistry::new();
    let a = start(&mut registry, "p1", vec![device("a", 11)], 4);
    let b = start(&mut registry, "p2", vec![device("b", 22)], 4);
    let router = CommandRouter::new(registry);
    assert_eq!(value(&router, "b/count"), Value::Integer(22));
    assert!(a.trace.lock().unwrap().is_empty());
    assert_eq!(b.trace.lock().unwrap().len(), 1);
    received(
        router
            .set(2, "a/enabled", Value::Boolean(true), deadline())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value(&router, "a/enabled"), Value::Boolean(true));
    assert_eq!(value(&router, "b/enabled"), Value::Boolean(false));
    assert_eq!(a.trace.lock().unwrap().len(), 2);
}

#[test]
fn exact_global_index_preserves_slashes_in_local_names_and_maps_observations() {
    let mut registry = DeviceRegistry::new();
    let port = start(&mut registry, "p", vec![device("a", 8), device("b", 9)], 4);
    let router = CommandRouter::new(registry);
    let mut observation = received(router.read(7, "a/nested/value", deadline()).unwrap()).unwrap();
    assert_eq!(observation.observation.source, "nested/value");
    let parameter = router
        .registry()
        .observation_parameter(&observation)
        .unwrap();
    assert_eq!(parameter.global_key, "a/nested/value");
    assert_eq!(parameter.port_id, "p");
    assert_eq!(*port.trace.lock().unwrap(), ["1:nested/value:false"]);
    observation.device_id = "b".into();
    assert_eq!(
        router
            .registry()
            .observation_parameter(&observation)
            .unwrap()
            .global_key,
        "b/nested/value"
    );
    observation.device_id = "a/nested".into();
    observation.observation.source = "value".into();
    assert!(
        router
            .registry()
            .observation_parameter(&observation)
            .is_none()
    );
    observation.device_id = "a".into();
    observation.observation.source = "missing".into();
    assert!(
        router
            .registry()
            .observation_parameter(&observation)
            .is_none()
    );
    for key in [
        "count",
        "missing/count",
        "a/COUNT",
        "a/missing",
        "a//count",
        "a/count/",
    ] {
        assert!(
            matches!(router.read(1, key, deadline()), Err(RouteError::UnknownSignal(actual)) if actual == key)
        );
        assert!(
            matches!(router.set(1, key, Value::Integer(1), deadline()), Err(RouteError::UnknownSignal(actual)) if actual == key)
        );
    }
    assert_eq!(
        router.availability("unknown"),
        Err(RouteError::UnknownDevice("unknown".into()))
    );
}

#[test]
fn registration_rejects_duplicates_missing_ports_and_ambiguous_device_ids_atomically() {
    let mut registry = DeviceRegistry::new();
    assert_eq!(
        registry.register_unavailable_port(" ", "offline"),
        Err(RegistryError::InvalidPortId(" ".into()))
    );
    registry.register_unavailable_port("p", "offline").unwrap();
    registry.register_unavailable_port("q", "offline").unwrap();
    assert_eq!(
        registry.register_unavailable_port("p", "replacement"),
        Err(RegistryError::DuplicatePort("p".into()))
    );
    let a = device("a", 1);
    assert_eq!(
        registry.register_device("missing", &a),
        Err(RegistryError::UnknownPort("missing".into()))
    );
    assert_eq!(registry.devices().count(), 0);
    assert_eq!(registry.parameters().count(), 0);
    registry.register_device("p", &a).unwrap();
    assert_eq!(
        registry.register_device("q", &a),
        Err(RegistryError::DuplicateDevice("a".into()))
    );
    assert_eq!(
        registry.register_device("p", &device("a/nested", 1)),
        Err(RegistryError::InvalidDeviceId("a/nested".into()))
    );
    assert_eq!(registry.devices().count(), 1);
    assert_eq!(registry.parameters().count(), 5);
    assert_eq!(registry.device("a").unwrap().port_id, "p");
    assert_eq!(
        registry.port("p").unwrap().availability(),
        Availability::Unavailable {
            reason: "offline".into()
        }
    );
}

#[test]
fn partial_start_keeps_unavailable_catalog_and_routes_independent_ports() {
    let mut registry = DeviceRegistry::new();
    let _a = start(&mut registry, "p1", vec![device("furnace", 1)], 4);
    let _b = start(&mut registry, "p3", vec![device("sensor", 2)], 4);
    registry
        .register_unavailable_port("p2", "COM4: access denied")
        .unwrap();
    for id in ["gauge", "controller"] {
        registry.register_device("p2", &device(id, 3)).unwrap();
    }
    let router = CommandRouter::new(registry);
    assert_eq!(router.registry().devices().count(), 4);
    assert_eq!(router.registry().parameters().count(), 20);
    assert!(router.registry().port("p2").unwrap().handle().is_none());
    assert_eq!(router.registry().port("p2").unwrap().loss_epoch(), None);
    for id in ["gauge", "controller"] {
        assert_eq!(
            router.availability(id),
            Ok(Availability::Unavailable {
                reason: "COM4: access denied".into()
            })
        );
        let key = format!("{id}/enabled");
        assert_eq!(router.description(&key).unwrap().port_id, "p2");
        let expected = RouteError::Unavailable {
            device_id: id.into(),
            port_id: "p2".into(),
            reason: "COM4: access denied".into(),
        };
        assert_eq!(
            router.read(1, &key, deadline()).err(),
            Some(expected.clone())
        );
        assert_eq!(
            router.set(1, &key, Value::Boolean(true), deadline()).err(),
            Some(expected)
        );
    }
    assert_eq!(value(&router, "furnace/count"), Value::Integer(1));
    assert_eq!(value(&router, "sensor/count"), Value::Integer(2));
}

#[test]
fn invalid_writes_are_rejected_before_queue_admission_or_driver_calls() {
    let mut registry = DeviceRegistry::new();
    let port = start(&mut registry, "p", vec![device("a", 0)], 1);
    let router = CommandRouter::new(registry);
    for (key, value) in [
        ("a/enabled", Value::Integer(1)),
        ("a/enabled", Value::Float(0.0)),
        ("a/count", Value::Boolean(false)),
        ("a/count", Value::Integer(-11)),
        ("a/count", Value::Integer(10_001)),
        ("a/level", Value::Integer(1)),
        ("a/level", Value::Float(5.1)),
        ("a/level", Value::Float(f64::NAN)),
        ("a/level", Value::Float(f64::INFINITY)),
    ] {
        assert_eq!(
            router.set(1, key, value, deadline()).err(),
            Some(RouteError::InvalidValue {
                global_key: key.into()
            })
        );
    }
    assert_eq!(
        router
            .set(2, "a/readonly", Value::Boolean(true), deadline())
            .err(),
        Some(RouteError::ReadOnly {
            global_key: "a/readonly".into()
        })
    );
    // A subsequent read is a FIFO fence: rejected writes never entered the queue.
    assert_eq!(value(&router, "a/count"), Value::Integer(0));
    assert_eq!(*port.trace.lock().unwrap(), ["1:count:false"]);
    for (key, new_value) in [
        ("a/count", Value::Integer(-10)),
        ("a/level", Value::Float(5.0)),
    ] {
        received(router.set(3, key, new_value.clone(), deadline()).unwrap()).unwrap();
        assert_eq!(value(&router, key), new_value);
    }
}

#[test]
fn concurrent_nonblocking_admission_and_queue_full_are_local_to_one_port() {
    let (gate, entered, release) = gate();
    let mut driver = FakeDriver::<1>::new(1);
    driver.gate = Some(gate);
    let mut registry = DeviceRegistry::new();
    let _slow = start(
        &mut registry,
        "slow",
        vec![
            DeviceInstance::new("a", driver).unwrap(),
            device("shared", 5),
        ],
        1,
    );
    let _fast = start(&mut registry, "fast", vec![device("b", 2)], 1);
    let router = CommandRouter::new(registry);
    let active = router.read(1, "a/count", deadline()).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    // Submission must finish while the physical transaction is held at the gate.
    let sender_router = router.clone();
    let (submitted, result) = mpsc::channel();
    let sender = thread::spawn(move || {
        submitted
            .send(sender_router.set(2, "shared/enabled", Value::Boolean(true), deadline()))
            .unwrap();
    });
    let queued = result
        .recv_timeout(WAIT)
        .expect("routing waited for device I/O")
        .unwrap();
    sender.join().unwrap();
    for key in ["a/count", "shared/count"] {
        assert_eq!(
            router.read(3, key, deadline()).err(),
            Some(RouteError::QueueFull {
                port_id: "slow".into()
            })
        );
    }
    assert_eq!(
        router
            .set(4, "a/enabled", Value::Boolean(true), deadline())
            .err(),
        Some(RouteError::QueueFull {
            port_id: "slow".into()
        })
    );
    assert_eq!(value(&router, "b/count"), Value::Integer(2));
    release.send(()).unwrap();
    assert!(received(active).unwrap().observation.reading.is_ok());
    assert_eq!(received(queued), Ok(()));
    assert_eq!(value(&router, "shared/enabled"), Value::Boolean(true));
}

#[test]
fn concurrent_senders_keep_requests_device_state_and_ids_independent() {
    let mut registry = DeviceRegistry::new();
    let devices = (0..8).map(|i| device(&format!("d{i}"), i)).collect();
    let port = start(&mut registry, "shared", devices, 16);
    let router = CommandRouter::new(registry);
    let barrier = Arc::new(Barrier::new(8));
    let senders: Vec<_> = (0..8)
        .map(|i| {
            let router = router.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let key = format!("d{i}/count");
                for round in 0..12 {
                    let expected = i * 100 + round;
                    let request_id = expected as u64;
                    let set = router
                        .set(request_id, &key, Value::Integer(expected), deadline())
                        .unwrap();
                    received(set).unwrap();
                    let mut read = router.read(request_id, &key, deadline()).unwrap();
                    let limit = Instant::now() + WAIT;
                    loop {
                        match read.poll(Instant::now()) {
                            Poll::Waiting(next) => read = next,
                            Poll::Finished(Completion::Received { id, value }) => {
                                assert_eq!(id, request_id);
                                let observation = value.unwrap();
                                assert_eq!(observation.device_id, format!("d{i}"));
                                assert_eq!(observation.observation.source, "count");
                                assert_eq!(
                                    observation.observation.reading,
                                    Ok(Value::Integer(expected))
                                );
                                break;
                            }
                            _ => panic!("unexpected pending completion"),
                        }
                        assert!(Instant::now() < limit);
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            })
        })
        .collect();
    for sender in senders {
        sender.join().unwrap();
    }
    assert_eq!(port.trace.lock().unwrap().len(), 8 * 12 * 2);
}

#[test]
fn concurrent_physical_failure_stops_shared_devices_and_rejects_queued_commands() {
    for state in [PortState::Desynchronized, PortState::Disconnected] {
        let (gate, entered, release) = gate();
        let error = DriverError::new("broken transaction", state);
        let reason = StopReason::Driver(error.clone());
        let mut driver = FakeDriver::<1>::new(1);
        driver.gate = Some(gate);
        driver.failure = Some(error);
        let mut registry = DeviceRegistry::new();
        let mut failed = start(
            &mut registry,
            "p1",
            vec![
                DeviceInstance::new("a", driver).unwrap(),
                device("shared", 1),
            ],
            2,
        );
        let _healthy = start(&mut registry, "p2", vec![device("b", 2)], 2);
        let router = CommandRouter::new(registry);
        let active = router.read(1, "a/count", deadline()).unwrap();
        entered.recv_timeout(WAIT).unwrap();
        let queued_read = router.read(2, "shared/count", deadline()).unwrap();
        let queued_set = router
            .set(3, "a/enabled", Value::Boolean(true), deadline())
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(
            received(active).unwrap().observation.reading,
            Err("broken transaction".into())
        );
        assert_eq!(
            received(queued_read).err(),
            Some(CommandError::PortStopped(reason.clone()))
        );
        assert_eq!(
            received(queued_set),
            Err(CommandError::PortStopped(reason.clone()))
        );
        assert_eq!(failed.join(), reason);
        for id in ["a", "shared"] {
            assert_eq!(
                router.availability(id),
                Ok(Availability::Stopped {
                    reason: reason.clone()
                })
            );
            let key = format!("{id}/enabled");
            assert!(router.description(&key).is_ok());
            let expected = RouteError::Stopped {
                device_id: id.into(),
                port_id: "p1".into(),
                reason: reason.clone(),
            };
            assert_eq!(
                router.read(4, &key, deadline()).err(),
                Some(expected.clone())
            );
            assert_eq!(
                router.set(5, &key, Value::Boolean(true), deadline()).err(),
                Some(expected)
            );
        }
        assert_eq!(router.availability("b"), Ok(Availability::Available));
        assert_eq!(value(&router, "b/count"), Value::Integer(2));
        assert_eq!(failed.trace.lock().unwrap().len(), 1);
        assert_eq!(router.registry().parameters().count(), 15);
    }
}

#[test]
fn synchronized_read_error_preserves_observation_and_port_availability() {
    let mut driver = FakeDriver::<1>::new(1);
    driver.failure = Some(DriverError::new(
        "measurement rejected",
        PortState::Synchronized,
    ));
    let mut registry = DeviceRegistry::new();
    let _port = start(
        &mut registry,
        "p",
        vec![DeviceInstance::new("a", driver).unwrap(), device("b", 2)],
        4,
    );
    let router = CommandRouter::new(registry);
    let observation = received(router.read(1, "a/count", deadline()).unwrap()).unwrap();
    assert_eq!(
        observation.observation.reading,
        Err("measurement rejected".into())
    );
    assert_eq!(
        router
            .registry()
            .observation_parameter(&observation)
            .unwrap()
            .global_key,
        "a/count"
    );
    assert_eq!(router.availability("a"), Ok(Availability::Available));
    assert_eq!(value(&router, "a/count"), Value::Integer(1));
    assert_eq!(value(&router, "b/count"), Value::Integer(2));
}

#[test]
fn started_set_driver_errors_preserve_outcome_unknown_without_retries() {
    for state in [
        PortState::Synchronized,
        PortState::Desynchronized,
        PortState::Disconnected,
    ] {
        let mut driver = FakeDriver::<1>::new(1);
        let error = DriverError::new("write result uncertain", state);
        driver.failure = Some(error.clone());
        let mut registry = DeviceRegistry::new();
        let port = start(
            &mut registry,
            "p",
            vec![DeviceInstance::new("a", driver).unwrap()],
            4,
        );
        let router = CommandRouter::new(registry);
        assert_eq!(
            received(
                router
                    .set(1, "a/enabled", Value::Boolean(true), deadline())
                    .unwrap()
            ),
            Err(CommandError::OutcomeUnknown)
        );
        let expected = if state == PortState::Synchronized {
            Availability::Available
        } else {
            Availability::Stopped {
                reason: StopReason::Driver(error),
            }
        };
        assert_eq!(router.availability("a"), Ok(expected));
        assert_eq!(*port.trace.lock().unwrap(), ["1:enabled:true"]);
    }
}

#[test]
fn concurrent_set_timeout_before_start_never_reaches_driver() {
    let (gate, entered, release) = gate();
    let mut driver = FakeDriver::<1>::new(1);
    driver.gate = Some(gate);
    let mut registry = DeviceRegistry::new();
    let port = start(
        &mut registry,
        "p",
        vec![DeviceInstance::new("a", driver).unwrap()],
        4,
    );
    let router = CommandRouter::new(registry);
    let active = router.read(1, "a/count", deadline()).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let expires = deadline();
    let cancelled = router
        .set(2, "a/enabled", Value::Boolean(true), expires)
        .unwrap();
    assert!(matches!(
        cancelled.poll(expires),
        Poll::Finished(Completion::Timeout { id: 2 })
    ));
    let expired = router
        .set(
            3,
            "a/enabled",
            Value::Boolean(true),
            Instant::now() - Duration::from_secs(1),
        )
        .unwrap();
    release.send(()).unwrap();
    received(active).unwrap();
    // A read queued after both writes proves the worker has processed them.
    assert_eq!(value(&router, "a/enabled"), Value::Boolean(false));
    assert_eq!(received(expired), Err(CommandError::RuntimeTimeout));
    assert_eq!(
        *port.trace.lock().unwrap(),
        ["1:count:false", "1:enabled:false"]
    );
}

#[test]
fn concurrent_set_timeout_after_start_retains_outcome_unknown() {
    let (gate, entered, release) = gate();
    let mut driver = FakeDriver::<1>::new(1);
    driver.gate = Some(gate);
    let mut registry = DeviceRegistry::new();
    let port = start(
        &mut registry,
        "p",
        vec![DeviceInstance::new("a", driver).unwrap()],
        4,
    );
    let router = CommandRouter::new(registry);
    let expires = deadline();
    let pending = router
        .set(41, "a/enabled", Value::Boolean(true), expires)
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        pending.poll(expires),
        Poll::Finished(Completion::OutcomeUnknown { id: 41 })
    ));
    release.send(()).unwrap();
    assert_eq!(value(&router, "a/enabled"), Value::Boolean(true));
    assert_eq!(
        *port.trace.lock().unwrap(),
        ["1:enabled:true", "1:enabled:false"]
    );
}

#[test]
fn concurrent_shutdown_rejects_pending_without_changing_catalog_or_other_ports() {
    let (gate, entered, release) = gate();
    let mut driver = FakeDriver::<1>::new(1);
    driver.gate = Some(gate);
    let mut registry = DeviceRegistry::new();
    let mut port = start(
        &mut registry,
        "p",
        vec![DeviceInstance::new("a", driver).unwrap()],
        2,
    );
    let _healthy = start(&mut registry, "q", vec![device("b", 2)], 2);
    let router = CommandRouter::new(registry);
    let active = router.read(1, "a/count", deadline()).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let queued = router
        .set(2, "a/enabled", Value::Boolean(true), deadline())
        .unwrap();
    port.channels.commands.shutdown();
    assert_eq!(
        router.availability("a"),
        Ok(Availability::Stopped {
            reason: StopReason::Shutdown
        })
    );
    assert_eq!(router.registry().device("a").unwrap().parameters.len(), 5);
    assert!(router.description("a/count").is_ok());
    assert_eq!(value(&router, "b/count"), Value::Integer(2));
    release.send(()).unwrap();
    received(active).unwrap();
    assert_eq!(
        received(queued),
        Err(CommandError::PortStopped(StopReason::Shutdown))
    );
    assert_eq!(port.join(), StopReason::Shutdown);
    assert_eq!(port.trace.lock().unwrap().len(), 1);
}

#[test]
fn port_loss_counters_and_periodic_observation_mapping_remain_accessible() {
    let mut a = device("a", 1);
    a.set_poll_period("count", Some(Duration::from_nanos(1)))
        .unwrap();
    let mut registry = DeviceRegistry::new();
    let mut lossy = start(&mut registry, "p", vec![a], 4);
    let _independent = start(&mut registry, "q", vec![device("b", 2)], 4);
    let router = CommandRouter::new(registry);
    let port = router.registry().port("p").unwrap();
    let limit = Instant::now() + WAIT;
    while port.loss_epoch() == Some(0) {
        assert!(Instant::now() < limit);
        thread::sleep(Duration::from_millis(1));
    }
    let first_epoch = port.loss_epoch().unwrap();
    let observation = lossy.channels.observations.recv_timeout(WAIT).unwrap();
    assert_eq!(
        router
            .registry()
            .observation_parameter(&observation)
            .unwrap()
            .global_key,
        "a/count"
    );
    assert_eq!(observation.observation.source, "count");
    assert_eq!(router.registry().port("q").unwrap().loss_epoch(), Some(0));
    assert_eq!(value(&router, "b/count"), Value::Integer(2));
    received(
        router
            .set(2, "a/enabled", Value::Boolean(true), deadline())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value(&router, "a/enabled"), Value::Boolean(true));
    assert_eq!(router.availability("a"), Ok(Availability::Available));
    lossy.channels.commands.shutdown();
    assert_eq!(lossy.join(), StopReason::Shutdown);
    for _ in lossy.channels.observations.try_iter() {}
    assert!(port.loss_epoch().unwrap() >= first_epoch);
    assert_eq!(
        port.loss_epoch(),
        Some(lossy.channels.commands.loss_epoch())
    );
}
