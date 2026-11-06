use std::io::{self, BufRead, Cursor, Read};
use std::sync::{Arc, Barrier, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use lab_server::device_registry::DeviceRegistry;
use lab_server::driver::{DeviceIo, Driver, DriverError, PortState};
use lab_server::model::{Observation, ParameterDescription, Spec, Value};
use lab_server::observation_aggregator::{
    AggregatorEvent, BuildError, MappingError, ObservationAggregator,
};
use lab_server::pending::{Completion, Pending, Poll};
use lab_server::port_worker::{
    DeviceInstance, DeviceObservation, PortHandle, PortWorker, StopReason,
};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct FakeIo(Cursor<Vec<u8>>);

impl Read for FakeIo {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.0.read(out)
    }
}
impl BufRead for FakeIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.0.fill_buf()
    }
    fn consume(&mut self, size: usize) {
        self.0.consume(size);
    }
}
impl DeviceIo for FakeIo {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        assert!(!request.is_empty());
        assert!(
            self.0.fill_buf()?.is_empty(),
            "interleaved physical transactions"
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
    let (entered, calls) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    (Gate { entered, resume }, calls, release)
}

struct FakeDriver<const PROTOCOL: u8> {
    sequence: i64,
    enabled: bool,
    failure: Option<DriverError>,
    gate: Option<Gate>,
}
impl<const PROTOCOL: u8> FakeDriver<PROTOCOL> {
    fn new() -> Self {
        Self {
            sequence: 0,
            enabled: false,
            failure: None,
            gate: None,
        }
    }
    fn transaction(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        io.send(format!("{PROTOCOL}:{parameter}").as_bytes())?;
        if let Some(gate) = &self.gate {
            gate.entered.send(()).unwrap();
            gate.resume.recv_timeout(WAIT).unwrap();
        }
        let mut response = String::new();
        io.read_line(&mut response)?;
        assert_eq!(response, "ok\n");
        self.failure.take().map_or(Ok(()), Err)
    }
}
impl<const PROTOCOL: u8> Driver for FakeDriver<PROTOCOL> {
    fn parameters(&self) -> Vec<ParameterDescription> {
        [
            (
                "value",
                Spec::Integer {
                    min: 0,
                    max: i64::MAX,
                },
                "count",
            ),
            (
                "level",
                Spec::Float {
                    min: 0.0,
                    max: 100.0,
                },
                "V",
            ),
            ("enabled", Spec::Boolean, ""),
        ]
        .into_iter()
        .map(|(id, spec, unit)| ParameterDescription {
            id: id.into(),
            spec,
            unit: unit.into(),
            writable: id == "enabled",
            default_poll_period: None,
        })
        .collect()
    }
    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.transaction(parameter, io)?;
        Ok(match parameter {
            "value" => {
                self.sequence += 1;
                Value::Integer(self.sequence)
            }
            "level" => Value::Float(2.5),
            "enabled" => Value::Boolean(self.enabled),
            _ => panic!("unknown local parameter"),
        })
    }
    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        self.transaction(parameter, io)?;
        let ("enabled", Value::Boolean(value)) = (parameter, value) else {
            panic!("invalid write")
        };
        self.enabled = *value;
        Ok(())
    }
}

fn device(id: &str, period: Option<Duration>) -> DeviceInstance {
    let mut device = DeviceInstance::new(id, FakeDriver::<1>::new()).unwrap();
    device.set_poll_period("value", period).unwrap();
    device
}

struct RunningPort {
    id: String,
    handle: PortHandle,
    receiver: Option<mpsc::Receiver<DeviceObservation>>,
    join: Option<JoinHandle<StopReason>>,
}
impl RunningPort {
    fn source(&mut self) -> (String, mpsc::Receiver<DeviceObservation>) {
        (self.id.clone(), self.receiver.take().unwrap())
    }
    fn join(&mut self) -> StopReason {
        self.join.take().unwrap().join().unwrap()
    }
    fn stop(&mut self) -> StopReason {
        self.handle.shutdown();
        self.join()
    }
}
impl Drop for RunningPort {
    fn drop(&mut self) {
        self.handle.shutdown();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn start(
    registry: &mut DeviceRegistry,
    id: &str,
    devices: Vec<DeviceInstance>,
    capacity: usize,
) -> RunningPort {
    let (worker, channels) = PortWorker::new(FakeIo::default(), devices, 8, capacity).unwrap();
    registry
        .register_port(id, channels.commands.clone())
        .unwrap();
    for device in worker.devices() {
        registry.register_device(id, device).unwrap();
    }
    RunningPort {
        id: id.into(),
        handle: channels.commands,
        receiver: Some(channels.observations),
        join: Some(thread::spawn(move || worker.run())),
    }
}

fn collect_until(
    aggregator: &mut ObservationAggregator<'_>,
    done: impl Fn(&[AggregatorEvent], bool) -> bool,
) -> Vec<AggregatorEvent> {
    let mut events = Vec::new();
    let limit = Instant::now() + WAIT;
    loop {
        events.extend(aggregator.poll(16));
        if done(&events, aggregator.is_finished()) {
            return events;
        }
        assert!(
            Instant::now() < limit,
            "aggregator made no progress: {events:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn received<T>(mut pending: Pending<T>) -> T {
    let limit = Instant::now() + WAIT;
    loop {
        match pending.poll(Instant::now()) {
            Poll::Waiting(next) => pending = next,
            Poll::Finished(Completion::Received { value, .. }) => return value,
            _ => panic!("unexpected command completion"),
        }
        assert!(Instant::now() < limit);
        thread::sleep(Duration::from_millis(1));
    }
}

fn injected(device: &str, source: &str, reading: Result<Value, String>) -> DeviceObservation {
    DeviceObservation {
        device_id: device.into(),
        observation: Observation {
            source: source.into(),
            unit: "original-unit".into(),
            completed_at: SystemTime::UNIX_EPOCH + Duration::from_millis(123_456),
            reading,
        },
    }
}

#[test]
fn two_workers_multiple_devices_and_value_types_use_global_keys() {
    let period = Some(Duration::from_secs(3600));
    let mut a = device("a", period);
    a.set_poll_period("level", period).unwrap();
    a.set_poll_period("enabled", period).unwrap();
    let mut b = DeviceInstance::new("b", FakeDriver::<2>::new()).unwrap();
    b.set_poll_period("value", period).unwrap();
    let mut registry = DeviceRegistry::new();
    let mut p = start(&mut registry, "p", vec![a, b], 8);
    let mut q = start(&mut registry, "q", vec![device("c", period)], 8);
    let mut aggregator = ObservationAggregator::new(&registry, [p.source(), q.source()]).unwrap();
    let events = collect_until(&mut aggregator, |events, _| events.len() == 5);
    let mut keys = Vec::new();
    for event in events {
        let AggregatorEvent::Measurement {
            port_id,
            global_key,
            observation,
        } = event
        else {
            panic!("unexpected event")
        };
        keys.push(global_key.clone());
        assert_eq!(
            port_id,
            if global_key.starts_with("c/") {
                "q"
            } else {
                "p"
            }
        );
        assert!(observation.completed_at > SystemTime::UNIX_EPOCH);
        match &*observation.source {
            "value" => {
                assert_eq!(observation.reading, Ok(Value::Integer(1)));
                assert_eq!(observation.unit, "count");
            }
            "level" => {
                assert_eq!(observation.reading, Ok(Value::Float(2.5)));
                assert_eq!(observation.unit, "V");
            }
            "enabled" => {
                assert_eq!(observation.reading, Ok(Value::Boolean(false)));
                assert_eq!(observation.unit, "");
            }
            _ => panic!("unexpected local name"),
        }
    }
    keys.sort();
    assert_eq!(
        keys,
        ["a/enabled", "a/level", "a/value", "b/value", "c/value"]
    );
    p.stop();
    q.stop();
    let terminals = collect_until(&mut aggregator, |_, finished| finished);
    assert_eq!(terminals.len(), 2);
    assert!(terminals.iter().all(|event| matches!(
        event,
        AggregatorEvent::PortStopped {
            reason: StopReason::Shutdown,
            ..
        }
    )));
    assert!(aggregator.poll(16).is_empty());
}

#[test]
fn mapping_preserves_original_timestamp_units_values_and_read_errors_exactly() {
    let mut registry = DeviceRegistry::new();
    let _p = start(&mut registry, "p", vec![device("a", None)], 1);
    // Injection is needed to compare exact timestamps and malformed metadata;
    // the registered handle still belongs to a real hardware-free PortWorker.
    let (sender, receiver) = mpsc::sync_channel(4);
    let originals = [
        injected("a", "value", Ok(Value::Integer(23))),
        injected("a", "level", Ok(Value::Float(1.25))),
        injected("a", "enabled", Ok(Value::Boolean(true))),
        injected("a", "value", Err("complete error reply".into())),
    ];
    for original in &originals {
        sender.send(original.clone()).unwrap();
    }
    let mut aggregator = ObservationAggregator::new(&registry, [("p".into(), receiver)]).unwrap();
    for original in originals {
        let events = aggregator.poll(10);
        let [
            AggregatorEvent::Measurement {
                global_key,
                observation,
                ..
            },
        ] = events.as_slice()
        else {
            panic!("{events:?}")
        };
        assert_eq!(global_key, &format!("a/{}", original.observation.source));
        assert_eq!(observation.source, original.observation.source);
        assert_eq!(observation.unit, original.observation.unit);
        assert_eq!(observation.completed_at, original.observation.completed_at);
        assert_eq!(observation.reading, original.observation.reading);
    }
    assert!(aggregator.poll(1).is_empty());
    assert!(!aggregator.is_finished());
}

#[test]
fn unknown_device_parameter_and_wrong_physical_port_are_explicit_errors() {
    let mut registry = DeviceRegistry::new();
    let _p = start(&mut registry, "p", vec![device("a", None)], 1);
    let _q = start(&mut registry, "q", vec![device("b", None)], 1);
    let (sender, receiver) = mpsc::sync_channel(3);
    let cases = [
        (
            injected("missing", "value", Ok(Value::Integer(1))),
            MappingError::UnknownDevice,
        ),
        (
            injected("a", "unknown", Ok(Value::Integer(1))),
            MappingError::UnknownParameter,
        ),
        (
            injected("b", "value", Ok(Value::Integer(1))),
            MappingError::WrongPort {
                registered_port_id: "q".into(),
            },
        ),
    ];
    for (observation, _) in &cases {
        sender.send(observation.clone()).unwrap();
    }
    let mut aggregator = ObservationAggregator::new(&registry, [("p".into(), receiver)]).unwrap();
    for (original, expected_error) in cases {
        let events = aggregator.poll(1);
        let [
            AggregatorEvent::InvalidObservation {
                port_id,
                observation,
                error,
            },
        ] = events.as_slice()
        else {
            panic!("{events:?}")
        };
        assert_eq!(port_id, "p");
        assert_eq!(error, &expected_error);
        assert_eq!(observation.device_id, original.device_id);
        assert_eq!(observation.observation.source, original.observation.source);
    }
}

#[test]
fn source_registration_rejects_unknown_unavailable_and_duplicate_ports() {
    let mut registry = DeviceRegistry::new();
    let _p = start(&mut registry, "p", vec![device("a", None)], 1);
    registry
        .register_unavailable_port("offline", "COM4 unavailable")
        .unwrap();
    registry
        .register_device("offline", &device("offline-device", None))
        .unwrap();
    let (_, receiver) = mpsc::channel();
    assert_eq!(
        ObservationAggregator::new(&registry, [("missing".into(), receiver)]).err(),
        Some(BuildError::UnknownPort("missing".into()))
    );
    let (_, receiver) = mpsc::channel();
    assert_eq!(
        ObservationAggregator::new(&registry, [("offline".into(), receiver)]).err(),
        Some(BuildError::UnavailablePort("offline".into()))
    );
    let (_, first) = mpsc::channel();
    let (_, second) = mpsc::channel();
    assert_eq!(
        ObservationAggregator::new(&registry, [("p".into(), first), ("p".into(), second)]).err(),
        Some(BuildError::DuplicatePort("p".into()))
    );
    let mut empty = ObservationAggregator::new(&registry, []).unwrap();
    assert!(empty.is_finished());
    assert!(empty.poll(usize::MAX).is_empty());
}

#[test]
fn eof_without_worker_stop_is_explicit_and_not_a_fabricated_shutdown() {
    let mut registry = DeviceRegistry::new();
    let _p = start(&mut registry, "p", vec![device("a", None)], 1);
    let (sender, receiver) = mpsc::channel();
    drop(sender);
    let mut aggregator = ObservationAggregator::new(&registry, [("p".into(), receiver)]).unwrap();
    assert!(
        matches!(aggregator.poll(1).as_slice(), [AggregatorEvent::SourceClosedWithoutStop { port_id }] if port_id == "p")
    );
    assert!(aggregator.is_finished());
    assert!(aggregator.poll(1).is_empty());
}

#[test]
fn busy_and_idle_sources_have_bounded_round_robin_turns() {
    let mut registry = DeviceRegistry::new();
    let _p = start(&mut registry, "p", vec![device("busy", None)], 1);
    let _q = start(&mut registry, "q", vec![device("rare", None)], 1);
    let _r = start(&mut registry, "r", vec![device("idle", None)], 1);
    let (busy_sender, busy) = mpsc::sync_channel(100);
    let (rare_sender, rare) = mpsc::sync_channel(1);
    let (_idle_sender, idle) = mpsc::sync_channel(1);
    for n in 0..100 {
        busy_sender
            .send(injected("busy", "value", Ok(Value::Integer(n))))
            .unwrap();
    }
    rare_sender
        .send(injected("rare", "value", Ok(Value::Integer(17))))
        .unwrap();
    let mut aggregator = ObservationAggregator::new(
        &registry,
        [("p".into(), busy), ("q".into(), rare), ("r".into(), idle)],
    )
    .unwrap();
    assert!(aggregator.poll(0).is_empty());
    for expected in [Some("p"), Some("q"), None, Some("p"), None, None] {
        let events = aggregator.poll(1);
        match expected {
            Some(expected) => assert!(
                matches!(events.as_slice(), [AggregatorEvent::Measurement { port_id, .. }] if port_id == expected)
            ),
            None => assert!(events.is_empty()),
        }
    }
    // A huge budget still visits each source only once, not all 98 queued items.
    assert_eq!(aggregator.poll(usize::MAX).len(), 1);
    assert!(!aggregator.is_finished());
}

#[test]
fn overflow_is_detected_after_drain_coalesces_and_does_not_stop_commands() {
    let (gate, entered, release) = gate();
    let mut driver = FakeDriver::<1>::new();
    driver.gate = Some(gate);
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("value", Some(Duration::from_nanos(1)))
        .unwrap();
    let mut registry = DeviceRegistry::new();
    let mut p = start(&mut registry, "p", vec![a], 1);
    let mut q = start(&mut registry, "q", vec![device("b", None)], 1);
    entered.recv_timeout(WAIT).unwrap();
    release.send(()).unwrap();
    entered.recv_timeout(WAIT).unwrap(); // observation 1 fills the queue
    release.send(()).unwrap();
    entered.recv_timeout(WAIT).unwrap(); // observation 2 is lost
    assert_eq!(p.handle.loss_epoch(), 1);
    let drained = p.receiver.as_ref().unwrap().try_recv().unwrap();
    assert_eq!(drained.observation.reading, Ok(Value::Integer(1)));
    let mut aggregator = ObservationAggregator::new(&registry, [p.source(), q.source()]).unwrap();
    assert!(
        matches!(aggregator.poll(2).as_slice(), [AggregatorEvent::DataLost { port_id, loss_epoch: 1 }] if port_id == "p")
    );
    // Three transactions: one queued, then two additional losses.
    for _ in 0..3 {
        release.send(()).unwrap();
        entered.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(p.handle.loss_epoch(), 3);
    let events = aggregator.poll(2);
    assert!(matches!(events.as_slice(), [
        AggregatorEvent::DataLost { port_id, loss_epoch: 3 },
        AggregatorEvent::Measurement { observation: Observation { reading: Ok(Value::Integer(3)), .. }, .. }
    ] if port_id == "p"));
    assert!(aggregator.poll(2).is_empty()); // reading never resets the loss tracker
    let deadline = Instant::now() + WAIT;
    let read = p.handle.read(1, "a", "value", deadline).unwrap();
    release.send(()).unwrap(); // finish the in-flight poll
    entered.recv_timeout(WAIT).unwrap(); // queued read starts
    release.send(()).unwrap();
    entered.recv_timeout(WAIT).unwrap(); // next poll means the read completed
    assert!(received(read).unwrap().observation.reading.is_ok());
    let set = p
        .handle
        .set(2, "a", "enabled", Value::Boolean(true), deadline)
        .unwrap();
    release.send(()).unwrap();
    entered.recv_timeout(WAIT).unwrap(); // queued set starts
    release.send(()).unwrap();
    entered.recv_timeout(WAIT).unwrap(); // poll resumes
    assert_eq!(received(set), Ok(()));
    assert!(p.handle.stop_reason().is_none());
    assert!(
        received(q.handle.read(3, "b", "value", deadline).unwrap())
            .unwrap()
            .observation
            .reading
            .is_ok()
    );
    assert_eq!(q.handle.loss_epoch(), 0);
    p.handle.shutdown();
    release.send(()).unwrap();
    assert_eq!(p.join(), StopReason::Shutdown);
    q.stop();
    let final_epoch = p.handle.loss_epoch();
    assert!(final_epoch > 3);
    let events = collect_until(&mut aggregator, |_, finished| finished);
    assert!(events.iter().any(|event| matches!(event, AggregatorEvent::DataLost { port_id, loss_epoch } if port_id == "p" && *loss_epoch == final_epoch)));
    assert!(
        !events.iter().any(
            |event| matches!(event, AggregatorEvent::DataLost { port_id, .. } if port_id == "q")
        )
    );
    let stop_position = events
        .iter()
        .position(
            |event| matches!(event, AggregatorEvent::PortStopped { port_id, .. } if port_id == "p"),
        )
        .unwrap();
    assert!(events[..stop_position].iter().any(
        |event| matches!(event, AggregatorEvent::Measurement { port_id, .. } if port_id == "p")
    ));
    assert!(!events[stop_position + 1..].iter().any(
        |event| matches!(event, AggregatorEvent::Measurement { port_id, .. } if port_id == "p")
    ));
}

#[test]
fn concurrent_collection_and_worker_losses_preserve_fifo_and_port_isolation() {
    let (gate, entered, release) = gate();
    let mut driver = FakeDriver::<1>::new();
    driver.gate = Some(gate);
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("value", Some(Duration::from_nanos(1)))
        .unwrap();
    let mut registry = DeviceRegistry::new();
    let mut p = start(&mut registry, "p", vec![a], 1);
    let mut q = start(
        &mut registry,
        "q",
        vec![device("b", Some(Duration::from_secs(3600)))],
        8,
    );
    entered.recv_timeout(WAIT).unwrap();
    for _ in 0..2 {
        release.send(()).unwrap();
        entered.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(p.handle.loss_epoch(), 1);
    // Two completed commands fence q's first due poll under the existing
    // command/poll alternation, independent of OS thread startup scheduling.
    for id in [100, 101] {
        received(
            q.handle
                .read(id, "b", "value", Instant::now() + WAIT)
                .unwrap(),
        )
        .unwrap();
    }
    let mut aggregator = ObservationAggregator::new(&registry, [p.source(), q.source()]).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let events = thread::scope(|scope| {
        let consumer_barrier = barrier.clone();
        let consumer = scope.spawn(move || {
            consumer_barrier.wait();
            let mut events = Vec::new();
            let limit = Instant::now() + WAIT;
            while !aggregator.is_finished() {
                let batch = aggregator.poll(1);
                assert!(batch.len() <= 2);
                events.extend(batch);
                assert!(Instant::now() < limit);
                thread::yield_now();
            }
            events
        });
        barrier.wait();
        for _ in 0..64 {
            release.send(()).unwrap();
            entered.recv_timeout(WAIT).unwrap();
        }
        p.handle.shutdown();
        release.send(()).unwrap();
        assert_eq!(p.join(), StopReason::Shutdown);
        q.stop();
        consumer.join().unwrap()
    });
    let mut previous_value = 0;
    let mut epochs = Vec::new();
    let mut stopped = false;
    let mut other_measurements = 0;
    for event in events {
        match event {
            AggregatorEvent::DataLost {
                port_id,
                loss_epoch,
            } => {
                assert_eq!(port_id, "p");
                assert!(!stopped);
                epochs.push(loss_epoch);
            }
            AggregatorEvent::Measurement {
                port_id,
                observation,
                ..
            } if port_id == "p" => {
                assert!(!stopped);
                let Ok(Value::Integer(value)) = observation.reading else {
                    panic!("wrong type")
                };
                assert!(value > previous_value);
                previous_value = value;
            }
            AggregatorEvent::Measurement { port_id, .. } => {
                assert_eq!(port_id, "q");
                other_measurements += 1;
            }
            AggregatorEvent::PortStopped { port_id, reason } => {
                assert_eq!(reason, StopReason::Shutdown);
                if port_id == "p" {
                    stopped = true;
                }
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(stopped);
    assert_eq!(other_measurements, 1);
    assert!(previous_value > 0);
    assert!(epochs.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(epochs.last(), Some(&p.handle.loss_epoch()));
}

#[test]
fn synchronized_measurement_error_does_not_emit_port_stopped() {
    let mut driver = FakeDriver::<1>::new();
    driver.failure = Some(DriverError::new(
        "complete negative reply",
        PortState::Synchronized,
    ));
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("value", Some(Duration::from_secs(3600)))
        .unwrap();
    let mut registry = DeviceRegistry::new();
    let mut p = start(&mut registry, "p", vec![a], 4);
    let mut aggregator = ObservationAggregator::new(&registry, [p.source()]).unwrap();
    let events = collect_until(&mut aggregator, |events, _| !events.is_empty());
    assert!(
        matches!(events.as_slice(), [AggregatorEvent::Measurement { observation: Observation { reading: Err(error), .. }, .. }] if error == "complete negative reply")
    );
    assert!(!aggregator.is_finished());
    assert!(p.handle.stop_reason().is_none());
    assert!(
        received(
            p.handle
                .read(1, "a", "value", Instant::now() + WAIT)
                .unwrap()
        )
        .unwrap()
        .observation
        .reading
        .is_ok()
    );
}

#[test]
fn driver_stop_reason_follows_queued_measurement_and_other_port_continues() {
    for state in [PortState::Desynchronized, PortState::Disconnected] {
        let error = DriverError::new("physical failure", state);
        let mut driver = FakeDriver::<1>::new();
        driver.failure = Some(error.clone());
        let mut a = DeviceInstance::new("a", driver).unwrap();
        a.set_poll_period("value", Some(Duration::from_secs(3600)))
            .unwrap();
        let mut registry = DeviceRegistry::new();
        let mut p = start(&mut registry, "p", vec![a], 2);
        let (gate, entered, release) = gate();
        let mut healthy_driver = FakeDriver::<2>::new();
        healthy_driver.gate = Some(gate);
        let mut b = DeviceInstance::new("b", healthy_driver).unwrap();
        b.set_poll_period("value", Some(Duration::from_nanos(1)))
            .unwrap();
        let mut q = start(&mut registry, "q", vec![b], 8);
        entered.recv_timeout(WAIT).unwrap();
        let reason = StopReason::Driver(error);
        assert_eq!(p.join(), reason);
        let mut aggregator =
            ObservationAggregator::new(&registry, [p.source(), q.source()]).unwrap();
        let events = collect_until(&mut aggregator, |events, _| {
            events.iter().any(|event| matches!(event, AggregatorEvent::PortStopped { port_id, .. } if port_id == "p"))
        });
        assert!(matches!(events.as_slice(), [
            AggregatorEvent::Measurement { port_id, observation: Observation { reading: Err(error), .. }, .. },
            AggregatorEvent::PortStopped { reason: actual, .. }
        ] if port_id == "p" && error == "physical failure" && actual == &reason));
        assert!(!aggregator.is_finished());
        for expected in 1..=3 {
            release.send(()).unwrap();
            entered.recv_timeout(WAIT).unwrap();
            let events = aggregator.poll(1);
            assert!(
                matches!(events.as_slice(), [AggregatorEvent::Measurement { port_id, observation: Observation { reading: Ok(Value::Integer(value)), .. }, .. }] if port_id == "q" && *value == expected)
            );
        }
        q.handle.shutdown();
        release.send(()).unwrap();
        q.join();
        collect_until(&mut aggregator, |_, finished| finished);
    }
}

#[test]
fn other_worker_stop_reason_is_preserved_without_observation_receiver() {
    let mut registry = DeviceRegistry::new();
    let mut p = start(
        &mut registry,
        "p",
        vec![device("a", Some(Duration::from_nanos(1)))],
        1,
    );
    drop(p.receiver.take());
    assert_eq!(p.join(), StopReason::ObservationReceiverDropped);
    // The dropped queue cannot be recovered; a closed test receiver exercises
    // forwarding this existing terminal reason rather than assuming Shutdown.
    let (sender, receiver) = mpsc::channel();
    drop(sender);
    let mut aggregator = ObservationAggregator::new(&registry, [("p".into(), receiver)]).unwrap();
    let events = aggregator.poll(1);
    assert!(matches!(
        events.last(),
        Some(AggregatorEvent::PortStopped {
            reason: StopReason::ObservationReceiverDropped,
            ..
        })
    ));
    assert!(aggregator.is_finished());
}
