use lab_server::{
    command_router::CommandRouter,
    device_registry::DeviceRegistry,
    driver::{DeviceIo, Driver, DriverError},
    fanout::ObservationReceiver,
    model::{Observation, ParameterDescription, SourceMetadata, Spec, Value},
    observation_aggregator::AggregatorEvent,
    port_worker::{DeviceInstance, PortChannels, PortWorker},
    service::DeviceService,
};
use serde_json::{Value as Json, json};
use std::{
    io::{BufReader, Cursor},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
        mpsc::TryRecvError,
    },
    time::{Duration, Instant, SystemTime},
};

struct Source;
impl Driver for Source {
    fn parameters(&self) -> Vec<ParameterDescription> {
        ["x", "y", "power", "backup"]
            .into_iter()
            .map(|name| {
                let writable = matches!(name, "power" | "backup");
                ParameterDescription {
                    id: name.into(),
                    unit: String::new(),
                    writable,
                    spec: if writable {
                        Spec::Integer { min: 0, max: 100 }
                    } else {
                        Spec::Float {
                            min: -100.,
                            max: 100.,
                        }
                    },
                    default_poll_period: (!writable).then_some(Duration::from_secs(1)),
                }
            })
            .collect()
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        Ok(Value::Float(1.))
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        panic!("no I/O in graph fixture")
    }
}
type Worker = PortWorker<BufReader<Cursor<Vec<u8>>>>;
struct Fixture {
    service: Arc<DeviceService>,
    _workers: Vec<Worker>,
    channels: Vec<PortChannels>,
    sequence: [AtomicU64; 2],
}
impl Fixture {
    fn new() -> Self {
        let mut registry = DeviceRegistry::new();
        let mut workers = Vec::new();
        let mut channels = Vec::new();
        for id in ["a", "b"] {
            let (worker, channel) = PortWorker::new(
                BufReader::new(Cursor::new(vec![])),
                vec![DeviceInstance::new(id, Source).unwrap()],
                64,
                64,
            )
            .unwrap();
            registry
                .register_port(id, channel.commands.clone())
                .unwrap();
            registry.register_device(id, &worker.devices()[0]).unwrap();
            workers.push(worker);
            channels.push(channel);
        }
        Self {
            service: Arc::new(DeviceService::new(CommandRouter::new(registry), 128)),
            _workers: workers,
            channels,
            sequence: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
    fn manage(&self, command: Json) -> Json {
        self.service
            .manage(serde_json::from_value(command).unwrap())
            .unwrap()
    }
    fn subscribe(&self, key: &str) -> ObservationReceiver {
        let r = self.service.register_client().unwrap();
        r.interest.set_keys(&[key.into()]);
        r
    }
    fn event(&self, key: &str, value: f64) -> AggregatorEvent {
        let (port, source) = key.split_once('/').unwrap();
        let index = usize::from(port == "b");
        let sequence = self.sequence[index].fetch_add(1, Ordering::SeqCst) + 1;
        AggregatorEvent::Measurement {
            port_id: port.into(),
            global_key: key.into(),
            observation: Observation {
                source: source.into(),
                unit: String::new(),
                completed_at: SystemTime::now(),
                metadata: Some(SourceMetadata {
                    source_instance: self.channels[index].commands.source_instance(),
                    sequence,
                    generation: 0,
                    acquired_at: Instant::now(),
                    elapsed: Duration::from_millis(sequence),
                }),
                reading: Ok(Value::Float(value)),
            },
        }
    }
    fn publish(&self, key: &str, value: f64) {
        self.service.apply_event(self.event(key, value)).unwrap();
    }
}
fn config(name: &str, input: &str) -> Json {
    json!({"name":name,"type_id":"ema","inputs":{"input":input},"parameters":{"tau":1.},"max_age_ms":10000})
}
fn create(f: &Fixture, name: &str, input: &str, revision: u64) {
    f.manage(
        json!({"action":"create","configuration":config(name,input),"expected_revision":revision}),
    );
}

#[test]
fn rewire_closes_only_affected_histories_on_same_or_different_port() {
    for target in ["a/y", "b/y"] {
        let f = Fixture::new();
        create(&f, "smooth", "a/x", 0);
        create(&f, "child", "processing/smooth/value", 1);
        create(&f, "independent", "a/y", 2);
        let old = f.subscribe("processing/smooth/value");
        let child = f.subscribe("processing/child/value");
        let independent = f.subscribe("processing/independent/value");
        f.publish("a/x", 10.);
        // Keep queued old observations; they may drain, but no replacement value may follow.
        f.manage(json!({"action":"configure","configuration":config("smooth",target),"expected_revision":3}));
        let new = f.subscribe("processing/child/value");
        f.publish(target, 20.);
        f.publish("a/y", 30.);
        for r in [&old, &child] {
            assert_eq!(
                r.try_recv().unwrap().observation.reading,
                Ok(Value::Float(10.))
            );
            assert!(matches!(r.try_recv(), Err(TryRecvError::Disconnected)));
            assert_eq!(r.closed_error(), "observation_stream_closed");
            r.interest.set_keys(&[]);
            r.interest.set_keys(&["processing/smooth/value".into()]);
            assert!(r.terminal_when_inactive());
            assert!(matches!(r.try_recv(), Err(TryRecvError::Disconnected)));
        }
        assert_eq!(
            new.try_recv().unwrap().observation.reading,
            Ok(Value::Float(20.))
        );
        assert!(!independent.terminal_when_inactive());
        assert!(independent.try_recv().unwrap().observation.reading.is_ok());
    }
}

#[test]
fn parameter_retune_preserves_active_subscription_and_history() {
    let f = Fixture::new();
    create(&f, "smooth", "a/x", 0);
    let r = f.subscribe("processing/smooth/value");
    f.publish("a/x", 10.);
    let mut c = config("smooth", "a/x");
    c["parameters"]["tau"] = json!(2.);
    f.manage(json!({"action":"configure","configuration":c,"expected_revision":1}));
    f.publish("a/x", 20.);
    assert_eq!(
        r.try_recv().unwrap().observation.reading,
        Ok(Value::Float(10.))
    );
    let Value::Float(v) = r.try_recv().unwrap().observation.reading.unwrap() else {
        panic!()
    };
    assert!((10. ..20.).contains(&v));
    assert!(matches!(r.try_recv(), Err(TryRecvError::Empty)));
    assert!(!r.terminal_when_inactive());
}

#[test]
fn concurrent_rewire_and_publication_never_mix_histories() {
    for _ in 0..32 {
        let f = Fixture::new();
        create(&f, "smooth", "a/x", 0);
        let old = f.subscribe("processing/smooth/value");
        f.publish("a/x", 10.);
        let event = f.event("a/y", 90.);
        let service = f.service.clone();
        let barrier = Arc::new(Barrier::new(2));
        let other = barrier.clone();
        let join = std::thread::spawn(move || {
            other.wait();
            service.apply_event(event).unwrap();
        });
        barrier.wait();
        f.manage(json!({"action":"configure","configuration":config("smooth","a/y"),"expected_revision":1}));
        join.join().unwrap();
        let new = f.subscribe("processing/smooth/value");
        f.publish("a/y", 90.);
        assert_eq!(
            old.try_recv().unwrap().observation.reading,
            Ok(Value::Float(10.))
        );
        assert!(matches!(old.try_recv(), Err(TryRecvError::Disconnected)));
        assert_eq!(old.closed_error(), "observation_stream_closed");
        assert_eq!(
            new.try_recv().unwrap().observation.reading,
            Ok(Value::Float(90.))
        );
    }
}
