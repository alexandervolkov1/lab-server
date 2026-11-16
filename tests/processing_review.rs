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

struct Source(usize);
impl Driver for Source {
    fn parameters(&self) -> Vec<ParameterDescription> {
        ["x", "y", "power", "backup"]
            .into_iter()
            .map(|name| {
                let writable = matches!(name, "power" | "backup");
                ParameterDescription {
                    id: if name == "backup" && self.0 > 0 {
                        "q".repeat(self.0)
                    } else {
                        name.into()
                    },
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
        Self::with_target_len(0)
    }
    fn with_target_len(target_len: usize) -> Self {
        let mut registry = DeviceRegistry::new();
        let mut workers = Vec::new();
        let mut channels = Vec::new();
        for id in ["a", "b"] {
            let (worker, channel) = PortWorker::new(
                BufReader::new(Cursor::new(vec![])),
                vec![DeviceInstance::new(id, Source(target_len)).unwrap()],
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

#[test]
fn binding_snapshots_track_revision_state_and_incarnation() {
    let f = Fixture::new();
    let mut c = config("loop", "a/x");
    c["type_id"] = json!("pid");
    c["parameters"] = json!({});
    let first = f.manage(json!({"action":"create","configuration":c,"expected_revision":0}));
    assert!(first["instances"][0]["binding"].is_null());
    let binding = |target, timeout| json!({"target":target,"command_timeout_ms":timeout});
    for (revision, target, timeout) in [(1, "a/power", 1000), (2, "b/power", 500)] {
        let list = f.manage(json!({"action":"bind","name":"loop","target":target,"command_timeout_ms":timeout,"expected_revision":revision}));
        assert_eq!(list["revision"], revision + 1);
        assert_eq!(list["instances"][0]["binding"], binding(target, timeout));
    }
    let before = f.manage(json!({"action":"list"}));
    assert!(f.service.manage(serde_json::from_value(json!({"action":"bind","name":"loop","target":"a/power","command_timeout_ms":1,"expected_revision":2})).unwrap()).is_err());
    assert_eq!(before, f.manage(json!({"action":"list"})));
    f.publish("a/x", 1.);
    for (revision, state) in [(3, "auto"), (4, "stopped"), (5, "shadow")] {
        let list = f.manage(
            json!({"action":"state","name":"loop","state":state,"expected_revision":revision}),
        );
        assert_eq!(list["instances"][0]["state"], state);
        assert_eq!(list["instances"][0]["binding"], binding("b/power", 500));
        if state == "auto" {
            // Leave an old incarnation's physical request queued through deletion.
            f.publish("a/x", 2.);
        }
    }
    f.service
        .apply_event(AggregatorEvent::DataLost {
            port_id: "a".into(),
            loss_epoch: 1,
        })
        .unwrap();
    let fault = f.manage(json!({"action":"list"}));
    assert_eq!(fault["revision"], 6);
    assert_eq!(fault["instances"][0]["state"], "fault");
    assert_eq!(fault["instances"][0]["binding"], binding("b/power", 500));
    let deleted = f.manage(json!({"action":"delete","name":"loop","expected_revision":6}));
    assert!(deleted["instances"].as_array().unwrap().is_empty());
    let recreated = f.manage(json!({"action":"create","configuration":c,"expected_revision":7}));
    assert!(recreated["instances"][0]["binding"].is_null());
    assert_ne!(
        first["instances"][0]["incarnation"],
        recreated["instances"][0]["incarnation"]
    );
}

#[test]
fn binding_bytes_participate_in_pagination_before_response_is_built() {
    let f = Fixture::with_target_len(4000);
    let target = format!("a/{}", "q".repeat(4000));
    for n in 0..20 {
        let mut c = config(&format!("loop{n:02}"), "a/x");
        c["type_id"] = json!("pid");
        c["parameters"] = json!({});
        f.manage(json!({"action":"create","configuration":c,"expected_revision":2*n}));
        let page = f.manage(json!({"action":"bind","name":format!("loop{n:02}"),"target":target,"command_timeout_ms":500,"expected_revision":2*n+1}));
        assert!(page.to_string().len() <= 48 * 1024);
    }
    let mut offset = 0;
    let mut names = std::collections::BTreeSet::new();
    loop {
        let page = f.manage(json!({"action":"list","offset":offset,"limit":64}));
        assert_eq!(page["revision"], 40);
        assert!(page.to_string().len() <= 48 * 1024);
        let instances = page["instances"].as_array().unwrap();
        assert!(!instances.is_empty());
        assert!(
            instances.len() < 16,
            "byte budget, not item limit, must split this page"
        );
        for i in instances {
            assert_eq!(i["binding"]["target"], target);
            assert!(names.insert(i["configuration"]["name"].as_str().unwrap().to_owned()));
        }
        if page["next_offset"].is_null() {
            break;
        }
        let next = page["next_offset"].as_u64().unwrap();
        assert!(next > offset);
        offset = next;
    }
    assert_eq!(names.len(), 20);
}
