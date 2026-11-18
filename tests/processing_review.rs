use lab_server::{
    command_router::CommandRouter,
    device_registry::DeviceRegistry,
    driver::{DeviceIo, Driver, DriverError},
    fanout::ObservationReceiver,
    model::{Observation, ParameterDescription, SourceMetadata, Spec, Value},
    observation_aggregator::AggregatorEvent,
    port_worker::{DeviceInstance, PortChannels, PortWorker},
    recorder::{Recorder, RecorderOptions},
    service::DeviceService,
    session::Session,
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

#[path = "support/recording_db.rs"]
mod audit_db;

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
fn rejected_controller_proposal_invalidates_the_batch_before_publication() {
    use lab_server::pending::{Completion, Poll};
    use lab_server::port_worker::StopReason;

    let mut f = Fixture::new();
    let database = audit_db::Database::new();
    let recorder = Recorder::with_options(
        &database.0,
        RecorderOptions {
            audit_enabled: true,
            ..Default::default()
        },
    )
    .unwrap();
    let recording = recorder.handle();
    f.service.enable_audit(recording.clone());

    let pid = |name, input| json!({"name":name,"type_id":"pid","inputs":{"input":input},"parameters":{"setpoint":150.,"output_max":200.},"max_age_ms":10000});
    f.manage(json!({"action":"create","configuration":pid("root", "a/x"),"expected_revision":0}));
    create(&f, "dependent_filter", "processing/root/output", 1);
    f.manage(json!({"action":"create","configuration":pid("dependent", "processing/dependent_filter/value"),"expected_revision":2}));
    create(&f, "independent", "a/x", 3);
    f.manage(json!({"action":"bind","name":"root","target":"a/power","command_timeout_ms":10000,"expected_revision":4}));
    f.manage(json!({"action":"bind","name":"dependent","target":"a/backup","command_timeout_ms":10000,"expected_revision":5}));
    f.publish("a/x", 80.);
    f.manage(json!({"action":"state","name":"root","state":"auto","expected_revision":6}));
    f.manage(json!({"action":"state","name":"dependent","state":"auto","expected_revision":7}));

    let keys = [
        ("processing/root/p", false), // Computed before the proposal output.
        ("processing/root/output", false),
        ("processing/dependent_filter/value", false),
        ("processing/dependent/output", false),
        ("processing/independent/value", true),
        ("a/x", true),
    ];
    let subscriptions = keys.map(|(key, _)| f.subscribe(key));
    let mut recorded = Vec::new();
    f.service
        .apply_event_recorded(f.event("a/x", 20.), |event| {
            recording.try_record(event.clone()).unwrap();
            recorded.push(event.clone());
        })
        .unwrap();

    let list = f.manage(json!({"action":"list"}));
    for name in ["root", "dependent"] {
        let controller = list["instances"]
            .as_array()
            .unwrap()
            .iter()
            .find(|node| node["configuration"]["name"] == name)
            .unwrap();
        assert_eq!(controller["state"], "fault", "{name}");
        assert_eq!(controller["fault"], "invalid_output", "{name}");
    }
    let mut session = Session::new(8, 65536);
    for (index, ((key, valid), subscription)) in keys.into_iter().zip(subscriptions).enumerate() {
        let recorded_sample = recorded
            .iter()
            .find_map(|event| match event {
                AggregatorEvent::Measurement {
                    global_key,
                    observation,
                    ..
                } if global_key == key => Some(observation),
                _ => None,
            })
            .unwrap();
        assert_eq!(recorded_sample.reading.is_ok(), valid, "recorded {key}");
        let event = subscription.try_recv().unwrap();
        assert_eq!(
            event.observation.reading, recorded_sample.reading,
            "subscription {key}"
        );
        assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));

        f.service
            .receive_message(
                &mut session,
                &json!({"id":index+1,"op":"get","key":key}).to_string(),
                Instant::now(),
            )
            .unwrap();
        let reply: Json = serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap();
        assert_eq!(
            reply["status"],
            if valid { "ok" } else { "error" },
            "get {key}"
        );
        if let Ok(Value::Float(value)) = recorded_sample.reading {
            assert_eq!(reply["value"], value, "get {key}");
        }
    }

    // A FIFO read behind the batch checks for attempted writes before shutdown.
    // The fixture driver panics if either controller reaches write().
    let port = &f.channels[0].commands;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut read = port.read(1, "a", "x", deadline).unwrap();
    let worker = f._workers.remove(0);
    let join = std::thread::spawn(move || worker.run());
    loop {
        match read.poll(Instant::now()) {
            Poll::Waiting(pending) => {
                read = pending;
                std::thread::yield_now();
            }
            Poll::Finished(Completion::Received { value: Ok(_), .. }) => break,
            _ => panic!("read behind rejected proposals failed"),
        }
    }
    port.shutdown();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);

    recorder.shutdown().unwrap();
    let db = database.connect();
    let measures: Vec<(String, String)> = db
        .prepare("SELECT global_key, value_type FROM measurements ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(measures.len(), recorded.len());
    for (key, value_type) in measures {
        assert_eq!(
            value_type,
            if matches!(key.as_str(), "a/x" | "processing/independent/value") {
                "float"
            } else {
                "error"
            },
            "SQLite {key}"
        );
    }
    let audit: Vec<(String, String, String)> = db.prepare(
        "SELECT json_extract(context_json,'$.component'), kind, COALESCE(json_extract(details_json,'$.error'), '')
         FROM audit_events WHERE json_extract(context_json,'$.source')='controller' ORDER BY id"
    ).unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(
        audit,
        [("root".into(), "rejected".into(), "invalid_output".into())]
    );
}

#[test]
fn stopping_controller_invalidates_its_graph_without_a_false_fault() {
    for auto in [false, true] {
        let f = Fixture::new();
        let database = audit_db::Database::new();
        let recorder = Recorder::with_options(
            &database.0,
            RecorderOptions {
                audit_enabled: true,
                ..Default::default()
            },
        )
        .unwrap();
        f.service.enable_audit(recorder.handle());

        let pid = |name, input| json!({"name":name,"type_id":"pid","inputs":{"input":input},"parameters":{"setpoint":20.},"max_age_ms":60000});
        f.manage(
            json!({"action":"create","configuration":pid("root", "a/x"),"expected_revision":0}),
        );
        create(&f, "filter", "processing/root/output", 1);
        f.manage(json!({"action":"create","configuration":pid("dependent", "processing/filter/value"),"expected_revision":2}));
        let mut revision = 3;
        if auto {
            f.manage(json!({"action":"bind","name":"root","target":"a/power","command_timeout_ms":1000,"expected_revision":revision}));
            revision += 1;
        }

        let keys = [
            "processing/root/output",
            "processing/filter/value",
            "processing/dependent/output",
        ];
        let observations = f.service.register_client().unwrap();
        let mut session = Session::new(16, 65536);
        session.bind_observations(&observations);
        let mut request_id = 0;
        let mut request = |session: &mut Session, mut body: Json| {
            request_id += 1;
            body["id"] = json!(request_id);
            f.service
                .receive_message(session, &body.to_string(), Instant::now())
                .unwrap();
            serde_json::from_str::<Json>(&session.next_message().unwrap().unwrap()).unwrap()
        };
        assert_eq!(
            request(&mut session, json!({"op":"subscribe","keys":keys}))["status"],
            "ok"
        );
        f.publish("a/x", 10.);
        session.collect_observations(&observations).unwrap();
        for key in keys {
            let event: Json =
                serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap();
            assert_eq!(event["key"], key);
            assert_eq!(event["status"], "ok");
        }
        for key in keys {
            assert_eq!(
                request(&mut session, json!({"op":"get","key":key}))["value"],
                10.
            );
        }
        if auto {
            let snapshot = f.manage(
                json!({"action":"state","name":"root","state":"auto","expected_revision":revision}),
            );
            assert_eq!(snapshot["instances"][2]["state"], "auto");
            revision += 1;
        }

        let stop = request(
            &mut session,
            json!({"op":"components","command":{"action":"state","name":"root","state":"stopped","expected_revision":revision}}),
        );
        assert_eq!(stop["status"], "ok");
        assert_eq!(stop["data"]["revision"], revision + 1);
        let instances = stop["data"]["instances"].as_array().unwrap();
        for (name, state) in [
            ("root", "stopped"),
            ("filter", "running"),
            ("dependent", "fault"),
        ] {
            let instance = instances
                .iter()
                .find(|node| node["configuration"]["name"] == name)
                .unwrap();
            assert_eq!(instance["state"], state);
            assert_eq!(instance["fault"], "component_stopped");
        }
        for key in keys {
            assert_eq!(
                request(&mut session, json!({"op":"get","key":key}))["status"],
                "no_data"
            );
        }
        session.collect_observations(&observations).unwrap();
        assert!(session.next_message().unwrap().is_none());
        f.publish("a/x", 11.);
        session.collect_observations(&observations).unwrap();
        for key in keys {
            let event: Json =
                serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap();
            assert_eq!(event["key"], key);
            assert_eq!(event["status"], "error");
            assert!(event.get("value").is_none());
        }
        assert!(session.next_message().unwrap().is_none());

        f.manage(json!({"action":"state","name":"root","state":"shadow","expected_revision":revision + 1}));
        f.service
            .apply_event(AggregatorEvent::DataLost {
                port_id: "a".into(),
                loss_epoch: 1,
            })
            .unwrap();
        recorder.shutdown().unwrap();
        let db = database.connect();
        let mut faults = db.prepare(
            "SELECT json_extract(context_json,'$.component'),
                    json_extract(context_json,'$.request.reason'),
                    json_extract(details_json,'$.state_before')
             FROM audit_events WHERE kind='applied' AND json_extract(details_json,'$.state_after')='fault'
             ORDER BY id"
        ).unwrap();
        let faults = faults
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            faults,
            [
                (
                    "dependent".into(),
                    "component_stopped".into(),
                    "shadow".into()
                ),
                ("root".into(), "source_data_lost".into(), "shadow".into()),
            ],
            "root started in {}",
            if auto { "Auto" } else { "Shadow" }
        );
        assert_eq!(
            audit_db::scalar(
                &db,
                "SELECT count(*) FROM audit_events WHERE kind='applied' AND json_extract(context_json,'$.source')='tcp' AND json_extract(context_json,'$.request.command.state')='stopped'"
            ),
            1
        );
        assert_eq!(
            audit_db::scalar(
                &db,
                "SELECT count(*) FROM audit_events WHERE json_extract(context_json,'$.source')='controller'"
            ),
            0
        );
    }
}

#[test]
fn server_status_preserves_controller_output_ownership_subscriptions_and_faults() {
    let f = Fixture::new();
    create(&f, "smooth", "a/x", 0);
    let mut c = config("loop", "processing/smooth/value");
    c["type_id"] = json!("pid");
    c["parameters"] = json!({"setpoint":20.});
    f.manage(json!({"action":"create","configuration":c,"expected_revision":1}));
    f.manage(json!({"action":"bind","name":"loop","target":"a/power","command_timeout_ms":1000,"expected_revision":2}));
    f.publish("a/x", 10.);
    f.manage(json!({"action":"state","name":"loop","state":"auto","expected_revision":3}));
    let list = f.manage(json!({"action":"list"}));
    let outputs = f.manage(json!({"action":"outputs"}));
    let receiver = f.subscribe("processing/loop/setpoint");
    let mut session = lab_server::session::Session::new(64, 65536);
    f.service
        .receive_message(
            &mut session,
            r#"{"id":1,"op":"subscribe","keys":["processing/loop/setpoint"]}"#,
            Instant::now(),
        )
        .unwrap();
    assert!(session.next_message().unwrap().unwrap().contains("\"ok\""));
    let keys = session.subscriptions.keys();
    for id in 2..20 {
        f.service
            .receive_message(
                &mut session,
                &json!({"id":id,"op":"server_status"}).to_string(),
                Instant::now(),
            )
            .unwrap();
        let response: Json =
            serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap();
        assert_eq!(
            response["data"]["processing"],
            json!([
                {"name":"loop","type_id":"pid","state":"auto","error":null},
                {"name":"smooth","type_id":"ema","state":"running","error":null}
            ])
        );
        assert_eq!(session.subscriptions.keys(), keys);
        assert!(session.pending.is_empty());
        assert!(session.next_message().unwrap().is_none());
    }
    assert_eq!(f.manage(json!({"action":"list"})), list);
    assert_eq!(f.manage(json!({"action":"outputs"})), outputs);
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    // Explicitly leave Auto before publishing, so this fixture never writes.
    f.manage(json!({"action":"state","name":"loop","state":"shadow","expected_revision":4}));
    f.publish("a/x", 11.);
    assert_eq!(
        receiver.try_recv().unwrap().observation.reading,
        Ok(Value::Float(20.))
    );
    f.service
        .apply_event(AggregatorEvent::DataLost {
            port_id: "a".into(),
            loss_epoch: 1,
        })
        .unwrap();
    let fault = f.manage(json!({"action":"list"}));
    let status = f.service.diagnostics().unwrap();
    assert_eq!(status["processing"][0]["state"], "fault");
    assert_eq!(
        status["processing"][0]["error"],
        fault["instances"][0]["fault"]
    );
    assert!(status["processing"][0]["error"].is_string());
    assert_eq!(f.manage(json!({"action":"list"})), fault);
}

#[test]
fn parameter_set_is_atomic_and_concurrent_edits_preserve_other_parameters() {
    let f = Fixture::new();
    let mut c = config("loop", "a/x");
    c["type_id"] = json!("pid");
    c["parameters"] = json!({"setpoint":20.,"ki":0.2});
    let before = f.manage(json!({"action":"create","configuration":c,"expected_revision":0}));
    for (name, parameter, value, revision, error) in [
        ("missing", "kp", json!(2.), None, "unknown_instance"),
        ("loop", "missing", json!(2.), None, "unknown_parameter"),
        ("loop", "kp", json!(true), None, "invalid_parameter:kp"),
        ("loop", "kp", json!("2"), None, "invalid_parameter:kp"),
        ("loop", "kp", json!(-1.), None, "invalid_parameter:kp"),
        ("loop", "kp", Json::Null, None, "invalid_parameter:kp"),
        ("loop", "output_min", json!(101.), None, "invalid_limits"),
        ("loop", "kp", json!(2.), Some(0), "revision_conflict"),
    ] {
        let cmd = json!({"action":"parameter_set","name":name,"parameter":parameter,"value":value,"expected_revision":revision});
        assert_eq!(
            f.service
                .manage(serde_json::from_value(cmd).unwrap())
                .unwrap_err(),
            error
        );
        assert_eq!(f.manage(json!({"action":"list"})), before);
    }
    let barrier = Arc::new(Barrier::new(3));
    let joins: Vec<_> = [("setpoint", 35.), ("kp", 3.)]
        .into_iter()
        .map(|(p, v)| {
            let service = f.service.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                service
                    .manage(
                        serde_json::from_value(
                            json!({"action":"parameter_set","name":"loop","parameter":p,"value":v}),
                        )
                        .unwrap(),
                    )
                    .unwrap()["revision"]
                    .as_u64()
                    .unwrap()
            })
        })
        .collect();
    barrier.wait();
    let mut revisions: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
    revisions.sort();
    assert_eq!(revisions, [2, 3]);
    let mut expected = before;
    expected["revision"] = json!(3);
    expected["instances"][0]["configuration"]["parameters"]["setpoint"] = json!(35.);
    expected["instances"][0]["configuration"]["parameters"]["kp"] = json!(3.);
    assert_eq!(f.manage(json!({"action":"list"})), expected);
    let after = f.manage(json!({"action":"parameter_set","name":"loop","parameter":"ki","value":0.3,"expected_revision":3}));
    assert_eq!(after["revision"], 4);
}

#[test]
fn dynamic_filter_replacement_requires_explicit_controller_recovery() {
    let mut f = Fixture::new();
    create(&f, "smooth", "a/x", 0);
    let mut c = config("loop", "processing/smooth/value");
    c["type_id"] = json!("pid");
    c["parameters"] = json!({"setpoint":20.,"ki":1.});
    f.manage(json!({"action":"create","configuration":c,"expected_revision":1}));
    f.manage(json!({"action":"bind","name":"loop","target":"a/power","command_timeout_ms":1000,"expected_revision":2}));
    f.publish("a/x", 10.);
    f.manage(json!({"action":"state","name":"loop","state":"auto","expected_revision":3}));
    let auto = f.manage(json!({"action":"list"}));
    assert_eq!(f.manage(json!({"action":"outputs"}))[1]["mode"], "auto");
    assert_eq!(f.service.manage(serde_json::from_value(json!({"action":"parameter_set","name":"loop","parameter":"setpoint","value":35.})).unwrap()).unwrap_err(), "stop_before_configure");
    assert_eq!(f.manage(json!({"action":"list"})), auto);
    f.manage(json!({"action":"state","name":"loop","state":"shadow","expected_revision":4}));
    assert!(
        f.manage(json!({"action":"outputs"}))
            .as_array()
            .unwrap()
            .iter()
            .all(|o| o["mode"] == "manual" && o["owner"].is_null())
    );
    let history = f.subscribe("processing/loop/i");
    f.publish("a/x", 10.);
    f.publish("a/x", 10.);
    assert_eq!(
        history.try_recv().unwrap().observation.reading,
        Ok(Value::Float(0.))
    );
    assert!(
        matches!(history.try_recv().unwrap().observation.reading, Ok(Value::Float(v)) if v > 0.)
    );
    create(&f, "smooth2", "a/x", 5);
    c["inputs"]["input"] = json!("processing/smooth2/value");
    let rewired = f.manage(json!({"action":"configure","configuration":c,"expected_revision":6}));
    assert_eq!(rewired["instances"][0]["state"], "fault");
    assert_eq!(rewired["instances"][0]["fault"], "input_changed");
    assert_eq!(history.closed_error(), "observation_stream_closed");
    assert!(matches!(
        history.try_recv(),
        Err(TryRecvError::Disconnected)
    ));
    for command in [
        json!({"action":"delete","name":"smooth2","expected_revision":7}),
        json!({"action":"state","name":"loop","state":"auto","expected_revision":7}),
    ] {
        assert!(
            f.service
                .manage(serde_json::from_value(command).unwrap())
                .is_err()
        );
        assert_eq!(f.manage(json!({"action":"list"})), rewired);
    }
    f.manage(json!({"action":"state","name":"loop","state":"shadow","expected_revision":7}));
    let new = f.subscribe("processing/loop/i");
    f.publish("a/x", 10.);
    assert_eq!(
        new.try_recv().unwrap().observation.reading,
        Ok(Value::Float(0.))
    );
    c["inputs"]["input"] = json!("processing/smooth/value");
    f.manage(json!({"action":"configure","configuration":c,"expected_revision":8}));
    assert_eq!(new.closed_error(), "observation_stream_closed");
    let deleted = f.manage(json!({"action":"delete","name":"smooth2","expected_revision":9}));
    assert_eq!(deleted["instances"].as_array().unwrap().len(), 2);
    f.manage(json!({"action":"state","name":"loop","state":"shadow","expected_revision":10}));
    f.publish("a/x", 10.);
    f.manage(json!({"action":"state","name":"loop","state":"auto","expected_revision":11}));
    f.manage(json!({"action":"state","name":"loop","state":"shadow","expected_revision":12}));
    // Drain the command queue through a read barrier. Source::write panics on ANY
    // physical write, including an implicit zero on release or reconfiguration.
    let worker = f._workers.remove(0);
    let join = std::thread::spawn(move || worker.run());
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut read = f.channels[0].commands.read(99, "a", "x", deadline).unwrap();
    loop {
        match read.poll(Instant::now()) {
            lab_server::pending::Poll::Waiting(p) => {
                read = p;
                std::thread::yield_now();
            }
            lab_server::pending::Poll::Finished(lab_server::pending::Completion::Received {
                value: Ok(_),
                ..
            }) => break,
            _ => panic!("queue barrier failed"),
        }
    }
    f.channels[0].commands.shutdown();
    join.join().unwrap();
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
    f.manage(json!({"action":"parameter_set","name":"smooth","parameter":"tau","value":2.}));
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
        let Value::Float(value) = new.try_recv().unwrap().observation.reading.unwrap() else {
            panic!("numeric output required")
        };
        assert!((value - 90.).abs() < 1e-10);
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
    // Seven PID outputs leave room for 16 instances plus long physical keys.
    for n in 0..16 {
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
        assert_eq!(page["revision"], 32);
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
    assert_eq!(names.len(), 16);
}
