#[path = "support/recording_db.rs"]
mod audit_db;
use lab_server::{
    command_router::CommandRouter,
    device_registry::DeviceRegistry,
    driver::{DeviceIo, Driver, DriverError},
    model::*,
    observation_aggregator::AggregatorEvent,
    port_worker::{DeviceInstance, PortWorker},
    processing::Management,
    service::DeviceService,
    session::Session,
};
use serde_json::json;
use std::{
    io::{BufReader, Cursor},
    time::{Duration, Instant, SystemTime},
};
struct Source;
impl Driver for Source {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "raw".into(),
            unit: "C".into(),
            spec: Spec::Float {
                min: -100.,
                max: 100.,
            },
            writable: false,
            default_poll_period: Some(Duration::from_secs(1)),
        }]
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        Ok(Value::Float(0.))
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        panic!("no writes")
    }
}
#[test]
fn ema_chain_retune_loss_old_queue_and_dynamic_catalog() {
    let (worker, channels) = PortWorker::new(
        BufReader::new(Cursor::new(vec![])),
        vec![DeviceInstance::new("a", Source).unwrap()],
        8,
        8,
    )
    .unwrap();
    let instance = channels.commands.source_instance();
    let mut registry = DeviceRegistry::new();
    registry.register_port("p", channels.commands).unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let service = DeviceService::new(CommandRouter::new(registry), 32);
    for (rev, name, input) in [(0, "one", "a/raw"), (1, "two", "processing/one/value")] {
        service.manage(serde_json::from_value::<Management>(json!({"action":"create","expected_revision":rev,"configuration":{"name":name,"type_id":"ema","inputs":{"input":input},"parameters":{"tau":1.},"max_age_ms":10000}})).unwrap()).unwrap();
    }
    let start = Instant::now();
    let event = |sequence, generation, seconds, value| AggregatorEvent::Measurement {
        port_id: "p".into(),
        global_key: "a/raw".into(),
        observation: Observation {
            source: "raw".into(),
            unit: "C".into(),
            completed_at: SystemTime::now(),
            metadata: Some(SourceMetadata {
                source_instance: instance,
                sequence,
                generation,
                acquired_at: start + Duration::from_secs(seconds),
                elapsed: Duration::from_secs(seconds),
            }),
            reading: Ok(Value::Float(value)),
        },
    };
    let mut recorded = Vec::new();
    service
        .apply_event_recorded(event(1, 0, 0, 0.), |e| recorded.push(e.clone()))
        .unwrap();
    assert_eq!(recorded.len(), 3);
    service.apply_event(event(2, 0, 1, 10.)).unwrap();
    let get = |id| {
        let mut session = Session::new(64, 65536);
        service
            .receive_message(
                &mut session,
                &json!({"id":id,"op":"get","key":"processing/one/value"}).to_string(),
                Instant::now(),
            )
            .unwrap();
        serde_json::from_str::<serde_json::Value>(&session.next_message().unwrap().unwrap())
            .unwrap()
    };
    assert!((get(1)["value"].as_f64().unwrap() - 10. * (1. - (-1f64).exp())).abs() < 1e-10);
    service.manage(serde_json::from_value(json!({"action":"configure","expected_revision":2,"configuration":{"name":"one","type_id":"ema","inputs":{"input":"a/raw"},"parameters":{"tau":10.},"max_age_ms":10000}})).unwrap()).unwrap();
    service.apply_event(event(3, 0, 2, 10.)).unwrap();
    assert!(get(1)["value"].as_f64().unwrap() < 7.);
    service
        .apply_event(AggregatorEvent::DataLost {
            port_id: "p".into(),
            loss_epoch: 1,
        })
        .unwrap();
    service.apply_event(event(4, 0, 3, 90.)).unwrap();
    assert_eq!(get(1)["status"], "error");
    service.apply_event(event(6, 1, 5, 20.)).unwrap();
    assert_eq!(get(1)["value"], 20.);
    service
        .manage(
            serde_json::from_value(
                json!({"action":"delete","name":"one","cascade":true,"expected_revision":3}),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(get(1)["error"], "unknown_key");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteResult {
    Success,
    Failure,
    LateSuccess,
    LateFailure,
    SuccessAfterTimeout,
}

#[test]
fn stopping_auto_preserves_state_after_queued_write_cancellation() {
    check_controller_writes(&[
        (Some("stopped"), WriteResult::Success),
        (Some("shadow"), WriteResult::Success),
        (Some("shutdown"), WriteResult::Success),
    ]);
}

#[test]
fn auto_uses_router_integer_output_and_faults_on_driver_error() {
    check_controller_writes(&[(None, WriteResult::Success), (None, WriteResult::Failure)]);
}

#[test]
fn stopping_auto_preserves_started_write_results_and_the_unknown_latch() {
    check_controller_writes(&[
        (Some("stopped"), WriteResult::LateSuccess),
        (Some("shadow"), WriteResult::LateFailure),
        (Some("shadow"), WriteResult::SuccessAfterTimeout),
    ]);
}

fn check_controller_writes(cases: &[(Option<&str>, WriteResult)]) {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::thread;

    struct WriteGate {
        entered: mpsc::Sender<()>,
        resume: mpsc::Receiver<()>,
    }
    struct Actuator {
        writes: Arc<AtomicUsize>,
        fail: bool,
        gate: Option<WriteGate>,
    }
    impl Driver for Actuator {
        fn parameters(&self) -> Vec<ParameterDescription> {
            let mut p = Source.parameters();
            p.push(ParameterDescription {
                id: "power".into(),
                unit: "%".into(),
                spec: Spec::Integer { min: 0, max: 100 },
                writable: true,
                default_poll_period: None,
            });
            p
        }
        fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
            Ok(Value::Float(1.))
        }
        fn write(
            &mut self,
            _: &str,
            value: &Value,
            _: &mut dyn DeviceIo,
        ) -> Result<(), DriverError> {
            assert_eq!(*value, Value::Integer(3));
            self.writes.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = self.gate.take() {
                gate.entered.send(()).unwrap();
                gate.resume.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            if self.fail {
                Err(DriverError::new(
                    "write failed",
                    lab_server::driver::PortState::Synchronized,
                ))
            } else {
                Ok(())
            }
        }
    }
    use WriteResult::*;
    for &(stop, write_result) in cases {
        let stop_during_write = matches!(
            write_result,
            LateSuccess | LateFailure | SuccessAfterTimeout
        );
        let cancelled = stop.is_some() && !stop_during_write;
        let fail_write = matches!(write_result, Failure | LateFailure);
        let command_timeout = if write_result == SuccessAfterTimeout {
            3000
        } else {
            10000
        };
        let database = audit_db::Database::new();
        let recorder = lab_server::recorder::Recorder::with_options(
            &database.0,
            lab_server::recorder::RecorderOptions {
                audit_enabled: true,
                ..Default::default()
            },
        )
        .unwrap();
        let writes = Arc::new(AtomicUsize::new(0));
        let (entered, started) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let (worker, channels) = PortWorker::new(
            BufReader::new(Cursor::new(vec![])),
            vec![
                DeviceInstance::new(
                    "a",
                    Actuator {
                        writes: writes.clone(),
                        fail: fail_write,
                        gate: stop_during_write.then_some(WriteGate { entered, resume }),
                    },
                )
                .unwrap(),
            ],
            8,
            8,
        )
        .unwrap();
        let instance = channels.commands.source_instance();
        let mut registry = DeviceRegistry::new();
        registry
            .register_port("p", channels.commands.clone())
            .unwrap();
        registry.register_device("p", &worker.devices()[0]).unwrap();
        let router = CommandRouter::new(registry);
        let service = DeviceService::new(router.clone(), 32);
        service.enable_audit(recorder.handle());
        let manage = |command| service.manage(serde_json::from_value(command).unwrap());
        manage(json!({"action":"create","expected_revision":0,"configuration":{"name":"loop","type_id":"pid","inputs":{"input":"a/raw"},"parameters":{"setpoint":3.5},"max_age_ms":60000}})).unwrap();
        manage(json!({"action":"bind","name":"loop","target":"a/power","command_timeout_ms":command_timeout,"expected_revision":1})).unwrap();
        let event = |seq| AggregatorEvent::Measurement {
            port_id: "p".into(),
            global_key: "a/raw".into(),
            observation: Observation {
                source: "raw".into(),
                unit: "C".into(),
                completed_at: SystemTime::now(),
                metadata: Some(SourceMetadata {
                    source_instance: instance,
                    sequence: seq,
                    generation: 0,
                    acquired_at: Instant::now(),
                    elapsed: Duration::from_millis(seq),
                }),
                reading: Ok(Value::Float(1.)),
            },
        };
        service.apply_event(event(1)).unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 0);
        manage(json!({"action":"state","name":"loop","state":"auto","expected_revision":2}))
            .unwrap();
        assert!(
            router
                .set(
                    1,
                    "a/power",
                    Value::Integer(2),
                    Instant::now() + Duration::from_secs(1)
                )
                .is_err()
        );
        service.apply_event(event(2)).unwrap();
        let deadline = Instant::now() + Duration::from_millis(command_timeout);
        let stop_controller = || match stop {
            Some("shutdown") => service.stop_controls(),
            Some(state) => {
                manage(json!({"action":"state","name":"loop","state":state,"expected_revision":3}))
                    .unwrap();
            }
            None => {}
        };
        if cancelled {
            // The worker has not been started, so the proposal is still queued.
            stop_controller();
        }
        let join = thread::spawn(move || worker.run());
        if stop_during_write {
            started.recv_timeout(Duration::from_secs(5)).unwrap();
            stop_controller();
            let output = &manage(json!({"action":"outputs"})).unwrap()[0];
            assert_eq!(output["mode"], "manual");
            assert_eq!(output["in_flight"], 1);
            assert_eq!(output["outcome_unknown"], false);
            if write_result == SuccessAfterTimeout {
                // The driver's channel keeps the call in flight across the deadline.
                while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                    thread::sleep(remaining);
                }
                service.tick_recorded(|_| {}).unwrap();
                let output = &manage(json!({"action":"outputs"})).unwrap()[0];
                assert_eq!(output["in_flight"], 1);
                assert_eq!(output["outcome_unknown"], true);
                assert_eq!(
                    manage(json!({"action":"list"})).unwrap()["instances"][0]["fault"],
                    "output_failed"
                );
            }
            release.send(()).unwrap();
        }
        let end = Instant::now() + Duration::from_secs(3);
        // A read queued behind the mutation proves the worker crossed its start boundary.
        let mut read = channels.commands.read(7, "a", "raw", end).unwrap();
        loop {
            match read.poll(Instant::now()) {
                lab_server::pending::Poll::Waiting(p) => {
                    read = p;
                    std::thread::yield_now();
                }
                lab_server::pending::Poll::Finished(
                    lab_server::pending::Completion::Received { value: Ok(_), .. },
                ) => break,
                _ => panic!("worker failed"),
            }
        }
        assert_eq!(
            writes.load(Ordering::SeqCst),
            usize::from(!cancelled),
            "{stop:?} {write_result:?}"
        );
        if let Some(state) = stop.filter(|_| cancelled || write_result == LateSuccess) {
            service.tick_recorded(|_| {}).unwrap();
            let list = manage(json!({"action":"list"})).unwrap();
            let controller = &list["instances"][0];
            let (expected_state, expected_fault) = match state {
                "shutdown" => ("fault", json!("shutdown")),
                "stopped" => ("stopped", json!("component_stopped")),
                _ => ("shadow", json!(null)),
            };
            assert_eq!(controller["state"], expected_state, "after {state}");
            assert_eq!(controller["fault"], expected_fault, "after {state}");
            assert_eq!(
                manage(json!({"action":"outputs"})).unwrap()[0]["mode"],
                "manual"
            );
        }
        if write_result == Failure {
            let mut recorded = Vec::new();
            service.tick_recorded(|e| recorded.push(e.clone())).unwrap();
            assert_eq!(recorded.len(), 7);
            assert!(recorded.iter().all(|e| matches!(e, AggregatorEvent::Measurement { observation, .. } if observation.reading.is_err())));
            let list = manage(json!({"action":"list"})).unwrap();
            assert_eq!(list["instances"][0]["state"], "fault");
            assert_eq!(list["instances"][0]["fault"], "output_failed");
            let outputs = manage(json!({"action":"outputs"})).unwrap();
            assert_eq!(outputs[0]["mode"], "manual");
            assert_eq!(outputs[0]["outcome_unknown"], true);
            service
                .tick_recorded(|_| panic!("fault must only be published once"))
                .unwrap();
            assert_eq!(writes.load(Ordering::SeqCst), 1);
        }
        service.tick_recorded(|_| {}).unwrap();
        if matches!(write_result, LateFailure | SuccessAfterTimeout) {
            let list = manage(json!({"action":"list"})).unwrap();
            assert_eq!(list["instances"][0]["state"], "fault");
            assert_eq!(list["instances"][0]["fault"], "output_failed");
            let output = &manage(json!({"action":"outputs"})).unwrap()[0];
            assert_eq!(output["in_flight"], 0);
            assert_eq!(output["outcome_unknown"], true);
            assert_eq!(
                router.authority().acquire("a/power", ("new".into(), 2)),
                Err("output_fault")
            );
        }
        assert_eq!(writes.load(Ordering::SeqCst), usize::from(!cancelled));
        service.stop_controls();
        channels.commands.shutdown();
        join.join().unwrap();
        recorder.shutdown().unwrap();
        let c = database.connect();
        let controller_events = c.prepare(
            "SELECT kind FROM audit_events WHERE json_extract(context_json,'$.source')='controller' ORDER BY sequence"
        ).unwrap().query_map([], |row| row.get::<_, String>(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        let expected: &[&str] = if cancelled {
            &["requested", "accepted", "cancelled", "controller_result"]
        } else if write_result == SuccessAfterTimeout {
            &[
                "requested",
                "accepted",
                "started",
                "controller_result",
                "completed",
            ]
        } else {
            &[
                "requested",
                "accepted",
                "started",
                "completed",
                "controller_result",
            ]
        };
        assert_eq!(controller_events, expected);
        let status: String = c.query_row(
            "SELECT json_extract(details_json,'$.status') FROM audit_events WHERE kind='controller_result'", [], |row| row.get(0)
        ).unwrap();
        assert_eq!(
            status,
            if cancelled {
                "cancelled"
            } else if fail_write || write_result == SuccessAfterTimeout {
                "error"
            } else {
                "ok"
            }
        );
        if fail_write {
            assert_eq!(
                audit_db::scalar(
                    &c,
                    "SELECT count(*) FROM audit_events WHERE kind='completed' AND json_extract(details_json,'$.message')='write failed'"
                ),
                1
            );
        }
        if write_result == SuccessAfterTimeout {
            assert_eq!(
                audit_db::scalar(
                    &c,
                    "SELECT count(*) FROM audit_events WHERE kind='controller_result' AND json_extract(details_json,'$.error')='outcome_unknown'"
                ),
                1
            );
            assert_eq!(
                audit_db::scalar(
                    &c,
                    "SELECT count(*) FROM audit_events WHERE kind='completed' AND json_extract(details_json,'$.status')='ok'"
                ),
                1
            );
        }
        assert_eq!(
            audit_db::scalar(
                &c,
                "SELECT count(*) FROM audit_events WHERE json_extract(context_json,'$.component')='loop' AND json_extract(context_json,'$.incarnation')='1' AND kind='requested' AND json_extract(details_json,'$.value')=3"
            ),
            1
        );
        assert_eq!(
            audit_db::scalar(
                &c,
                "SELECT count(*) FROM audit_events WHERE kind='applied' AND json_extract(context_json,'$.request.command.state')='auto' AND json_extract(details_json,'$.revision_before')=2 AND json_extract(details_json,'$.revision_after')=3"
            ),
            1
        );
    }
}

#[test]
fn audit_management_records_arguments_revisions_and_shadow_transition() {
    let database = audit_db::Database::new();
    let recorder = lab_server::recorder::Recorder::with_options(
        &database.0,
        lab_server::recorder::RecorderOptions {
            audit_enabled: true,
            ..Default::default()
        },
    )
    .unwrap();
    let (worker, channels) = PortWorker::new(
        BufReader::new(Cursor::new(vec![])),
        vec![DeviceInstance::new("a", Source).unwrap()],
        8,
        8,
    )
    .unwrap();
    let mut registry = DeviceRegistry::new();
    registry.register_port("p", channels.commands).unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let service = DeviceService::new(CommandRouter::new(registry), 8);
    service.enable_audit(recorder.handle());
    let mut session = Session::new(8, 65536);
    let config = json!({"name":"loop","type_id":"pid","inputs":{"input":"a/raw"},"parameters":{},"max_age_ms":10000});
    let commands = [
        json!({"action":"create","configuration":config,"expected_revision":0}),
        json!({"action":"parameter_set","name":"loop","parameter":"kp","value":2.0,"expected_revision":1}),
        json!({"action":"state","name":"loop","state":"stopped","expected_revision":2}),
        json!({"action":"state","name":"loop","state":"shadow","expected_revision":3}),
        json!({"action":"configure","configuration":config,"expected_revision":4}),
        json!({"action":"delete","name":"loop","expected_revision":5}),
    ];
    for (i, command) in commands.iter().enumerate() {
        service
            .receive_message(
                &mut session,
                &json!({"id":i+1,"op":"components","command":command}).to_string(),
                Instant::now(),
            )
            .unwrap();
        let reply: serde_json::Value =
            serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap();
        assert_eq!(reply["status"], "ok", "{reply}");
    }
    recorder.shutdown().unwrap();
    let c = database.connect();
    assert_eq!(
        audit_db::scalar(
            &c,
            "SELECT count(*) FROM audit_events WHERE kind='applied' AND json_extract(details_json,'$.revision_after')=json_extract(details_json,'$.revision_before')+1"
        ),
        6
    );
    assert_eq!(
        audit_db::scalar(
            &c,
            "SELECT count(*) FROM audit_events WHERE kind='applied' AND json_extract(context_json,'$.request.command.action')='parameter_set' AND json_extract(context_json,'$.request.command.value')=2.0"
        ),
        1
    );
    assert_eq!(
        audit_db::scalar(
            &c,
            "SELECT count(*) FROM audit_events WHERE kind='applied' AND json_extract(context_json,'$.request.command.state')='shadow'"
        ),
        1
    );
}
