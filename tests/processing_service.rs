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

#[test]
fn auto_uses_router_integer_output_and_stop_revokes_queued_commands() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Actuator(Arc<AtomicUsize>, bool);
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
            self.0.fetch_add(1, Ordering::SeqCst);
            if self.1 {
                Err(DriverError::new(
                    "write failed",
                    lab_server::driver::PortState::Synchronized,
                ))
            } else {
                Ok(())
            }
        }
    }
    for (stop_before_start, fail_write) in [(true, false), (false, false), (false, true)] {
        let writes = Arc::new(AtomicUsize::new(0));
        let (worker, channels) = PortWorker::new(
            BufReader::new(Cursor::new(vec![])),
            vec![DeviceInstance::new("a", Actuator(writes.clone(), fail_write)).unwrap()],
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
        let manage = |command| service.manage(serde_json::from_value(command).unwrap());
        manage(json!({"action":"create","expected_revision":0,"configuration":{"name":"loop","type_id":"pid","inputs":{"input":"a/raw"},"parameters":{"setpoint":3.5},"max_age_ms":10000}})).unwrap();
        manage(json!({"action":"bind","name":"loop","target":"a/power","command_timeout_ms":1000,"expected_revision":1})).unwrap();
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
        if stop_before_start {
            manage(json!({"action":"state","name":"loop","state":"stopped","expected_revision":3}))
                .unwrap();
        }
        let join = std::thread::spawn(move || worker.run());
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
            usize::from(!stop_before_start)
        );
        if fail_write {
            let mut recorded = Vec::new();
            service.tick_recorded(|e| recorded.push(e.clone())).unwrap();
            assert_eq!(recorded.len(), 7);
            assert!(recorded.iter().all(|e| matches!(e, AggregatorEvent::Measurement { observation, .. } if observation.reading.is_err())));
            let list = manage(json!({"action":"list"})).unwrap();
            assert_eq!(list["instances"][0]["state"], "fault");
            let outputs = manage(json!({"action":"outputs"})).unwrap();
            assert_eq!(outputs[0]["mode"], "manual");
            assert_eq!(outputs[0]["outcome_unknown"], true);
            service
                .tick_recorded(|_| panic!("fault must only be published once"))
                .unwrap();
            assert_eq!(writes.load(Ordering::SeqCst), 1);
        }
        service.stop_controls();
        channels.commands.shutdown();
        join.join().unwrap();
    }
}
