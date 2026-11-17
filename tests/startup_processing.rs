use lab_server::{
    config::ServerConfig,
    driver::{DeviceIo, Driver, DriverError},
    model::{Observation, ParameterDescription, SourceMetadata, Spec, Value},
    observation_aggregator::AggregatorEvent,
    service::DeviceService,
    session::Session,
    startup::{DriverSetup, StartedServer, start_with},
};
use serde_json::{Value as Json, json};
use std::{
    io::{BufReader, Cursor},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

const GRAPH: &str = r#"
[[processing.components]]
name = "smooth"
type_id = "ema"
max_age_ms = 5000
[processing.components.inputs]
input = "furnace1/sample_temperature"
[processing.components.parameters]
tau = 2.0

[[processing.components]]
name = "loop"
type_id = "pid"
max_age_ms = 5000
[processing.components.inputs]
input = "processing/smooth/value"
[processing.components.parameters]
setpoint = 40.0
kp = 2.0
ki = 0.1
kd = 0.0
output_min = 0.0
output_max = 100.0
max_gap = 5.0
[processing.components.binding]
target = "furnace1/power"
command_timeout_ms = 1000
"#;

fn config(graph: &str) -> ServerConfig {
    ServerConfig::from_toml(&format!(
        "{}\n{graph}",
        include_str!("../examples/arduino.toml").replace("127.0.0.1:7240", "127.0.0.1:0")
    ))
    .unwrap()
}

struct Source {
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
}
impl Driver for Source {
    fn parameters(&self) -> Vec<ParameterDescription> {
        ["sample_temperature", "power", "flag"]
            .into_iter()
            .map(|id| ParameterDescription {
                id: id.into(),
                unit: String::new(),
                spec: if id == "flag" {
                    Spec::Boolean
                } else {
                    Spec::Float { min: 0., max: 100. }
                },
                writable: id != "sample_temperature",
                default_poll_period: (id != "power").then_some(Duration::from_secs(60)),
            })
            .collect()
    }
    fn read(&mut self, key: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(if key == "flag" {
            Value::Boolean(false)
        } else {
            Value::Float(20.)
        })
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn start(
    config: &ServerConfig,
    reads: &Arc<AtomicUsize>,
    writes: &Arc<AtomicUsize>,
) -> Result<StartedServer, Box<dyn std::error::Error + Send + Sync>> {
    start_with(
        config,
        |_| {
            Ok(DriverSetup {
                driver: Box::new(Source {
                    reads: reads.clone(),
                    writes: writes.clone(),
                }),
                post_open_delay: Duration::ZERO,
            })
        },
        |_, _| Ok(BufReader::new(Cursor::new(Vec::<u8>::new()))),
    )
}

fn request(service: &DeviceService, mut message: Json) -> Json {
    message["id"] = json!(1);
    let mut session = Session::new(64, 65536);
    service
        .receive_message(&mut session, &message.to_string(), Instant::now())
        .unwrap();
    serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap()
}
fn list(service: &DeviceService) -> Json {
    request(
        service,
        json!({"op":"components","command":{"action":"list"}}),
    )["data"]
        .clone()
}

#[test]
fn optional_processing_keeps_empty_graph_and_revision_zero() {
    for graph in ["", "[processing]", "[processing]\ncomponents = []"] {
        let server = start(&config(graph), &Arc::default(), &Arc::default()).unwrap();
        assert_eq!(list(server.backend())["revision"], 0);
        assert_eq!(list(server.backend())["total"], 0);
    }
}

#[test]
fn startup_graph_is_visible_processes_measurements_and_never_writes() {
    let reads = Arc::default();
    let writes = Arc::new(AtomicUsize::new(0));
    let server = start(&config(GRAPH), &reads, &writes).unwrap();
    let service = server.backend();
    let snapshot = list(service);
    assert_eq!(snapshot["revision"], 3);
    assert_eq!(snapshot["total"], 2);
    assert_eq!(snapshot["instances"][0]["state"], "shadow");
    assert_eq!(
        snapshot["instances"][0]["configuration"]["inputs"]["input"],
        "processing/smooth/value"
    );
    assert_eq!(
        snapshot["instances"][0]["binding"],
        json!({"target":"furnace1/power","command_timeout_ms":1000})
    );
    assert_eq!(snapshot["instances"][1]["state"], "running");
    assert_eq!(
        snapshot["instances"][1]["configuration"]["parameters"]["tau"],
        2.0
    );
    let catalog = request(service, json!({"op":"describe"}));
    assert!(
        catalog["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["key"] == "processing/smooth/value")
    );
    let instance = service
        .registry()
        .ports()
        .next()
        .unwrap()
        .handle()
        .unwrap()
        .source_instance();
    let now = Instant::now();
    for (sequence, value) in [(1, 20.), (2, 30.)] {
        service
            .apply_event(AggregatorEvent::Measurement {
                port_id: "arduino_link".into(),
                global_key: "furnace1/sample_temperature".into(),
                observation: Observation {
                    source: "sample_temperature".into(),
                    unit: String::new(),
                    completed_at: SystemTime::now(),
                    metadata: Some(SourceMetadata {
                        source_instance: instance,
                        sequence,
                        generation: 0,
                        acquired_at: now + Duration::from_secs(sequence - 1),
                        elapsed: Duration::from_secs(sequence - 1),
                    }),
                    reading: Ok(Value::Float(value)),
                },
            })
            .unwrap();
    }
    let value = request(service, json!({"op":"get","key":"processing/smooth/value"}))["value"]
        .as_f64()
        .unwrap();
    assert!((value - (20. + 10. * (1. - (-0.5_f64).exp()))).abs() < 1e-10);
    assert_eq!(
        request(service, json!({"op":"get","key":"processing/loop/output"}))["status"],
        "ok"
    );
    assert_eq!(list(service)["instances"][0]["state"], "shadow");
    let outputs = request(
        service,
        json!({"op":"components","command":{"action":"outputs"}}),
    );
    for output in outputs["data"].as_array().unwrap() {
        assert_eq!(output["mode"], "manual");
        assert!(output["owner"].is_null());
        assert_eq!(output["in_flight"], 0);
    }
    // A queued command would be drained before worker shutdown; check after join.
    drop(server);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_startup_aborts_before_workers_and_reports_component_and_reason() {
    let split = GRAPH
        .find("\n[[processing.components]]\nname = \"loop\"")
        .unwrap();
    let reversed = format!("{}{}", &GRAPH[split..], &GRAPH[..split]);
    let cases = [
        (
            GRAPH.replace("\"ema\"", "\"missing\""),
            "smooth",
            "unknown_type",
        ),
        (
            GRAPH.replace("tau = 2.0", "tau = -1.0"),
            "smooth",
            "invalid_parameter:tau",
        ),
        (
            GRAPH.replace("tau = 2.0", "tau = 'wrong'"),
            "smooth",
            "invalid_parameter:tau",
        ),
        (
            GRAPH.replace("tau = 2.0", "typo = 2.0"),
            "smooth",
            "unknown_parameter",
        ),
        (
            GRAPH.replace("furnace1/sample_temperature", "missing/input"),
            "smooth",
            "unknown_or_non_numeric_input",
        ),
        (
            GRAPH.replace("furnace1/sample_temperature", "furnace1/flag"),
            "smooth",
            "unknown_or_non_numeric_input",
        ),
        (
            GRAPH.replace("furnace1/sample_temperature", "furnace1/power"),
            "smooth",
            "unknown_or_non_numeric_input",
        ),
        (
            GRAPH.replace("furnace1/sample_temperature", "processing/smooth/value"),
            "smooth",
            "dependency_cycle",
        ),
        (
            GRAPH.replace("input = \"furnace1", "typo = \"furnace1"),
            "smooth",
            "invalid_inputs",
        ),
        (
            GRAPH.replace("max_age_ms = 5000", "max_age_ms = 0"),
            "smooth",
            "invalid_inputs",
        ),
        (reversed, "loop", "unknown_or_non_numeric_input"),
        (
            GRAPH.replace(
                "target = \"furnace1/power\"",
                "target = \"furnace1/sample_temperature\"",
            ),
            "loop",
            "invalid_output_target",
        ),
        (
            GRAPH.replace("target = \"furnace1/power\"", "target = \"furnace1/flag\""),
            "loop",
            "invalid_output_target",
        ),
        (
            GRAPH.replace("target = \"furnace1/power\"", "target = \"missing/power\""),
            "loop",
            "unknown",
        ),
        (
            GRAPH.replace("command_timeout_ms = 1000", "command_timeout_ms = 0"),
            "loop",
            "invalid_timeout",
        ),
        (
            GRAPH.replace("output_min = 0.0", "output_min = 100.0"),
            "loop",
            "invalid_limits",
        ),
        (
            GRAPH.replace("name = \"loop\"", "name = \"smooth\""),
            "smooth",
            "invalid_instance",
        ),
    ];
    for (graph, name, reason) in cases {
        let reads = Arc::new(AtomicUsize::new(0));
        let writes = Arc::default();
        let error = match start(&config(&graph), &reads, &writes) {
            Ok(_) => panic!("accepted {name}: {reason}"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(name) && error.contains(reason),
            "{name}: {reason}: {error}"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn startup_uses_existing_instance_resource_limit() {
    let graph: String = (0..129).map(|i| format!("\n[[processing.components]]\nname='ema{i}'\ntype_id='ema'\nmax_age_ms=5000\n[processing.components.inputs]\ninput='furnace1/sample_temperature'\n")).collect();
    let error = start(&config(&graph), &Arc::default(), &Arc::default())
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("ema128") && error.contains("invalid_instance"),
        "{error}"
    );
}
