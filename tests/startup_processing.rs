use lab_server::{
    config::{RecordingConfig, ServerConfig, WebSocketConfig},
    driver::{DeviceIo, Driver, DriverError},
    model::{ActionDescription, Observation, ParameterDescription, SourceMetadata, Spec, Value},
    observation_aggregator::AggregatorEvent,
    recorder::Recorder,
    service::DeviceService,
    session::Session,
    startup::{DriverSetup, StartedServer, start_with},
};
use serde_json::{Value as Json, json};
use std::{
    io::{self, BufReader, Cursor},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

#[path = "support/recording_db.rs"]
mod db;
use db::{Database, scalar};

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
        |_| Ok(test_driver(reads, writes)),
        |_, _| Ok(mock_port()),
    )
}

fn test_driver(reads: &Arc<AtomicUsize>, writes: &Arc<AtomicUsize>) -> DriverSetup {
    DriverSetup {
        driver: Box::new(Source {
            reads: reads.clone(),
            writes: writes.clone(),
        }),
        post_open_delay: Duration::ZERO,
    }
}

fn mock_port() -> BufReader<Cursor<Vec<u8>>> {
    BufReader::new(Cursor::new(Vec::new()))
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
fn startup_graph_is_identical_for_every_declaration_order() {
    let mut config = config(GRAPH);
    let processing = config.processing.as_mut().unwrap();
    let mut independent = processing.components[0].clone();
    independent.configuration.name = "independent".into();
    processing.components.push(independent);
    let components = processing.components.clone();
    let mut expected = None;
    for indices in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        config.processing.as_mut().unwrap().components =
            indices.map(|i| components[i].clone()).into();
        let server = start(&config, &Arc::default(), &Arc::default()).unwrap();
        let snapshot = list(server.backend());
        assert_eq!(snapshot["revision"], 4);
        assert_eq!(snapshot["instances"][0]["incarnation"], 1); // independent
        assert_eq!(snapshot["instances"][1]["incarnation"], 3); // loop
        assert_eq!(snapshot["instances"][2]["incarnation"], 2); // smooth
        let catalog = request(server.backend(), json!({"op":"describe"}));
        let result = (snapshot, catalog);
        if let Some(expected) = &expected {
            assert_eq!(&result, expected);
        } else {
            expected = Some(result);
        }
    }
}

#[test]
fn invalid_startup_aborts_before_open_and_reports_component_and_reason() {
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
            GRAPH.replace("furnace1/sample_temperature", "processing/loop/output"),
            "loop",
            "dependency_cycle",
        ),
        (
            GRAPH.replace("processing/smooth/value", "processing/smooth/missing"),
            "loop",
            "unknown_or_non_numeric_input",
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
            GRAPH.replace("command_timeout_ms = 1000", "command_timeout_ms = 10001"),
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
        let reads = Arc::default();
        let writes = Arc::default();
        let mut opens = 0;
        let result = start_with(
            &config(&graph),
            |_| Ok(test_driver(&reads, &writes)),
            |_, _| {
                opens += 1;
                Ok(mock_port())
            },
        );
        let error = match result {
            Ok(_) => panic!("accepted {name}: {reason}"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(name) && error.contains(reason),
            "{name}: {reason}: {error}"
        );
        assert_eq!(opens, 0, "{name}: {reason}");
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn startup_uses_existing_instance_resource_limit() {
    let graph: String = (0..129).map(|i| format!("\n[[processing.components]]\nname='ema{i}'\ntype_id='ema'\nmax_age_ms=5000\n[processing.components.inputs]\ninput='furnace1/sample_temperature'\n")).collect();
    let mut opens = 0;
    let error = start_with(
        &config(&graph),
        |_| Ok(test_driver(&Arc::default(), &Arc::default())),
        |_, _| {
            opens += 1;
            Ok(mock_port())
        },
    )
    .err()
    .unwrap()
    .to_string();
    assert!(
        error.contains("ema128") && error.contains("invalid_instance"),
        "{error}"
    );
    assert_eq!(opens, 0);
}

struct CatalogDriver {
    parameters: Vec<ParameterDescription>,
    actions: Vec<ActionDescription>,
}

impl Driver for CatalogDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        self.parameters.clone()
    }

    fn actions(&self) -> Vec<ActionDescription> {
        self.actions.clone()
    }

    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        panic!("unexpected physical read");
    }

    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        panic!("unexpected physical write");
    }
}

#[test]
fn startup_rejects_a_derived_signal_colliding_with_a_physical_key() {
    let mut config = config(GRAPH);
    config.devices[0].id = "processing".into();
    let mut opens = 0;
    let error = start_with(
        &config,
        |_| {
            Ok(DriverSetup {
                driver: Box::new(CatalogDriver {
                    parameters: vec![ParameterDescription {
                        id: "smooth/value".into(),
                        spec: Spec::Boolean,
                        unit: String::new(),
                        writable: false,
                        default_poll_period: None,
                    }],
                    actions: Vec::new(),
                }),
                post_open_delay: Duration::ZERO,
            })
        },
        |_, _| {
            opens += 1;
            Ok(mock_port())
        },
    )
    .err()
    .unwrap()
    .to_string();
    assert!(
        error.contains("smooth") && error.contains("signal_collision"),
        "{error}"
    );
    assert_eq!(opens, 0);
}

#[test]
fn physical_catalog_with_actions_must_fit_describe_even_without_components() {
    let config = config("");
    for (bytes, valid) in [(62 * 1024, true), (64 * 1024, false)] {
        let mut opens = 0;
        let result = start_with(
            &config,
            |_| {
                Ok(DriverSetup {
                    driver: Box::new(CatalogDriver {
                        parameters: Vec::new(),
                        actions: vec![ActionDescription {
                            name: "reset".into(),
                            description: "x".repeat(bytes),
                        }],
                    }),
                    post_open_delay: Duration::ZERO,
                })
            },
            |_, _| {
                opens += 1;
                Ok(mock_port())
            },
        );
        if valid {
            let server = result.unwrap();
            let describe = request(server.backend(), json!({"op":"describe"}));
            assert_eq!(describe["actions"][0]["key"], "furnace1/reset");
            assert_eq!(opens, 1);
        } else {
            let error = result.err().unwrap().to_string();
            assert!(
                error.contains("describe catalog") && error.contains("catalog_capacity"),
                "{error}"
            );
            assert_eq!(opens, 0);
        }
    }
}

#[test]
fn valid_graph_is_ready_with_an_unavailable_port_and_no_early_io() {
    let reads = Arc::default();
    let writes = Arc::default();
    let mut config = config(GRAPH);
    let mut other = config.ports[0].clone();
    other.id = "other_link".into();
    other.name = "MOCK2".into();
    config.ports.push(other);
    config.ports.swap(0, 1);
    let mut device = config.devices[0].clone();
    device.id = "furnace2".into();
    device.port = "other_link".into();
    config.devices.push(device);
    let mut opens = 0;
    let server = start_with(
        &config,
        |_| Ok(test_driver(&reads, &writes)),
        |port, _| {
            assert_eq!(reads.load(Ordering::SeqCst), 0);
            assert_eq!(writes.load(Ordering::SeqCst), 0);
            opens += 1;
            if port.id == "other_link" {
                Ok(mock_port())
            } else {
                Err(io::Error::other("mock port unavailable"))
            }
        },
    )
    .unwrap();
    assert_eq!(opens, 2);
    let service = server.backend();
    assert_eq!(list(service)["revision"], 3);
    assert_eq!(list(service)["instances"][0]["state"], "shadow");
    let catalog = request(service, json!({"op":"describe"}));
    for key in ["processing/loop/output", "furnace2/sample_temperature"] {
        assert!(
            catalog["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["key"] == key)
        );
    }
    assert_eq!(
        request(
            service,
            json!({"op":"get","key":"furnace1/sample_temperature"})
        )["error"],
        "runtime_unavailable"
    );
    drop(server);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

fn enable_recording(config: &mut ServerConfig, db: &Database) {
    config.recording = Some(RecordingConfig {
        enabled: true,
        audit_enabled: true,
        path: Some(db.0.clone()),
        queue_capacity: None,
        audit_queue_capacity: None,
    });
}

#[test]
fn invalid_graph_preserves_existing_database_and_leaves_tcp_and_websocket_addresses_free() {
    let db = Database::new();
    let session = Recorder::open(&db.0, 8)
        .unwrap()
        .shutdown()
        .unwrap()
        .session_id;
    {
        let c = db.connect();
        c.execute(
            "INSERT INTO measurements(session_id,port_id,global_key,local_source,completed_at_ns,unit,value_type,integer_value) VALUES (?1,'p','a/value','value',42,'','integer',17)",
            [session],
        ).unwrap();
        c.execute(
            "INSERT INTO audit_events(session_id,operation_id,sequence,registered_at_ns,kind,context_json,details_json) VALUES (?1,'1',1,42,'accepted','{}','{}')",
            [session],
        ).unwrap();
    }
    let original = std::fs::read(&db.0).unwrap();

    // Create smooth successfully in memory, then fail the later binding.
    let mut config = config(&GRAPH.replace("command_timeout_ms = 1000", "command_timeout_ms = 0"));
    enable_recording(&mut config, &db);
    let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
    let websocket = TcpListener::bind("127.0.0.1:0").unwrap();
    config.tcp.bind = tcp.local_addr().unwrap();
    config.websocket = Some(WebSocketConfig {
        enabled: true,
        bind: Some(websocket.local_addr().unwrap()),
        allowed_origins: Vec::new(),
    });
    // Even with the addresses occupied, validation must report the binding error.
    let validate = || {
        let error = start_with(
            &config,
            |_| Ok(test_driver(&Arc::default(), &Arc::default())),
            |_, _| -> io::Result<BufReader<Cursor<Vec<u8>>>> {
                panic!("invalid graph opened a physical port");
            },
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            error.contains("loop") && error.contains("invalid_timeout"),
            "{error}"
        );
    };
    validate();
    drop((tcp, websocket));
    validate();
    let _tcp = TcpListener::bind(config.tcp.bind).unwrap();
    let _websocket = TcpListener::bind(config.websocket.unwrap().bind.unwrap()).unwrap();
    assert_eq!(std::fs::read(&db.0).unwrap(), original);
    let c = db.connect();
    assert_eq!(scalar(&c, "SELECT count(*) FROM sessions"), 1);
    assert_eq!(scalar(&c, "SELECT integer_value FROM measurements"), 17);
    assert_eq!(scalar(&c, "SELECT registered_at_ns FROM audit_events"), 42);
}

#[test]
fn successful_startup_audits_prepared_mutations_once_with_their_revisions() {
    let db = Database::new();
    let mut config = config(GRAPH);
    config.processing.as_mut().unwrap().components.reverse();
    enable_recording(&mut config, &db);
    let server = start(&config, &Arc::default(), &Arc::default()).unwrap();
    server.run_until(&AtomicBool::new(true)).unwrap();

    let c = db.connect();
    assert_eq!(
        scalar(
            &c,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        1
    );
    let mut statement = c.prepare(
        "SELECT kind, json_extract(context_json, '$.request.command.action'),
                json_extract(details_json, '$.revision_before'), json_extract(details_json, '$.revision_after')
         FROM audit_events WHERE json_extract(context_json, '$.source')='internal'
         AND json_extract(context_json, '$.request.op')='components' ORDER BY CAST(operation_id AS INTEGER), sequence"
    ).unwrap();
    let rows = statement
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<i64>>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = [("create", 0, 1), ("create", 1, 2), ("bind", 2, 3)]
        .into_iter()
        .flat_map(|(action, before, after)| {
            [
                ("accepted".to_owned(), action.to_owned(), None, None),
                (
                    "applied".to_owned(),
                    action.to_owned(),
                    Some(before),
                    Some(after),
                ),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}
