use super::backend::{TestBackend, TestCommand};
use super::*;
use crate::port_worker::{CommandError, DeviceObservation};

#[test]
fn ids_are_monotonic_across_operations_and_invalid_requests() {
    let now = Instant::now();
    for duplicate in [
        r#"{"id":10,"op":"set","key":"unknown","value":1}"#,
        r#"{"id":10,"op":"ping"}"#,
        r#"{"id":10,"op":"get","key":"unknown"}"#,
        r#"{"id":10,"op":"subscribe","keys":[]}"#,
        r#"{"id":10,"op":"unsubscribe","keys":[]}"#,
        r#"{"id":10,"op":"set"}"#,
        r#"{"id":9,"op":"ping"}"#,
    ] {
        let mut session = ClientSession::new(2, 2048);
        let (tx, rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);
        let catalog = power_catalog();
        let latest = Mutex::new(HashMap::new());
        for frame in [
            r#"{"id":10,"op":"set","key":"power","value":20}"#,
            duplicate,
        ] {
            for event in session.receive(format!("{frame}\n").as_bytes()) {
                session
                    .dispatch_incoming(event, &tx, &catalog, &latest, now)
                    .unwrap();
            }
        }
        let TestCommand::Set(command) = rx.try_recv().unwrap() else {
            panic!()
        };
        assert!(command.execution.try_start());
        command.complete(Ok(())).unwrap();
        session.collect_ready(now).unwrap();
        for event in session.receive(b"{\"id\":10,\"op\":\"ping\"}\n") {
            session
                .dispatch_incoming(event, &tx, &catalog, &latest, now)
                .unwrap();
        }
        let mut bytes = Vec::new();
        session.outbound.flush_to(&mut bytes).unwrap();
        let responses: Vec<serde_json::Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            responses.iter().filter(|r| r.get("id").is_some()).count(),
            1,
            "{duplicate}"
        );
        assert_eq!(responses[0]["error"], "duplicate_id");
        assert_eq!(responses[1]["id"], 10);
        assert_eq!(responses[2]["error"], "duplicate_id");
        assert!(rx.try_recv().is_err());
    }
}

#[test]
fn request_ids_are_independent_between_clients() {
    let (tx, _rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);
    for _ in 0..2 {
        let mut session = ClientSession::new(1, 128);
        session
            .dispatch_incoming(
                Incoming::Request(TcpRequest::Ping { id: 1 }),
                &tx,
                &HashMap::new(),
                &Mutex::new(HashMap::new()),
                Instant::now(),
            )
            .unwrap();
        let mut bytes = Vec::new();
        session.outbound.flush_to(&mut bytes).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["status"],
            "ok"
        );
    }
}

#[test]
fn session_accepts_multiple_requests_across_reads() {
    let mut session = ClientSession::new(2, 128);

    assert!(session.receive(b"{\"id\":1,\"op\":\"pin").is_empty());

    assert_eq!(
        session.receive(b"g\"}\n{\"id\":2,\"op\":\"get\",\"key\":\"power\"}\n"),
        vec![
            Incoming::Request(TcpRequest::Ping { id: 1 }),
            Incoming::Request(TcpRequest::Get {
                id: 2,
                key: "power".to_owned(),
            }),
        ]
    );

    assert!(!session.is_terminal());
    assert!(session.pending.is_empty());

    session
        .outbound
        .enqueue_line(r#"{"id":1,"status":"ok"}"#)
        .unwrap();

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    assert_eq!(output, b"{\"id\":1,\"status\":\"ok\"}\n");
}

#[test]
fn session_preserves_requests_before_framing_error() {
    use crate::protocol::JsonError;

    let mut session = ClientSession::new(2, 128);

    let incoming =
        session.receive(b"{\"id\":1,\"op\":\"ping\"}\nnot-json\n{\"id\":2,\"op\":\"ping\"}\n\n");

    assert_eq!(
        incoming,
        vec![
            Incoming::Request(TcpRequest::Ping { id: 1 }),
            Incoming::InvalidRequest {
                frame: "not-json".to_owned(),
                error: ParseError::Json(JsonError::InvalidJson),
            },
            Incoming::Request(TcpRequest::Ping { id: 2 }),
            Incoming::FramingError(FrameError::EmptyFrame),
        ]
    );

    assert!(session.is_terminal());

    assert!(session.receive(b"{\"id\":3,\"op\":\"ping\"}\n").is_empty());
}

#[test]
fn session_detects_incomplete_frame_at_eof() {
    let mut session = ClientSession::new(2, 128);

    assert!(session.receive(b"{\"id\":1").is_empty());

    assert_eq!(session.finish(), Err(FrameError::IncompleteFrame));
}

#[test]
fn session_encodes_completed_read_and_set() {
    let mut session = ClientSession::new(2, 1024);
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);

    let (backend, commands) = TestBackend::new(2);
    let mut description = get_catalog()["configuration"].clone();
    description.id = "sensor/sample_temperature".into();
    backend
        .enqueue_read(&mut session.pending, 7, deadline, description)
        .unwrap();
    backend
        .enqueue_set(
            &mut session.pending,
            8,
            deadline,
            "power".into(),
            Value::Integer(20),
        )
        .unwrap();

    session.collect_ready(now).unwrap();
    assert!(session.outbound.is_empty());

    let TestCommand::Read { reply, .. } = commands.try_recv().unwrap() else {
        panic!("expected Read command")
    };
    reply
        .send(Ok(DeviceObservation {
            device_id: "sensor".into(),
            observation: observation(),
        }))
        .unwrap();
    let TestCommand::Set(command) = commands.try_recv().unwrap() else {
        panic!("expected Set command")
    };
    command.complete(Err(CommandError::InvalidValue)).unwrap();

    session.collect_ready(now).unwrap();

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0]["id"], 7);
    assert_eq!(responses[0]["status"], "ok");
    assert_eq!(responses[0]["key"], "sensor/sample_temperature");
    assert_eq!(responses[0]["value"], 20.0);

    assert_eq!(responses[1]["id"], 8);
    assert_eq!(responses[1]["status"], "error");
    assert_eq!(responses[1]["error"], "Value does not match parameter spec");

    assert!(session.pending.is_empty());
    session.collect_ready(deadline).unwrap();
    assert!(
        session.outbound.is_empty(),
        "completed requests must not reply twice"
    );
}

#[test]
fn session_encodes_timeout_and_disconnection() {
    let mut session = ClientSession::new(2, 1024);
    let now = Instant::now();

    let (backend, commands) = TestBackend::new(2);
    backend
        .enqueue_read(
            &mut session.pending,
            1,
            now + Duration::from_secs(10),
            get_catalog()["configuration"].clone(),
        )
        .unwrap();
    drop(commands.try_recv().unwrap());
    backend
        .enqueue_set(
            &mut session.pending,
            2,
            now,
            "power".into(),
            Value::Integer(20),
        )
        .unwrap();

    session.collect_ready(now).unwrap();

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[0]["error"], "runtime_unavailable");

    assert_eq!(responses[1]["id"], 2);
    assert_eq!(responses[1]["error"], "runtime_timeout");
    let TestCommand::Set(command) = commands.try_recv().unwrap() else {
        panic!("expected Set command")
    };
    assert!(!command.execution.try_start());
    assert!(session.pending.is_empty());
    session
        .collect_ready(now + Duration::from_secs(11))
        .unwrap();
    assert!(session.outbound.is_empty());
}

#[test]
fn output_overflow_makes_session_terminal() {
    let mut session = ClientSession::new(1, 2);
    let now = Instant::now();

    let (backend, commands) = TestBackend::new(1);
    backend
        .enqueue_set(
            &mut session.pending,
            1,
            now + Duration::from_secs(10),
            "power".into(),
            Value::Integer(20),
        )
        .unwrap();
    let TestCommand::Set(command) = commands.try_recv().unwrap() else {
        panic!("expected Set command")
    };
    assert!(command.execution.try_start());
    command.complete(Ok(())).unwrap();

    assert!(session.collect_ready(now).is_err());
    assert!(session.is_terminal());
}

#[test]
fn validates_set_spec_before_queueing() {
    for (writable, spec, value, expected) in [
        (
            true,
            crate::model::Spec::Integer { min: 0, max: 100 },
            Value::Boolean(true),
            Some("Value does not match parameter spec"),
        ),
        (
            true,
            crate::model::Spec::Float {
                min: 0.0,
                max: 100.0,
            },
            Value::Boolean(false),
            Some("Value does not match parameter spec"),
        ),
        (
            false,
            crate::model::Spec::Integer { min: 0, max: 100 },
            Value::Integer(20),
            Some("Parameter is read-only"),
        ),
        (
            true,
            crate::model::Spec::Integer { min: 0, max: 100 },
            Value::Float(20.0),
            Some("Value does not match parameter spec"),
        ),
        (
            true,
            crate::model::Spec::Integer { min: 0, max: 100 },
            Value::Integer(101),
            Some("Value does not match parameter spec"),
        ),
        (
            true,
            crate::model::Spec::Float {
                min: 0.0,
                max: 100.0,
            },
            Value::Float(f64::NAN),
            Some("Value does not match parameter spec"),
        ),
        (
            true,
            crate::model::Spec::Integer { min: 100, max: 0 },
            Value::Integer(20),
            Some("Value does not match parameter spec"),
        ),
        (
            true,
            crate::model::Spec::Integer { min: 0, max: 100 },
            Value::Integer(20),
            None,
        ),
    ] {
        let mut catalog = power_catalog();
        let description = catalog.get_mut("power").unwrap();
        description.writable = writable;
        description.spec = spec;
        let (tx, rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);
        let mut session = ClientSession::new(1, 1024);
        session
            .dispatch_set(1, "power".into(), value, &tx, &catalog, Instant::now())
            .unwrap();
        if let Some(expected) = expected {
            assert!(rx.try_recv().is_err(), "invalid Set reached device queue");
            assert!(session.pending.is_empty());
            let mut bytes = Vec::new();
            session.outbound.flush_to(&mut bytes).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["error"],
                expected
            );
        } else {
            assert!(matches!(rx.try_recv(), Ok(TestCommand::Set(_))));
            assert!(rx.try_recv().is_err());
            assert_eq!(session.pending.len(), 1);
        }
    }
}

#[test]
fn dispatch_set_rejects_requests_before_device_queue() {
    let now = Instant::now();
    let catalog = power_catalog();

    let mut session = ClientSession::new(1, 512);
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    session
        .dispatch_set(
            1,
            "power".to_owned(),
            Value::Integer(25),
            &command_tx,
            &catalog,
            now,
        )
        .unwrap();

    let first_command = command_rx.try_recv().unwrap();

    session
        .dispatch_set(
            1,
            "power".to_owned(),
            Value::Integer(30),
            &command_tx,
            &catalog,
            now,
        )
        .unwrap();

    session
        .dispatch_set(
            2,
            "power".to_owned(),
            Value::Integer(40),
            &command_tx,
            &catalog,
            now,
        )
        .unwrap();

    session
        .dispatch_set(
            3,
            "unknown".to_owned(),
            Value::Integer(50),
            &command_tx,
            &catalog,
            now,
        )
        .unwrap();

    assert!(matches!(
        command_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["error"], "duplicate_id");
    assert_eq!(responses[1]["error"], "too_many_pending");
    assert_eq!(responses[2]["error"], "unknown_key");

    match first_command {
        TestCommand::Set(command) => {
            assert_eq!(command.parameter, "power");
            assert_eq!(command.value, Value::Integer(25));
            assert!(command.execution.try_start());
            command.complete(Ok(())).unwrap();
        }

        _ => panic!("expected Set command"),
    }

    session.collect_ready(now).unwrap();

    assert!(session.pending.is_empty());
}

#[test]
fn dispatch_get_uses_latest_for_polled_signal() {
    let mut session = ClientSession::new(1, 512);
    let catalog = get_catalog();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let now = Instant::now();

    session
        .dispatch_get(
            1,
            "sample_temperature".to_owned(),
            &command_tx,
            &catalog,
            &latest,
            now,
        )
        .unwrap();

    latest
        .lock()
        .unwrap()
        .insert("sample_temperature".to_owned(), observation());

    session
        .dispatch_get(
            2,
            "sample_temperature".to_owned(),
            &command_tx,
            &catalog,
            &latest,
            now,
        )
        .unwrap();

    assert!(session.pending.is_empty());

    assert!(matches!(
        command_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[0]["status"], "no_data");

    assert_eq!(responses[1]["id"], 2);
    assert_eq!(responses[1]["status"], "ok");
    assert_eq!(responses[1]["value"], 20.0);
}

#[test]
fn dispatch_get_queues_on_demand_read() {
    let mut session = ClientSession::new(1, 512);
    let catalog = get_catalog();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let now = Instant::now();

    session
        .dispatch_get(
            7,
            "configuration".to_owned(),
            &command_tx,
            &catalog,
            &latest,
            now,
        )
        .unwrap();

    assert_eq!(session.pending.len(), 1);
    assert!(session.outbound.is_empty());

    let command = command_rx.try_recv().unwrap();

    match command {
        TestCommand::Read { description, reply } => {
            assert_eq!(description.id, "configuration");

            reply
                .send(Ok(DeviceObservation {
                    device_id: "test-device".into(),
                    observation: Observation {
                        source: "configuration".to_owned(),
                        unit: "".to_owned(),
                        completed_at: UNIX_EPOCH,
                        reading: Ok(Value::Integer(30)),
                    },
                }))
                .unwrap();
        }

        _ => panic!("expected Read command"),
    }

    session.collect_ready(now).unwrap();

    assert!(session.pending.is_empty());

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let response: serde_json::Value = serde_json::from_slice(&output[..output.len() - 1]).unwrap();

    assert_eq!(response["id"], 7);
    assert_eq!(response["status"], "ok");
    assert_eq!(response["value"], 30);
}

#[test]
fn cached_get_works_while_pending_queue_is_full() {
    let mut session = ClientSession::new(1, 512);
    let catalog = get_catalog();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, _command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let now = Instant::now();

    session
        .dispatch_get(
            7,
            "configuration".to_owned(),
            &command_tx,
            &catalog,
            &latest,
            now,
        )
        .unwrap();

    assert_eq!(session.pending.len(), 1);

    session
        .dispatch_get(
            8,
            "sample_temperature".to_owned(),
            &command_tx,
            &catalog,
            &latest,
            now,
        )
        .unwrap();

    session
        .dispatch_get(
            7,
            "sample_temperature".to_owned(),
            &command_tx,
            &catalog,
            &latest,
            now,
        )
        .unwrap();

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0]["id"], 8);
    assert_eq!(responses[0]["status"], "no_data");

    assert!(responses[1].get("id").is_none());
    assert_eq!(responses[1]["error"], "duplicate_id");

    assert_eq!(session.pending.len(), 1);
}

#[test]
fn dispatch_incoming_routes_ping_get_and_set() {
    let mut session = ClientSession::new(4, 1024);

    let mut catalog = get_catalog();
    catalog.extend(power_catalog());

    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let input = concat!(
        "{\"id\":1,\"op\":\"ping\"}\n",
        "{\"id\":2,\"op\":\"get\",\"key\":\"sample_temperature\"}\n",
        "{\"id\":3,\"op\":\"set\",\"key\":\"power\",\"value\":25}\n"
    );

    let incoming = session.receive(input.as_bytes());

    assert_eq!(incoming.len(), 3);

    for event in incoming {
        let remaining = session
            .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now())
            .unwrap();

        assert!(remaining.is_none());
    }

    assert_eq!(session.pending.len(), 1);

    match command_rx.try_recv().unwrap() {
        TestCommand::Set(command) => {
            assert_eq!(command.parameter, "power");
            assert_eq!(command.value, Value::Integer(25));
        }
        _ => panic!("expected Set command"),
    }

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0], serde_json::json!({"id": 1, "status": "ok"}));

    assert_eq!(
        responses[1],
        serde_json::json!({
            "id": 2,
            "status": "no_data",
            "key": "sample_temperature"
        })
    );
}

#[test]
fn dispatch_incoming_handles_subscribe_and_invalid_request() {
    let mut session = ClientSession::new(2, 512);
    let catalog = get_catalog();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, _command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let input = concat!(
        "{\"id\":4,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\"]}\n",
        "{\"id\":5,\"op\":\"unknown\"}\n"
    );

    let incoming = session.receive(input.as_bytes());

    assert_eq!(incoming.len(), 2);

    let remaining: Vec<Incoming> = incoming
        .into_iter()
        .filter_map(|event| {
            session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now())
                .unwrap()
        })
        .collect();

    assert!(remaining.is_empty());

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0]["id"], 4);
    assert_eq!(responses[0]["status"], "ok");
    assert_eq!(
        responses[0]["keys"],
        serde_json::json!(["sample_temperature"])
    );

    assert_eq!(responses[1]["id"], 5);
    assert_eq!(responses[1]["status"], "error");
    assert_eq!(responses[1]["error"], "unsupported_op");

    assert_eq!(session.subscriptions.keys(), vec!["sample_temperature"]);

    assert!(session.pending.is_empty());
}

#[test]
fn hello_responds_immediately_and_rejects_pending_id() {
    let mut session = ClientSession::new(2, 512);
    let now = Instant::now();

    let catalog = HashMap::new();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    command_tx
        .enqueue_read(
            &mut session.pending,
            7,
            now + Duration::from_secs(10),
            get_catalog()["configuration"].clone(),
        )
        .unwrap();
    let _pending_command = command_rx.try_recv().unwrap();

    let input = concat!(
        "{\"id\":1,\"op\":\"hello\"}\n",
        "{\"id\":7,\"op\":\"hello\"}\n"
    );

    for event in session.receive(input.as_bytes()) {
        let remaining = session
            .dispatch_incoming(event, &command_tx, &catalog, &latest, now)
            .unwrap();

        assert!(remaining.is_none());
    }

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(
        responses[0],
        serde_json::json!({
            "id": 1,
            "status": "ok",
            "protocol": "lab-server",
            "version": 1
        })
    );

    assert!(responses[1].get("id").is_none());
    assert_eq!(responses[1]["status"], "error");
    assert_eq!(responses[1]["error"], "duplicate_id");

    assert_eq!(session.pending.len(), 1);
    assert!(session.outbound.is_empty());
    assert!(command_rx.try_recv().is_err());
}

#[test]
fn describe_serializes_catalog_without_device_commands() {
    let mut session = ClientSession::new(1, 2048);
    let now = Instant::now();

    let mut catalog = get_catalog();
    catalog.extend(power_catalog());

    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    command_tx
        .enqueue_read(
            &mut session.pending,
            7,
            now + Duration::from_secs(10),
            get_catalog()["configuration"].clone(),
        )
        .unwrap();
    let _pending_command = command_rx.try_recv().unwrap();

    let input = concat!(
        "{\"id\":42,\"op\":\"describe\"}\n",
        "{\"id\":7,\"op\":\"describe\"}\n"
    );

    for event in session.receive(input.as_bytes()) {
        assert!(
            session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, now,)
                .unwrap()
                .is_none()
        );
    }

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["id"], 42);
    assert_eq!(responses[0]["status"], "ok");

    let parameters = responses[0]["parameters"].as_array().unwrap();

    assert_eq!(parameters.len(), 3);

    let keys: Vec<_> = parameters
        .iter()
        .map(|parameter| parameter["key"].as_str().unwrap())
        .collect();

    assert_eq!(keys, vec!["configuration", "power", "sample_temperature"]);

    assert_eq!(parameters[0]["type"], "integer");
    assert_eq!(parameters[0]["min"], 0);
    assert_eq!(parameters[0]["max"], 100);
    assert!(parameters[0]["poll_period_ns"].is_null());

    assert_eq!(parameters[1]["unit"], "%");
    assert_eq!(parameters[1]["writable"], true);

    assert_eq!(parameters[2]["type"], "float");
    assert_eq!(parameters[2]["unit"], "degC");
    assert_eq!(parameters[2]["min"], -100.0);
    assert_eq!(parameters[2]["max"], 500.0);
    assert_eq!(parameters[2]["poll_period_ns"].as_u64(), Some(100_000_000));

    assert!(responses[1].get("id").is_none());
    assert_eq!(responses[1]["error"], "duplicate_id");

    assert_eq!(session.pending.len(), 1);
    assert!(command_rx.try_recv().is_err());
}

#[test]
fn invalid_requests_return_codes_without_ambiguous_ids() {
    let mut session = ClientSession::new(2, 4096);
    let catalog = get_catalog();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let input = concat!(
        "{\"id\":10,\"op\":\"unknown\"}\n",
        "{\"id\":11,\"op\":\"get\"}\n",
        "{\"id\":12,\"op\":\"get\",\"key\":\"a\",\"key\":\"b\"}\n",
        "not-json\n",
        "[]\n",
        "{\"op\":\"ping\"}\n",
        "{\"id\":13,\"op\":\"set\",\"key\":\"power\",\"value\":null}\n",
        "{\"id\":14,\"op\":\"ping\"}\n"
    );

    let incoming = session.receive(input.as_bytes());
    assert_eq!(incoming.len(), 8);

    for event in incoming {
        assert!(
            session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                .unwrap()
                .is_none()
        );
    }

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 8);

    assert_eq!(responses[0]["id"], 10);
    assert_eq!(responses[0]["error"], "unsupported_op");

    assert_eq!(responses[1]["id"], 11);
    assert_eq!(responses[1]["error"], "invalid_key");

    assert_eq!(responses[2]["error"], "duplicate_key");
    assert!(responses[2].get("id").is_none());

    assert_eq!(responses[3]["error"], "invalid_json");
    assert!(responses[3].get("id").is_none());

    assert_eq!(responses[4]["error"], "expected_object");
    assert!(responses[4].get("id").is_none());

    assert_eq!(responses[5]["error"], "invalid_id");
    assert!(responses[5].get("id").is_none());

    assert_eq!(responses[6]["id"], 13);
    assert_eq!(responses[6]["error"], "invalid_value");
    assert_eq!(responses[7]["id"], 14);
    assert_eq!(responses[7]["status"], "ok");

    assert!(!session.is_terminal());
    assert!(session.pending.is_empty());
    assert!(command_rx.try_recv().is_err());
}

#[test]
fn framing_error_preserves_valid_reply_before_terminal_error() {
    let mut session = ClientSession::new(2, 512);
    let catalog = HashMap::new();
    let latest = Mutex::new(HashMap::new());
    let (command_tx, command_rx) = TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

    let incoming = session.receive(b"{\"id\":1,\"op\":\"ping\"}\n\n");

    assert_eq!(
        incoming,
        vec![
            Incoming::Request(TcpRequest::Ping { id: 1 }),
            Incoming::FramingError(FrameError::EmptyFrame),
        ]
    );

    for event in incoming {
        assert!(
            session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                .unwrap()
                .is_none()
        );
    }

    assert!(session.is_terminal());
    assert!(session.receive(b"{\"id\":2,\"op\":\"ping\"}\n").is_empty());

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);

    assert_eq!(responses[0], serde_json::json!({"id": 1, "status": "ok"}));

    assert_eq!(
        responses[1],
        serde_json::json!({
            "status": "error",
            "error": "empty_frame"
        })
    );

    assert!(command_rx.try_recv().is_err());
}

#[test]
fn terminal_session_does_not_append_pending_responses() {
    let mut session = ClientSession::new(1, 256);
    let now = Instant::now();

    let (command_tx, command_rx) = TestBackend::new(1);
    command_tx
        .enqueue_set(
            &mut session.pending,
            7,
            now + Duration::from_secs(10),
            "power".into(),
            Value::Integer(20),
        )
        .unwrap();
    let TestCommand::Set(command) = command_rx.try_recv().unwrap() else {
        panic!("expected Set command")
    };
    assert!(command.execution.try_start());

    let incoming = session.receive(b"\n");

    assert_eq!(
        incoming,
        vec![Incoming::FramingError(FrameError::EmptyFrame)]
    );

    let catalog = HashMap::new();
    let latest = Mutex::new(HashMap::new());

    for event in incoming {
        assert!(
            session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, now)
                .unwrap()
                .is_none()
        );
    }

    command.complete(Ok(())).unwrap();
    session.collect_ready(now).unwrap();

    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();

    let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(
        responses,
        vec![serde_json::json!({
            "status": "error",
            "error": "empty_frame"
        })]
    );

    assert!(session.is_terminal());
}

#[test]
fn framing_errors_are_reported_without_request_id() {
    let cases = [
        (b"\xff\n".to_vec(), FrameError::InvalidUtf8, "invalid_utf8"),
        (
            vec![b'x'; crate::protocol::MAX_FRAME_BYTES + 1],
            FrameError::FrameTooLong,
            "frame_too_long",
        ),
    ];

    for (bytes, expected_error, expected_code) in cases {
        let mut session = ClientSession::new(1, 256);
        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) =
            TestBackend::new(crate::port_worker::COMMAND_QUEUE_CAPACITY);

        let incoming = session.receive(&bytes);

        assert_eq!(incoming, vec![Incoming::FramingError(expected_error)]);

        for event in incoming {
            assert!(
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                    .unwrap()
                    .is_none()
            );
        }

        assert!(session.is_terminal());

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let response: serde_json::Value = serde_json::from_slice(&output).unwrap();

        assert_eq!(response["status"], "error");
        assert_eq!(response["error"], expected_code);
        assert!(response.get("id").is_none());
    }
}

#[test]
fn runtime_queue_overflow_releases_pending_capacity() {
    let (tx, rx) = TestBackend::new(1);
    let catalog = power_catalog();
    let mut session = ClientSession::new(4, 1024);
    let now = Instant::now();
    session
        .dispatch_set(1, "power".into(), Value::Integer(1), &tx, &catalog, now)
        .unwrap();
    session
        .dispatch_set(2, "power".into(), Value::Integer(2), &tx, &catalog, now)
        .unwrap();
    session
        .dispatch_get(
            3,
            "power".into(),
            &tx,
            &catalog,
            &Mutex::new(HashMap::new()),
            now,
        )
        .unwrap();
    assert_eq!(session.pending.len(), 1);
    let mut output = Vec::new();
    session.outbound.flush_to(&mut output).unwrap();
    let messages: Vec<serde_json::Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["error"], "runtime_busy");
    assert_eq!(messages[1]["error"], "runtime_busy");

    // Failed admission must leave capacity for a later request of the same ID.
    let _first = rx.try_recv().unwrap();
    session
        .dispatch_set(2, "power".into(), Value::Integer(2), &tx, &catalog, now)
        .unwrap();
    assert_eq!(session.pending.len(), 2);
    assert!(session.outbound.is_empty());
    assert!(matches!(rx.try_recv(), Ok(TestCommand::Set(_))));
    assert!(rx.try_recv().is_err());
}

#[test]
fn queued_mutation_timeout_cancels_execution_and_releases_capacity() {
    let (backend, commands) = TestBackend::new(1);
    let mut session = ClientSession::new(1, 1024);
    let deadline = Instant::now() + Duration::from_secs(3600);
    backend
        .enqueue_set(
            &mut session.pending,
            1,
            deadline,
            "power".into(),
            Value::Integer(20),
        )
        .unwrap();
    let TestCommand::Set(command) = commands.try_recv().unwrap() else {
        panic!("expected Set command")
    };

    session.collect_ready(deadline).unwrap();
    assert!(
        !command.execution.try_start(),
        "timeout must prevent execution"
    );
    assert!(session.pending.is_empty());
    assert_eq!(
        session.next_message().unwrap(),
        Some(json!({"id":1,"status":"error","error":"runtime_timeout"}).to_string())
    );
    session
        .collect_ready(deadline + Duration::from_secs(1))
        .unwrap();
    assert!(session.outbound.is_empty());
    backend
        .enqueue_set(
            &mut session.pending,
            2,
            deadline,
            "power".into(),
            Value::Integer(30),
        )
        .unwrap();
    assert_eq!(session.pending.len(), 1);
}

#[test]
fn started_mutation_timeout_has_unknown_outcome_and_no_second_reply() {
    for value in [
        Value::Integer(20),
        Value::Boolean(true),
        Value::Boolean(false),
    ] {
        let (backend, commands) = TestBackend::new(1);
        let mut session = ClientSession::new(1, 1024);
        let deadline = Instant::now() + Duration::from_secs(3600);
        backend
            .enqueue_set(&mut session.pending, 1, deadline, "value".into(), value)
            .unwrap();
        let TestCommand::Set(command) = commands.try_recv().unwrap() else {
            panic!("expected Set command")
        };
        assert!(command.execution.try_start());
        session.collect_ready(deadline).unwrap();
        assert_eq!(
            session.next_message().unwrap(),
            Some(json!({"id":1,"status":"error","error":"outcome_unknown"}).to_string())
        );
        assert!(session.pending.is_empty());
        assert!(matches!(
            command.complete(Ok(())),
            Err(mpsc::TrySendError::Disconnected(_))
        ));
        session
            .collect_ready(deadline + Duration::from_secs(1))
            .unwrap();
        assert!(session.outbound.is_empty());
        assert!(
            commands.try_recv().is_err(),
            "timeout must not enqueue a retry"
        );
    }
}

#[test]
fn mutation_reply_disconnect_distinguishes_queued_and_started() {
    for started in [false, true] {
        let (backend, commands) = TestBackend::new(1);
        let mut session = ClientSession::new(1, 1024);
        let now = Instant::now();
        let deadline = now + Duration::from_secs(3600);
        backend
            .enqueue_set(
                &mut session.pending,
                1,
                deadline,
                "power".into(),
                Value::Integer(20),
            )
            .unwrap();
        let TestCommand::Set(command) = commands.try_recv().unwrap() else {
            panic!("expected Set command")
        };
        let execution = command.execution.clone();
        if started {
            assert!(execution.try_start());
        }
        drop(command);
        session.collect_ready(now).unwrap();
        let error = if started {
            "outcome_unknown"
        } else {
            "runtime_unavailable"
        };
        assert_eq!(
            session.next_message().unwrap(),
            Some(json!({"id":1,"status":"error","error":error}).to_string())
        );
        assert!(session.pending.is_empty());
        assert!(!execution.try_start());
        session.collect_ready(deadline).unwrap();
        assert!(session.outbound.is_empty());
        assert!(commands.try_recv().is_err());
    }
}

#[test]
fn available_mutation_result_wins_over_timeout_and_is_delivered_once() {
    for result in [Ok(()), Err(CommandError::OutcomeUnknown)] {
        let (backend, commands) = TestBackend::new(1);
        let mut session = ClientSession::new(1, 1024);
        let deadline = Instant::now() + Duration::from_secs(3600);
        backend
            .enqueue_set(
                &mut session.pending,
                1,
                deadline,
                "power".into(),
                Value::Integer(20),
            )
            .unwrap();
        let TestCommand::Set(command) = commands.try_recv().unwrap() else {
            panic!("expected Set command")
        };
        assert!(command.execution.try_start());
        let expected = if result.is_ok() {
            json!({"id":1,"status":"ok"})
        } else {
            json!({"id":1,"status":"error","error":"outcome_unknown"})
        };
        command.complete(result).unwrap();
        session.collect_ready(deadline).unwrap();
        assert_eq!(session.next_message().unwrap(), Some(expected.to_string()));
        assert!(session.pending.is_empty());
        session
            .collect_ready(deadline + Duration::from_secs(1))
            .unwrap();
        assert!(session.outbound.is_empty());
        assert!(commands.try_recv().is_err());
    }
}

#[test]
fn closed_backend_rejects_reads_and_sets_without_consuming_pending_capacity() {
    let (backend, commands) = TestBackend::new(1);
    drop(commands);
    let mut session = ClientSession::new(1, 1024);
    let now = Instant::now();
    let catalog = power_catalog();
    session
        .dispatch_set(
            1,
            "power".into(),
            Value::Integer(20),
            &backend,
            &catalog,
            now,
        )
        .unwrap();
    session
        .dispatch_get(
            2,
            "power".into(),
            &backend,
            &catalog,
            &Mutex::new(HashMap::new()),
            now,
        )
        .unwrap();
    assert!(session.pending.is_empty());
    for id in [1, 2] {
        assert_eq!(
            session.next_message().unwrap(),
            Some(json!({"id":id,"status":"error","error":"runtime_unavailable"}).to_string())
        );
    }
    session
        .collect_ready(now + Duration::from_secs(11))
        .unwrap();
    assert!(session.outbound.is_empty());
}
