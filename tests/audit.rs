//! No physical equipment: every write rendezvous is controlled by the test.
use lab_server::{
    command_router::CommandRouter,
    device_registry::DeviceRegistry,
    driver::{DeviceIo, Driver, DriverError, PortState},
    model::{ActionDescription, ParameterDescription, Spec, Value},
    port_worker::{DeviceInstance, PortChannels, PortWorker},
    recorder::{Recorder, RecorderOptions, RecorderState},
    service::DeviceService,
    session::Session,
};
use serde_json::{Value as Json, json};
use std::{
    io::{BufReader, Cursor},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
#[path = "support/recording_db.rs"]
mod db;
const WAIT: Duration = Duration::from_secs(10);
struct DriverStub {
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<bool>,
}
impl Driver for DriverStub {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "value".into(),
            unit: "".into(),
            spec: Spec::Boolean,
            writable: true,
            default_poll_period: None,
        }]
    }
    fn actions(&self) -> Vec<ActionDescription> {
        vec![ActionDescription {
            name: "reset".into(),
            description: "Reset".into(),
        }]
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        Ok(Value::Boolean(false))
    }
    fn write(&mut self, _: &str, _: &Value, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.invoke_action("reset", io)
    }
    fn invoke_action(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.entered.send(()).unwrap();
        if self.resume.recv_timeout(WAIT).unwrap() {
            Ok(())
        } else {
            Err(DriverError::new(
                "simulated failure",
                PortState::Synchronized,
            ))
        }
    }
}
struct Fixture {
    database: db::Database,
    recorder: Recorder,
    service: DeviceService,
    worker: PortWorker<BufReader<Cursor<Vec<u8>>>>,
    channels: PortChannels,
    entered: mpsc::Receiver<()>,
    resume: mpsc::Sender<bool>,
}
fn fixture() -> Fixture {
    let database = db::Database::new();
    let recorder = Recorder::with_options(
        &database.0,
        RecorderOptions {
            audit_enabled: true,
            measurements_enabled: false,
            ..Default::default()
        },
    )
    .unwrap();
    let (tx, entered) = mpsc::channel();
    let (resume, rx) = mpsc::channel();
    let (worker, channels) = PortWorker::new(
        BufReader::new(Cursor::new(vec![])),
        vec![
            DeviceInstance::new(
                "a",
                DriverStub {
                    entered: tx,
                    resume: rx,
                },
            )
            .unwrap(),
        ],
        8,
        8,
    )
    .unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("p", channels.commands.clone())
        .unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let service = DeviceService::new(CommandRouter::new(registry), 8);
    service.enable_audit(recorder.handle());
    Fixture {
        database,
        recorder,
        service,
        worker,
        channels,
        entered,
        resume,
    }
}
fn send(service: &DeviceService, session: &mut Session, request: Json) {
    service
        .receive_message(session, &request.to_string(), Instant::now())
        .unwrap();
}
fn response(session: &mut Session) -> Json {
    let end = Instant::now() + WAIT;
    loop {
        session.collect_ready(Instant::now()).unwrap();
        if let Some(s) = session.next_message().unwrap() {
            return serde_json::from_str(&s).unwrap();
        }
        assert!(Instant::now() < end);
        thread::yield_now();
    }
}
fn events(database: &db::Database) -> Vec<(String, Json, Json)> {
    let c = database.connect();
    c.prepare("SELECT kind,context_json,details_json FROM audit_events ORDER BY sequence")
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                serde_json::from_str(&r.get::<_, String>(1)?).unwrap(),
                serde_json::from_str(&r.get::<_, String>(2)?).unwrap(),
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
#[test]
fn manual_set_action_and_driver_failure_keep_distinct_results() {
    for (op, success) in [("set", true), ("invoke_action", true), ("set", false)] {
        let f = fixture();
        let mut session = Session::new(8, 65536);
        let join = thread::spawn(move || f.worker.run());
        send(
            &f.service,
            &mut session,
            if op == "set" {
                json!({"id":1,"op":op,"key":"a/value","value":true})
            } else {
                json!({"id":1,"op":op,"key":"a/reset"})
            },
        );
        f.entered.recv_timeout(WAIT).unwrap();
        f.resume.send(success).unwrap();
        let reply = response(&mut session);
        assert_eq!(reply["status"], if success { "ok" } else { "error" });
        f.channels.commands.shutdown();
        join.join().unwrap();
        f.recorder.shutdown().unwrap();
        let e = events(&f.database);
        assert_eq!(
            e.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
            ["accepted", "started", "completed", "response"]
        );
        assert_eq!(e[0].1["source"], "tcp");
        assert_eq!(e[0].1["request_id"], "1");
        assert!(e[0].1["connection_id"].is_string());
        assert_eq!(e[0].1["request"]["op"], op);
        assert_eq!(e[2].2["status"], if success { "ok" } else { "error" });
        if !success {
            assert_eq!(e[2].2["message"], "simulated failure");
            assert_eq!(e[3].2["error"], "outcome_unknown");
        }
        assert_eq!(
            db::scalar(
                &f.database.connect(),
                "SELECT audit_incomplete FROM sessions"
            ),
            0
        );
    }
}
#[test]
fn validation_and_duplicate_requests_never_write() {
    let f = fixture();
    let mut session = Session::new(8, 65536);
    for req in [
        json!({"id":1,"op":"set","key":"a/value","value":32}),
        json!({"id":1,"op":"set","key":"a/value","value":true}),
        json!({"id":2,"op":"invoke_action","key":"a/missing"}),
    ] {
        send(&f.service, &mut session, req);
        assert_eq!(response(&mut session)["status"], "error");
    }
    assert!(f.entered.try_recv().is_err());
    f.recorder.shutdown().unwrap();
    let c = f.database.connect();
    assert_eq!(
        db::scalar(
            &c,
            "SELECT count(*) FROM audit_events WHERE kind='rejected'"
        ),
        3
    );
    assert_eq!(
        db::scalar(&c, "SELECT count(*) FROM audit_events WHERE kind='started'"),
        0
    );
}
#[test]
fn cancellation_before_start_prevents_driver_and_records_one_response() {
    let f = fixture();
    let mut session = Session::new(8, 65536);
    send(
        &f.service,
        &mut session,
        json!({"id":1,"op":"set","key":"a/value","value":true}),
    );
    session.collect_ready(Instant::now() + WAIT).unwrap();
    assert_eq!(response(&mut session)["error"], "runtime_timeout");
    let join = thread::spawn(move || f.worker.run());
    f.channels.commands.shutdown();
    join.join().unwrap();
    assert!(f.entered.try_recv().is_err());
    f.recorder.shutdown().unwrap();
    let e = events(&f.database);
    assert_eq!(
        e.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
        ["accepted", "cancelled", "response"]
    );
}
#[test]
fn timeout_after_start_keeps_late_success_after_unknown() {
    let f = fixture();
    let mut session = Session::new(8, 65536);
    let join = thread::spawn(move || f.worker.run());
    send(
        &f.service,
        &mut session,
        json!({"id":1,"op":"set","key":"a/value","value":true}),
    );
    f.entered.recv_timeout(WAIT).unwrap();
    session.collect_ready(Instant::now() + WAIT).unwrap();
    assert_eq!(response(&mut session)["error"], "outcome_unknown");
    f.resume.send(true).unwrap();
    f.channels.commands.shutdown();
    join.join().unwrap();
    session.collect_ready(Instant::now() + WAIT).unwrap();
    assert!(session.next_message().unwrap().is_none());
    f.recorder.shutdown().unwrap();
    let e = events(&f.database);
    assert_eq!(
        e.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
        ["accepted", "started", "response", "completed"]
    );
    assert_eq!(e[3].2["status"], "ok");
}
#[test]
fn disconnected_session_does_not_invent_response_or_cancel_started_write() {
    let f = fixture();
    let mut session = Session::new(8, 65536);
    let join = thread::spawn(move || f.worker.run());
    send(
        &f.service,
        &mut session,
        json!({"id":1,"op":"set","key":"a/value","value":true}),
    );
    f.entered.recv_timeout(WAIT).unwrap();
    drop(session);
    f.resume.send(true).unwrap();
    f.channels.commands.shutdown();
    join.join().unwrap();
    f.recorder.shutdown().unwrap();
    let e = events(&f.database);
    assert_eq!(
        e.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
        ["accepted", "started", "detached", "completed"]
    );
}
#[test]
fn sqlite_failure_is_sticky_but_equipment_continues_without_retries() {
    let f = fixture();
    let handle = f.recorder.handle();
    f.database.connect().execute_batch("CREATE TRIGGER fail_audit BEFORE INSERT ON audit_events BEGIN SELECT RAISE(FAIL,'simulated disk failure'); END;").unwrap();
    let mut session = Session::new(8, 65536);
    let join = thread::spawn(move || f.worker.run());
    for id in 1..=2 {
        send(
            &f.service,
            &mut session,
            json!({"id":id,"op":"set","key":"a/value","value":true}),
        );
        f.entered.recv_timeout(WAIT).unwrap();
        f.resume.send(true).unwrap();
        assert_eq!(response(&mut session)["status"], "ok");
        let end = Instant::now() + WAIT;
        while !matches!(handle.state(), RecorderState::Failed(_)) {
            assert!(Instant::now() < end);
            thread::yield_now();
        }
    }
    f.channels.commands.shutdown();
    join.join().unwrap();
    assert!(f.entered.try_recv().is_err());
    assert!(f.recorder.shutdown().is_err());
    assert_eq!(handle.losses().dropped_audit_events, 8);
    let c = f.database.connect();
    assert_eq!(db::scalar(&c, "SELECT audit_incomplete FROM sessions"), 1);
    assert_eq!(
        db::scalar(
            &c,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NULL"
        ),
        1
    );
}

#[test]
fn rejected_component_mutation_and_read_exclusions() {
    let f = fixture();
    let mut session = Session::new(8, 65536);
    let commands = [
        json!({"action":"create","expected_revision":0,"configuration":{"name":"loop","type_id":"pid","inputs":{"input":"a/value"},"parameters":{},"max_age_ms":10000}}),
    ];
    // Boolean signals cannot drive PID; a filter/controller rejection is still auditable.
    send(
        &f.service,
        &mut session,
        json!({"id":1,"op":"components","command":commands[0]}),
    );
    assert_eq!(response(&mut session)["status"], "error");
    for (id, command) in [
        (2, json!({"action":"types"})),
        (3, json!({"action":"list"})),
        (4, json!({"action":"outputs"})),
    ] {
        send(
            &f.service,
            &mut session,
            json!({"id":id,"op":"components","command":command}),
        );
        response(&mut session);
    }
    send(&f.service, &mut session, json!({"id":5,"op":"ping"}));
    response(&mut session);
    f.recorder.shutdown().unwrap();
    assert_eq!(events(&f.database).len(), 2);
}
