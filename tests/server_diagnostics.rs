use lab_server::{
    command_router::CommandRouter,
    config::{RecordingConfig, ServerConfig},
    device_registry::DeviceRegistry,
    driver::{DeviceIo, Driver, DriverError},
    model::{ParameterDescription, Spec, Value},
    port_worker::{DeviceInstance, PortWorker},
    service::DeviceService,
    session::Session,
    startup::{DriverSetup, StartedServer, start_with},
};
use serde_json::{Value as Json, json};
use std::{
    io::{self, BufReader, Cursor},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
#[path = "support/recording_db.rs"]
mod db;
const WAIT: Duration = Duration::from_secs(10);

struct Source(Arc<AtomicUsize>);
impl Driver for Source {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "value".into(),
            unit: "degC".into(),
            spec: Spec::Float {
                min: -100.,
                max: 100.,
            },
            writable: false,
            default_poll_period: Some(Duration::from_secs(3600)),
        }]
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Value::Float(20.))
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        panic!("diagnostics must not write")
    }
}
fn config() -> ServerConfig {
    ServerConfig::from_toml("[tcp]\nbind='127.0.0.1:0'\n[[ports]]\nid='p'\nname='MEMORY'\nbaud=9600\ndata_bits=8\nparity='none'\nstop_bits=1\nflow_control='none'\ntimeout_ms=100\n[[devices]]\nid='a'\ndriver='test'\nport='p'").unwrap()
}
fn start(config: &ServerConfig, unavailable: bool, reads: Arc<AtomicUsize>) -> StartedServer {
    start_with(
        config,
        |_| {
            Ok(DriverSetup {
                driver: Box::new(Source(reads.clone())),
                post_open_delay: Duration::ZERO,
            })
        },
        |_, _| {
            if unavailable {
                Err(io::Error::other("test port unavailable"))
            } else {
                Ok(BufReader::new(Cursor::new(Vec::<u8>::new())))
            }
        },
    )
    .unwrap()
}
fn wait(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
}
fn request(service: &DeviceService, session: &mut Session, id: u64, op: &str) -> Json {
    service
        .receive_message(
            session,
            &json!({"id":id,"op":op}).to_string(),
            Instant::now(),
        )
        .unwrap();
    serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap()
}

#[test]
fn available_and_stopped_port_keep_last_success_without_diagnostic_reads() {
    let reads = Arc::new(AtomicUsize::new(0));
    let server = start(&config(), false, reads.clone());
    let service = server.backend().clone();
    let handle = service.registry().port("p").unwrap().handle().unwrap();
    wait(|| handle.last_success().is_some());
    let mut session = Session::new(64, 65536);
    let first = request(&service, &mut session, 1, "server_status")["data"].clone();
    assert_eq!(first["server"]["state"], "ready");
    assert_eq!(first["server"]["active_clients"], 0);
    assert_eq!(
        first["server"]["listeners"]["websocket"]["state"],
        "disabled"
    );
    assert_eq!(first["ports"][0]["state"], "available");
    assert_eq!(first["ports"][0]["loss_epoch"], 0);
    assert!(first["ports"][0]["last_success_unix_ms"].as_u64().unwrap() > 0);
    assert_eq!(
        first["devices"][0],
        json!({"id":"a","port":"p","state":"available","reason":null})
    );
    for id in 2..100 {
        assert_eq!(
            request(&service, &mut session, id, "server_status")["data"]["ports"],
            first["ports"]
        );
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    handle.shutdown();
    wait(|| !handle.source_alive());
    let stopped = request(&service, &mut session, 100, "server_status")["data"].clone();
    assert_eq!(stopped["ports"][0]["state"], "stopped");
    assert_eq!(stopped["ports"][0]["reason"], "Shutdown");
    assert_eq!(
        stopped["ports"][0]["last_success_unix_ms"],
        first["ports"][0]["last_success_unix_ms"]
    );
    assert_eq!(stopped["devices"][0]["state"], "stopped");
    drop(server);
    assert_eq!(
        service.diagnostics().unwrap()["server"]["listeners"]["tcp"]["state"],
        "closed"
    );
}

#[test]
fn unavailable_port_and_disabled_recorder_have_reasons_and_unknown_measurements() {
    let reads = Arc::new(AtomicUsize::new(0));
    let server = start(&config(), true, reads.clone());
    let status = request(
        server.backend(),
        &mut Session::new(64, 65536),
        1,
        "server_status",
    );
    let port = &status["data"]["ports"][0];
    assert_eq!(port["state"], "unavailable");
    assert!(
        port["reason"]
            .as_str()
            .unwrap()
            .contains("test port unavailable")
    );
    assert!(port["last_success_unix_ms"].is_null());
    assert!(port["loss_epoch"].is_null());
    assert_eq!(status["data"]["devices"][0]["state"], "unavailable");
    assert_eq!(
        status["data"]["recorder"],
        json!({"enabled":false,"state":"disabled","session_id":null,"losses":null,"error":null})
    );
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}

#[test]
fn recorder_running_failure_and_stopped_are_read_from_the_live_handle() {
    for fail in [false, true] {
        let db = db::Database::new();
        let mut config = config();
        config.recording = Some(RecordingConfig {
            audit_enabled: false,
            audit_queue_capacity: None,
            enabled: true,
            path: Some(db.0.clone()),
            queue_capacity: Some(64),
        });
        let reads = Arc::new(AtomicUsize::new(0));
        let server = start(&config, false, reads.clone());
        let service = server.backend().clone();
        let initial = service.diagnostics().unwrap();
        assert_eq!(initial["recorder"]["state"], "running");
        let session_id = initial["recorder"]["session_id"].as_i64().unwrap();
        assert!(session_id > 0);
        if fail {
            db.connect().execute_batch("CREATE TRIGGER fail_status_test BEFORE INSERT ON measurements BEGIN SELECT RAISE(FAIL, 'diagnostics injected write failure'); END;").unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let join = thread::spawn(move || server.run_until(&flag));
        if fail {
            wait(|| service.diagnostics().unwrap()["recorder"]["state"] == "failed");
            let status = request(&service, &mut Session::new(64, 65536), 1, "server_status");
            assert_eq!(status["data"]["server"]["state"], "running");
            assert_eq!(status["data"]["ports"][0]["state"], "available");
            assert_eq!(status["data"]["recorder"]["session_id"], session_id);
            assert!(
                status["data"]["recorder"]["error"]
                    .as_str()
                    .unwrap()
                    .contains("diagnostics injected write failure")
            );
        } else {
            wait(|| db::scalar(&db.connect(), "SELECT count(*) FROM measurements") > 0);
        }
        stop.store(true, Ordering::Release);
        assert_eq!(join.join().unwrap().is_err(), fail);
        let status = service.diagnostics().unwrap();
        assert_eq!(
            status["recorder"]["state"],
            if fail { "failed" } else { "stopped" }
        );
        assert_eq!(status["recorder"]["session_id"], session_id);
        assert_eq!(
            status["recorder"]["losses"],
            json!({"dropped_events":0,"dropped_data_lost_events":0,"rejected_invalid_data":0,"dropped_audit_events":0})
        );
        assert_eq!(status["server"]["state"], "stopped");
    }
}

#[test]
fn oversized_status_is_explicit_and_session_remains_usable() {
    let mut registry = DeviceRegistry::new();
    for n in 0..100 {
        registry
            .register_unavailable_port(format!("p{n}"), "reason".repeat(1000))
            .unwrap();
    }
    let service = DeviceService::new(CommandRouter::new(registry), 8);
    let mut session = Session::new(64, 65536);
    assert_eq!(
        request(&service, &mut session, 1, "server_status")["error"],
        "status_too_large"
    );
    assert_eq!(request(&service, &mut session, 2, "ping")["status"], "ok");
}

#[test]
fn port_losses_survive_stop_and_status_does_not_drain_observations() {
    let reads = Arc::new(AtomicUsize::new(0));
    let mut device = DeviceInstance::new("a", Source(reads)).unwrap();
    device
        .set_poll_period("value", Some(Duration::from_millis(1)))
        .unwrap();
    let (worker, channels) = PortWorker::new(
        BufReader::new(Cursor::new(Vec::<u8>::new())),
        vec![device],
        8,
        1,
    )
    .unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("p", channels.commands.clone())
        .unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let service = DeviceService::new(CommandRouter::new(registry), 8);
    let join = thread::spawn(move || worker.run());
    wait(|| channels.commands.loss_epoch() > 0);
    channels.commands.shutdown();
    join.join().unwrap();
    let loss_epoch = channels.commands.loss_epoch();
    let mut session = Session::new(64, 65536);
    for id in 1..10 {
        let status = request(&service, &mut session, id, "server_status");
        assert_eq!(status["data"]["ports"][0]["loss_epoch"], loss_epoch);
        assert_eq!(status["data"]["ports"][0]["state"], "stopped");
        assert!(status["data"]["ports"][0]["last_success_unix_ms"].is_u64());
    }
    assert!(channels.observations.try_recv().is_ok());
    assert!(channels.observations.try_recv().is_err());
}
