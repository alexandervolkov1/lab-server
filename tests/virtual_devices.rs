use std::fs;
use std::io::{self, BufRead, BufReader, Cursor, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use lab_server::config::ServerConfig;
use lab_server::port_worker::StopReason;
use lab_server::startup::{StartedServer, create_driver, start, start_with};
use lab_server::websocket::{PATH, SUBPROTOCOL};
use serde_json::{Value as Json, json};
use tungstenite::Message;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;

#[path = "support/recording_db.rs"]
mod db;
#[path = "support/experiment_files.rs"]
mod experiment_files;
use experiment_files::ExperimentFiles;

const WAIT: Duration = Duration::from_secs(10);
const POWER: &str = "furnace1/power";
const HEATER: &str = "furnace1/heater_temperature";
const SAMPLE: &str = "furnace1/sample_temperature";
const EMA: &str = "processing/smooth/value";
const PID: &str = "processing/loop/output";

fn connect(addr: SocketAddr) -> TcpStream {
    let socket = TcpStream::connect_timeout(&addr, WAIT).unwrap();
    socket.set_read_timeout(Some(WAIT)).unwrap();
    socket.set_write_timeout(Some(WAIT)).unwrap();
    socket
}

struct TcpClient {
    socket: BufReader<TcpStream>,
    last_id: u64,
}

impl TcpClient {
    fn new(addr: SocketAddr) -> Self {
        Self {
            socket: BufReader::new(connect(addr)),
            last_id: 0,
        }
    }

    fn send(&mut self, mut request: Json) -> u64 {
        self.last_id += 1;
        request["id"] = json!(self.last_id);
        writeln!(self.socket.get_mut(), "{request}").unwrap();
        self.last_id
    }

    fn read(&mut self) -> Json {
        let mut line = String::new();
        assert_ne!(
            self.socket.read_line(&mut line).unwrap(),
            0,
            "unexpected TCP EOF"
        );
        serde_json::from_str(&line).unwrap()
    }

    fn request(&mut self, request: Json) -> Json {
        let id = self.send(request);
        let end = Instant::now() + WAIT;
        loop {
            let reply = self.read();
            if reply["id"] == id {
                return reply;
            }
            assert_eq!(reply["event"], "measurement", "{reply}");
            assert!(Instant::now() < end, "missing reply for request {id}");
        }
    }

    fn get(&mut self, key: &str) -> Json {
        let deadline = Instant::now() + WAIT;
        loop {
            let result = self.request(json!({"op":"get", "key":key}));
            match result["status"].as_str() {
                Some("ok") => return result["value"].clone(),
                // TCP can accept a client before the first poll is published.
                Some("no_data") if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                _ => panic!("get {key}: {result}"),
            }
        }
    }
}

struct Running {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<io::Result<()>>>,
}

impl Running {
    fn new(server: StartedServer) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let signal = stop.clone();
        let join = thread::spawn(move || server.run_until(&signal));
        Self {
            stop,
            join: Some(join),
        }
    }

    fn shutdown(&mut self) -> thread::Result<io::Result<()>> {
        self.stop.store(true, Ordering::Release);
        self.join.take().map_or(Ok(Ok(())), JoinHandle::join)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn next_ws(ws: &mut tungstenite::WebSocket<TcpStream>) -> Json {
    let Message::Text(text) = ws.read().unwrap() else {
        panic!("expected a WebSocket text frame")
    };
    serde_json::from_str(&text).unwrap()
}

fn open_ws(addr: SocketAddr) -> tungstenite::WebSocket<TcpStream> {
    let mut request = format!("ws://{addr}{PATH}").into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(SUBPROTOCOL),
    );
    let (ws, handshake) = tungstenite::client(request, connect(addr)).unwrap();
    assert_eq!(handshake.headers()["sec-websocket-protocol"], SUBPROTOCOL);
    ws
}

#[test]
fn virtual_plant_serves_tcp_websocket_processing_and_sqlite_and_restarts_fresh() {
    let input = include_str!("../examples/virtual-plant.toml");
    check_virtual_experiment(ServerConfig::from_toml(input).unwrap(), 20.0);
}

#[test]
fn virtual_plant_runs_the_external_experiment_in_shadow() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/virtual-plant-external.toml");
    check_virtual_experiment(ServerConfig::load(path).unwrap(), 35.0);
}

fn check_virtual_experiment(mut config: ServerConfig, setpoint: f64) {
    let db = db::Database::new();
    config.tcp.bind.set_port(0);
    config
        .websocket
        .as_mut()
        .unwrap()
        .bind
        .as_mut()
        .unwrap()
        .set_port(0);
    config.recording.as_mut().unwrap().path = Some(db.0.clone());
    for key in ["heater_temperature", "sample_temperature", "power"] {
        config.devices[0].poll_periods_ms.insert(key.into(), 50);
    }
    assert!(config.ports.is_empty());
    let server = start(&config).unwrap();
    let port_id = &config.virtual_ports[0].id;
    let worker = server
        .backend()
        .registry()
        .port(port_id)
        .unwrap()
        .handle()
        .unwrap()
        .clone();
    let tcp_addr = server.local_addr().unwrap();
    let ws_addr = server.websocket_addr().unwrap().unwrap();
    let mut running = Running::new(server);
    let mut tcp = TcpClient::new(tcp_addr);

    assert_eq!(tcp.request(json!({"op":"hello"}))["version"], 1);
    let catalog = tcp.request(json!({"op":"describe"}));
    let parameters = catalog["parameters"].as_array().unwrap();
    for (key, kind, unit, writable) in [
        (HEATER, "float", "degC", false),
        (SAMPLE, "float", "degC", false),
        ("furnace1/room_temperature", "float", "degC", false),
        (POWER, "integer", "%", true),
    ] {
        let parameter = parameters.iter().find(|p| p["key"] == key).unwrap();
        assert_eq!(parameter["type"], kind);
        assert_eq!(parameter["unit"], unit);
        assert_eq!(parameter["writable"], writable);
    }
    assert_eq!(tcp.get(POWER), 0);
    assert_eq!(tcp.get(SAMPLE).as_f64(), Some(20.0));
    assert_eq!(tcp.get(EMA).as_f64(), Some(20.0));
    let proposal = tcp.get(PID).as_f64().unwrap();
    if setpoint > 20.0 {
        assert!(proposal > 0.0, "expected a positive Shadow proposal");
    } else {
        assert_eq!(proposal, 0.0);
    }
    assert_eq!(tcp.get(POWER), 0);
    assert_eq!(
        tcp.request(json!({"op":"set", "key":POWER, "value":101}))["status"],
        "error"
    );
    assert_eq!(
        tcp.request(json!({"op":"set", "key":POWER, "value":65}))["status"],
        "ok"
    );

    let mut ws = open_ws(ws_addr);
    ws.send(Message::Text(
        json!({"id":1,"op":"subscribe","keys":[EMA, PID]})
            .to_string()
            .into(),
    ))
    .unwrap();
    let ack = next_ws(&mut ws);
    assert_eq!(ack["id"], 1);
    assert_eq!(ack["status"], "ok");
    let mut derived = [false; 2];
    let deadline = Instant::now() + WAIT;
    while !derived.iter().all(|seen| *seen) {
        let event = next_ws(&mut ws);
        assert_eq!(event["event"], "measurement", "{event}");
        assert_eq!(event["status"], "ok", "{event}");
        assert!(event["value"].is_f64(), "{event}");
        let key = event["key"].as_str().unwrap();
        derived[[EMA, PID]
            .iter()
            .position(|expected| *expected == key)
            .unwrap()] = true;
        assert!(Instant::now() < deadline, "missing derived measurements");
    }

    assert_eq!(
        tcp.request(json!({"op":"subscribe", "key":HEATER}))["status"],
        "ok"
    );
    let deadline = Instant::now() + WAIT;
    loop {
        let event = tcp.read();
        assert_eq!(event["event"], "measurement", "{event}");
        assert_eq!(event["key"], HEATER);
        assert_eq!(event["unit"], "degC");
        assert!(event["completed_at_unix_ms"].as_u64().unwrap() > 0);
        if event["value"].as_f64().unwrap() > 20.0 {
            break;
        }
        assert!(Instant::now() < deadline, "heater did not warm up");
    }
    let ack = tcp.request(json!({"op":"unsubscribe", "keys":[HEATER]}));
    assert_eq!(ack["status"], "ok");
    assert_eq!(ack["keys"], json!([]));
    let id = tcp.send(json!({"op":"hello"}));
    assert_eq!(tcp.read()["id"], id);

    let list = tcp.request(json!({"op":"components", "command":{"action":"list"}}));
    let instances = list["data"]["instances"].as_array().unwrap();
    let pid = instances
        .iter()
        .find(|p| p["configuration"]["name"] == "loop")
        .unwrap();
    assert_eq!(pid["state"], "shadow");
    assert_eq!(pid["binding"]["target"], POWER);
    assert_eq!(pid["configuration"]["parameters"]["setpoint"], setpoint);
    let outputs = tcp.request(json!({"op":"components", "command":{"action":"outputs"}}));
    let output = &outputs["data"][0];
    assert_eq!(output["mode"], "manual");
    assert!(output["owner"].is_null());
    assert_eq!(tcp.get(POWER), 65);
    drop(ws);
    drop(tcp);
    running.shutdown().unwrap().unwrap();
    assert_eq!(worker.stop_reason(), Some(StopReason::Shutdown));
    assert!(!worker.source_alive());

    {
        let c = db.connect();
        let metadata: String = c.query_row(
            "SELECT source_metadata FROM measurements WHERE port_id=?1 AND global_key=?2 AND local_source='heater_temperature' AND value_type='float' AND real_value > 20.0 LIMIT 1",
            [port_id.as_str(), HEATER],
            |row| row.get(0),
        ).unwrap();
        let metadata: Json = serde_json::from_str(&metadata).unwrap();
        assert_eq!(
            metadata["source_instance"],
            worker.source_instance().to_string()
        );
        assert!(
            metadata["sequence"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
        assert_eq!(metadata["generation"], "0");
        assert_eq!(
            db::scalar(
                &c,
                "SELECT count(DISTINCT global_key) FROM measurements WHERE global_key IN ('processing/smooth/value', 'processing/loop/output') AND read_error IS NULL"
            ),
            2
        );
        assert_eq!(
            db::scalar(
                &c,
                "SELECT count(*) FROM audit_events WHERE kind='completed' AND json_extract(details_json, '$.status')='ok'"
            ),
            1
        );
    }

    let server = start(&config).unwrap();
    let restarted = server
        .backend()
        .registry()
        .port(port_id)
        .unwrap()
        .handle()
        .unwrap()
        .clone();
    assert_ne!(restarted.source_instance(), worker.source_instance());
    let addr = server.local_addr().unwrap();
    let mut running = Running::new(server);
    let mut tcp = TcpClient::new(addr);
    assert_eq!(tcp.get(POWER), 0);
    assert_eq!(tcp.get(HEATER).as_f64(), Some(20.0));
    drop(tcp);
    running.shutdown().unwrap().unwrap();
    assert_eq!(restarted.stop_reason(), Some(StopReason::Shutdown));
    let c = db.connect();
    assert_eq!(
        db::scalar(
            &c,
            "SELECT count(*) FROM sessions WHERE ended_at_ns IS NOT NULL"
        ),
        2
    );
    // Only the test's explicit write reached a driver across both startups.
    assert_eq!(
        db::scalar(
            &c,
            "SELECT count(*) FROM audit_events WHERE kind='started' AND json_extract(context_json, '$.request.op')='set'"
        ),
        1
    );
}

#[test]
fn mixed_startup_injects_only_physical_ports_and_keeps_virtual_instances_independent() {
    let mut config =
        ServerConfig::from_toml(include_str!("../examples/virtual-plant.toml")).unwrap();
    config.tcp.bind.set_port(0);
    config.websocket = None;
    config.recording = None;
    config.devices[0].poll_periods_ms.insert("power".into(), 25);
    let mut port = config.virtual_ports[0].clone();
    port.id = "simulated_furnace2".into();
    let mut device = config.devices[0].clone();
    device.id = "furnace2".into();
    device.port = port.id.clone();
    config.virtual_ports.push(port);
    config.devices.push(device);
    let physical = ServerConfig::from_toml(include_str!("../examples/processing.toml")).unwrap();
    config.ports = physical.ports;
    config
        .devices
        .extend(physical.devices.into_iter().map(|mut device| {
            device.id = "unavailable_furnace".into();
            device
        }));
    let mut open_calls = 0;
    let server = start_with(
        &config,
        |device| {
            assert_eq!(device.id, "unavailable_furnace");
            create_driver(device)
        },
        |port, delay| {
            assert_eq!(port.id, "arduino_link");
            assert_eq!(delay, Duration::from_secs(2));
            open_calls += 1;
            Err::<BufReader<Cursor<Vec<u8>>>, _>(io::Error::other("mock port unavailable"))
        },
    )
    .unwrap();
    assert_eq!(open_calls, 1);
    assert!(
        server
            .backend()
            .registry()
            .port("arduino_link")
            .unwrap()
            .handle()
            .is_none()
    );
    let addr = server.local_addr().unwrap();
    let mut running = Running::new(server);
    let mut tcp = TcpClient::new(addr);
    assert_eq!(
        tcp.request(json!({"op":"set","key":POWER,"value":80}))["status"],
        "ok"
    );
    // A write acknowledgment precedes publication of the next polled value.
    assert_eq!(
        tcp.request(json!({"op":"subscribe","key":POWER}))["status"],
        "ok"
    );
    let deadline = Instant::now() + WAIT;
    loop {
        let event = tcp.read();
        assert_eq!(event["event"], "measurement", "{event}");
        assert_eq!(event["key"], POWER);
        if event["value"] == 80 {
            break;
        }
        assert!(Instant::now() < deadline, "updated power was not published");
    }
    assert_eq!(tcp.get(POWER), 80);
    assert_eq!(tcp.get("furnace2/power"), 0);
    drop(tcp);
    running.shutdown().unwrap().unwrap();
}

fn components(tcp: &mut TcpClient, command: Json) -> Json {
    let reply = tcp.request(json!({"op":"components", "command":command}));
    assert_eq!(reply["status"], "ok", "{reply}");
    reply["data"].clone()
}

fn instance<'a>(page: &'a Json, name: &str) -> &'a Json {
    page["instances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["configuration"]["name"] == name)
        .unwrap()
}

fn output<'a>(outputs: &'a Json, key: &str) -> &'a Json {
    outputs
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["key"] == key)
        .unwrap()
}

#[test]
fn save_reloads_live_experiment_and_preserves_active_controller() {
    const NEW_TARGET: &str = "furnace2/power";
    let files = ExperimentFiles::new();
    let main = include_str!("../examples/virtual-plant-external.toml");
    fs::write(files.main(), format!("{main}\n[[virtual_ports]]\nid='second'\nmodel='thermal_plant'\n[[devices]]\nid='furnace2'\ndriver='virtual'\nport='second'\n")).unwrap();
    let initial = include_str!("../examples/experiments/thermal-loop.toml");
    fs::write(files.experiment(), initial).unwrap();
    let db = db::Database::new();
    let open_server = || {
        let mut config = ServerConfig::load(files.main()).unwrap();
        config.tcp.bind.set_port(0);
        config
            .websocket
            .as_mut()
            .unwrap()
            .bind
            .as_mut()
            .unwrap()
            .set_port(0);
        config.recording.as_mut().unwrap().path = Some(db.0.clone());
        config.devices[0]
            .poll_periods_ms
            .insert("sample_temperature".into(), 50);
        start(&config).unwrap()
    };
    let server = open_server();
    let addr = server.local_addr().unwrap();
    let ws_addr = server.websocket_addr().unwrap().unwrap();
    let mut running = Running::new(server);
    let mut tcp = TcpClient::new(addr);

    let rejected =
        tcp.request(json!({"op":"components","command":{"action":"save","path":"ignored.toml"}}));
    assert_eq!(rejected["status"], "error");
    assert!(
        rejected["error"]
            .as_str()
            .unwrap()
            .contains("unknown field `path`"),
        "{rejected}"
    );
    assert_eq!(fs::read_to_string(files.experiment()).unwrap(), initial);

    let mut page = components(
        &mut tcp,
        json!({"action":"parameter_set","name":"loop","parameter":"setpoint","value":46.5}),
    );
    page = components(
        &mut tcp,
        json!({"action":"parameter_set","name":"smooth","parameter":"tau","value":3.75,"expected_revision":page["revision"]}),
    );
    for name in ["alternate", "discarded"] {
        page = components(
            &mut tcp,
            json!({"action":"create","configuration":{"name":name,"type_id":"ema","inputs":{"input":SAMPLE},"max_age_ms":5000},"expected_revision":page["revision"]}),
        );
    }
    page = components(
        &mut tcp,
        json!({"action":"create","configuration":{"name":"monitor","type_id":"pid","inputs":{"input":"processing/alternate/value"},"max_age_ms":5000},"expected_revision":page["revision"]}),
    );
    let mut pid = instance(&page, "loop")["configuration"].clone();
    pid["inputs"]["input"] = json!("processing/alternate/value");
    pid["parameters"]["kp"] = json!(1.5);
    pid["parameters"]["ki"] = json!(0.25);
    pid["parameters"]["kd"] = json!(0.125);
    page = components(
        &mut tcp,
        json!({"action":"configure","configuration":pid,"expected_revision":page["revision"]}),
    );
    page = components(
        &mut tcp,
        json!({"action":"state","name":"loop","state":"shadow","expected_revision":page["revision"]}),
    );
    page = components(
        &mut tcp,
        json!({"action":"bind","name":"loop","target":NEW_TARGET,"command_timeout_ms":1750,"expected_revision":page["revision"]}),
    );
    page = components(
        &mut tcp,
        json!({"action":"delete","name":"discarded","expected_revision":page["revision"]}),
    );
    let revision = page["revision"].as_u64().unwrap();
    assert_eq!(
        components(&mut tcp, json!({"action":"save"})),
        json!({"revision":revision})
    );
    assert_eq!(components(&mut tcp, json!({"action":"list"})), page);
    let saved = fs::read(files.experiment()).unwrap();
    let loaded = ServerConfig::load(files.main())
        .unwrap()
        .processing
        .unwrap()
        .components;
    let names: Vec<_> = loaded
        .iter()
        .map(|component| component.configuration.name.as_str())
        .collect();
    assert_eq!(names, ["alternate", "loop", "monitor", "smooth"]);
    assert_eq!(loaded[0].configuration.parameters["tau"], 1.0);
    assert_eq!(loaded[1].configuration.parameters["setpoint"], 46.5);
    assert_eq!(loaded[1].configuration.parameters["kp"], 1.5);
    assert_eq!(loaded[1].configuration.parameters["ki"], 0.25);
    assert_eq!(loaded[1].configuration.parameters["kd"], 0.125);
    assert_eq!(
        loaded[1].configuration.inputs["input"],
        "processing/alternate/value"
    );
    assert_eq!(loaded[1].configuration.max_age_ms, 5000);
    let binding = loaded[1].binding.as_ref().unwrap();
    assert_eq!(binding.target, NEW_TARGET);
    assert_eq!(binding.command_timeout_ms, 1750);
    assert_eq!(loaded[2].configuration.parameters["kp"], 1.0);
    assert!(loaded[2].binding.is_none());
    assert_eq!(loaded[3].configuration.parameters["tau"], 3.75);

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        let held = fs::OpenOptions::new()
            .read(true)
            .share_mode(1)
            .open(files.experiment())
            .unwrap();
        let response = tcp.request(json!({"op":"components","command":{"action":"save"}}));
        assert_eq!(response["status"], "error");
        assert_eq!(response["error"], "experiment_write_failed");
        drop(held);
        assert_eq!(fs::read(files.experiment()).unwrap(), saved);
        assert_eq!(
            fs::read_dir(files.experiment().parent().unwrap())
                .unwrap()
                .count(),
            1
        );
    }

    // One WebSocket and two TCP client threads save the same revision concurrently.
    let barrier = Barrier::new(3);
    thread::scope(|scope| {
        let ws = scope.spawn(|| {
            barrier.wait();
            let mut ws = open_ws(ws_addr);
            ws.send(Message::Text(
                json!({"id":1,"op":"components","command":{"action":"save"}})
                    .to_string()
                    .into(),
            ))
            .unwrap();
            let reply = next_ws(&mut ws);
            assert_eq!(
                reply,
                json!({"id":1,"status":"ok","data":{"revision":revision}})
            );
        });
        let tcp_saves: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    assert_eq!(
                        components(&mut TcpClient::new(addr), json!({"action":"save"})),
                        json!({"revision":revision})
                    );
                })
            })
            .collect();
        ws.join().unwrap();
        for save in tcp_saves {
            save.join().unwrap();
        }
    });
    assert_eq!(fs::read(files.experiment()).unwrap(), saved);
    assert_eq!(
        fs::read_dir(files.experiment().parent().unwrap())
            .unwrap()
            .count(),
        1
    );

    assert!(tcp.get(PID).as_f64().unwrap() > 0.0);
    let auto = components(
        &mut tcp,
        json!({"action":"state","name":"loop","state":"auto","expected_revision":revision}),
    );
    assert_eq!(instance(&auto, "loop")["state"], "auto");
    let before = components(&mut tcp, json!({"action":"outputs"}));
    assert_eq!(output(&before, NEW_TARGET)["mode"], "auto");
    assert_eq!(
        components(&mut tcp, json!({"action":"save"}))["revision"],
        auto["revision"]
    );
    assert_eq!(components(&mut tcp, json!({"action":"list"})), auto);
    let after = components(&mut tcp, json!({"action":"outputs"}));
    for field in ["mode", "owner", "generation"] {
        assert_eq!(
            output(&before, NEW_TARGET)[field],
            output(&after, NEW_TARGET)[field]
        );
    }
    assert_eq!(fs::read(files.experiment()).unwrap(), saved);

    drop(tcp);
    running.shutdown().unwrap().unwrap();
    {
        let c = db.connect();
        assert_eq!(
            db::scalar(
                &c,
                "SELECT count(*) FROM audit_events WHERE kind='save_result' AND json_extract(details_json, '$.status')='ok'"
            ),
            5
        );
        assert_eq!(
            db::scalar(
                &c,
                "SELECT count(*) FROM audit_events WHERE kind='rejected' AND json_extract(context_json, '$.request.command.action')='save'"
            ),
            1
        );
        assert_eq!(
            db::scalar(
                &c,
                &format!(
                    "SELECT count(*) FROM audit_events WHERE kind='save_result' AND json_extract(context_json, '$.source')='websocket' AND json_extract(details_json, '$.revision')={revision}"
                )
            ),
            1
        );
        #[cfg(windows)]
        assert_eq!(
            db::scalar(
                &c,
                &format!(
                    "SELECT count(*) FROM audit_events WHERE kind='save_result' AND json_extract(details_json, '$.status')='error' AND json_extract(details_json, '$.revision')={revision}"
                )
            ),
            1
        );
    }

    let server = open_server();
    let addr = server.local_addr().unwrap();
    let mut running = Running::new(server);
    let mut tcp = TcpClient::new(addr);
    let restarted = components(&mut tcp, json!({"action":"list"}));
    assert_eq!(restarted["revision"], 5); // Four Creates and one Bind.
    for name in names {
        assert_eq!(
            instance(&restarted, name)["configuration"],
            instance(&page, name)["configuration"]
        );
    }
    let pid = instance(&restarted, "loop");
    assert_eq!(pid["state"], "shadow");
    assert_eq!(
        pid["binding"],
        json!({"target":NEW_TARGET,"command_timeout_ms":1750})
    );
    assert_eq!(instance(&restarted, "monitor")["state"], "shadow");
    let authority = components(&mut tcp, json!({"action":"outputs"}));
    for key in [POWER, NEW_TARGET] {
        assert_eq!(output(&authority, key)["mode"], "manual");
        assert!(output(&authority, key)["owner"].is_null());
        assert_eq!(tcp.get(key), 0);
    }
    drop(tcp);
    running.shutdown().unwrap().unwrap();
}

#[test]
fn inline_save_is_rejected_and_audited_without_rewriting_main_toml() {
    let files = ExperimentFiles::new();
    let input = include_str!("../examples/virtual-plant.toml");
    fs::write(files.main(), input).unwrap();
    let mut config = ServerConfig::load(files.main()).unwrap();
    let db = db::Database::new();
    config.recording.as_mut().unwrap().path = Some(db.0.clone());
    config.tcp.bind.set_port(0);
    config.websocket = None;
    let server = start(&config).unwrap();
    let addr = server.local_addr().unwrap();
    let mut running = Running::new(server);
    let mut tcp = TcpClient::new(addr);
    let reply = tcp.request(json!({"op":"components","command":{"action":"save"}}));
    assert_eq!(
        reply,
        json!({"id":1,"status":"error","error":"no_experiment_file"})
    );
    assert_eq!(fs::read_to_string(files.main()).unwrap(), input);
    drop(tcp);
    running.shutdown().unwrap().unwrap();
    assert_eq!(
        db::scalar(
            &db.connect(),
            "SELECT count(*) FROM audit_events WHERE kind='rejected' AND json_extract(details_json, '$.error')='no_experiment_file' AND json_extract(details_json, '$.revision')=3"
        ),
        1
    );
}
