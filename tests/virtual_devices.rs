use std::io::{self, BufRead, BufReader, Cursor, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

    let mut ws_request = format!("ws://{ws_addr}{PATH}")
        .into_client_request()
        .unwrap();
    ws_request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(SUBPROTOCOL),
    );
    let (mut ws, handshake) = tungstenite::client(ws_request, connect(ws_addr)).unwrap();
    assert_eq!(handshake.headers()["sec-websocket-protocol"], SUBPROTOCOL);
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
