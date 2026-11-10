use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use lab_server::config::ServerConfig;
use lab_server::driver::{DeviceIo, Driver, DriverError};
use lab_server::model::{Observation, ParameterDescription, Spec, Value};
use lab_server::observation_aggregator::AggregatorEvent;
use lab_server::port_worker::StopReason;
use lab_server::service::DeviceService;
use lab_server::startup::{DriverSetup, start_with};
use lab_server::websocket::{PATH, SUBPROTOCOL};
use serde_json::{Value as Json, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::HandshakeError;
use tungstenite::http::{HeaderValue, StatusCode};
use tungstenite::protocol::frame::Frame;
use tungstenite::protocol::frame::coding::{CloseCode, Data, OpCode};
use tungstenite::{Error, Message, WebSocket};

#[path = "support/recording_db.rs"]
mod db;
const WAIT: Duration = Duration::from_secs(15);

struct MemoryIo(Cursor<Vec<u8>>);
impl Read for MemoryIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}
impl BufRead for MemoryIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.0.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.0.consume(n);
    }
}
impl DeviceIo for MemoryIo {
    fn send(&mut self, _: &[u8]) -> io::Result<()> {
        Ok(())
    }
}

struct Gate {
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
struct MockDriver {
    values: HashMap<String, Value>,
    polls: Arc<AtomicU64>,
    gate: Arc<Mutex<Option<Gate>>>,
}
impl MockDriver {
    fn new(polls: Arc<AtomicU64>, gate: Arc<Mutex<Option<Gate>>>) -> Self {
        Self {
            polls,
            gate,
            values: HashMap::from([
                ("count".into(), Value::Integer(i64::MAX)),
                ("level".into(), Value::Float(1.25)),
                ("enabled".into(), Value::Boolean(true)),
                ("temperature".into(), Value::Float(20.0)),
                ("flag".into(), Value::Boolean(true)),
            ]),
        }
    }
}
impl Driver for MockDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        [
            (
                "count",
                Spec::Integer {
                    min: i64::MIN,
                    max: i64::MAX,
                },
                "count",
                None,
            ),
            (
                "level",
                Spec::Float {
                    min: -100.0,
                    max: 100.0,
                },
                "V",
                None,
            ),
            ("enabled", Spec::Boolean, "", None),
            (
                "temperature",
                Spec::Float {
                    min: -100.0,
                    max: 500.0,
                },
                "degC",
                Some(Duration::from_secs(3600)),
            ),
            ("flag", Spec::Boolean, "", Some(Duration::from_secs(3600))),
            (
                "sample",
                Spec::Integer {
                    min: 0,
                    max: i64::MAX,
                },
                "count",
                Some(Duration::from_secs(3600)),
            ),
        ]
        .into_iter()
        .map(
            |(id, spec, unit, default_poll_period)| ParameterDescription {
                id: id.into(),
                spec,
                unit: unit.into(),
                writable: default_poll_period.is_none(),
                default_poll_period,
            },
        )
        .collect()
    }
    fn read(&mut self, key: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        if key == "sample" {
            return Ok(Value::Integer(
                self.polls.fetch_add(1, Ordering::SeqCst) as i64
            ));
        }
        if matches!(key, "temperature" | "flag") {
            self.polls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(self.values[key].clone())
    }
    fn write(&mut self, key: &str, value: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.resume.recv_timeout(WAIT).unwrap();
        }
        self.values.insert(key.into(), value.clone());
        Ok(())
    }
}

fn configuration() -> ServerConfig {
    let mut text = String::from(
        "[tcp]\nbind='127.0.0.1:0'\n[websocket]\nenabled=true\nbind='127.0.0.1:0'\nallowed_origins=['http://localhost:3000']\n",
    );
    for (port, device) in [("p", "a"), ("q", "b")] {
        text += &format!(
            "[[ports]]\nid='{port}'\nname='MOCK-{port}'\nbaud=9600\ndata_bits=8\nparity='none'\nstop_bits=1\nflow_control='none'\ntimeout_ms=100\n[[devices]]\nid='{device}'\ndriver='mock'\nport='{port}'\n"
        );
    }
    ServerConfig::from_toml(&text).unwrap()
}

struct Running {
    tcp: SocketAddr,
    ws: SocketAddr,
    service: Arc<DeviceService>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<io::Result<()>>>,
    polls: Arc<AtomicU64>,
    gate: Arc<Mutex<Option<Gate>>>,
}
impl Running {
    fn new() -> Self {
        Self::with_config(configuration())
    }
    fn with_config(config: ServerConfig) -> Self {
        let polls = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Mutex::new(None));
        let server = start_with(
            &config,
            |_| {
                Ok(DriverSetup {
                    driver: Box::new(MockDriver::new(polls.clone(), gate.clone())),
                    post_open_delay: Duration::ZERO,
                })
            },
            |_, _| Ok(MemoryIo(Cursor::new(vec![]))),
        )
        .unwrap();
        let tcp = server.local_addr().unwrap();
        let ws = server.websocket_addr().unwrap().unwrap();
        let service = server.backend().clone();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let join = Some(thread::spawn(move || server.run_until(&flag)));
        // Wait on actual cache state: no sleeps or assumptions about initial
        // worker scheduling/publication relative to a client's subscription.
        let deadline = Instant::now() + WAIT;
        let mut probe = lab_server::session::Session::new(64, 4096);
        let mut id = 0;
        for device in &config.devices {
            for parameter in ["sample", "temperature", "flag"] {
                loop {
                    id += 1;
                    service
                        .receive_message(
                            &mut probe,
                            &json!({"id":id,"op":"get","key":format!("{}/{parameter}", device.id)})
                                .to_string(),
                            Instant::now(),
                        )
                        .unwrap();
                    let reply: Json =
                        serde_json::from_str(&probe.next_message().unwrap().unwrap()).unwrap();
                    if reply["status"] == "ok" {
                        break;
                    }
                    assert!(Instant::now() < deadline);
                    thread::yield_now();
                }
            }
        }
        Self {
            tcp,
            ws,
            service,
            stop,
            join,
            polls,
            gate,
        }
    }
    fn connect(&self) -> WebSocket<TcpStream> {
        connect(self.ws, PATH, Some(SUBPROTOCOL), None).unwrap()
    }
    fn tcp(&self) -> BufReader<TcpStream> {
        let stream = TcpStream::connect(self.tcp).unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        stream.set_write_timeout(Some(WAIT)).unwrap();
        BufReader::new(stream)
    }
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            join.join().unwrap().unwrap();
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop();
    }
}

fn connect(
    addr: SocketAddr,
    path: &str,
    protocol: Option<&str>,
    origin: Option<&str>,
) -> Result<WebSocket<TcpStream>, Box<Error>> {
    let stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    stream.set_write_timeout(Some(WAIT)).unwrap();
    let mut request = format!("ws://{addr}{path}").into_client_request().unwrap();
    if let Some(protocol) = protocol {
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(protocol).unwrap(),
        );
    }
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("Origin", HeaderValue::from_str(origin).unwrap());
    }
    match tungstenite::client(request, stream) {
        Ok((socket, response)) => {
            assert_eq!(response.headers()["sec-websocket-protocol"], SUBPROTOCOL);
            Ok(socket)
        }
        Err(HandshakeError::Failure(error)) => Err(Box::new(error)),
        Err(HandshakeError::Interrupted(_)) => panic!("blocking handshake interrupted"),
    }
}
fn receive(client: &mut WebSocket<TcpStream>) -> Json {
    let Message::Text(text) = client.read().unwrap() else {
        panic!("expected JSON text")
    };
    assert!(
        !text.contains('\n'),
        "WS must not contain NDJSON delimiters"
    );
    serde_json::from_str(&text).unwrap()
}
fn request(client: &mut WebSocket<TcpStream>, value: Json) -> Json {
    client
        .send(Message::Text(value.to_string().into()))
        .unwrap();
    receive(client)
}
fn tcp_request(client: &mut BufReader<TcpStream>, value: Json) -> Json {
    writeln!(client.get_mut(), "{value}").unwrap();
    tcp_receive(client)
}
fn tcp_receive(client: &mut BufReader<TcpStream>) -> Json {
    let mut line = String::new();
    client.read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}
fn publish(server: &Running, port: &str, key: &str, value: Value) {
    server
        .service
        .apply_event(AggregatorEvent::Measurement {
            port_id: port.into(),
            global_key: key.into(),
            observation: Observation {
                source: key.split_once('/').unwrap().1.into(),
                unit: match key.split_once('/').unwrap().1 {
                    "flag" => "",
                    "temperature" => "degC",
                    _ => "count",
                }
                .into(),
                completed_at: SystemTime::UNIX_EPOCH + Duration::from_nanos(1_234_567_890),
                reading: Ok(value),
            },
        })
        .unwrap();
}

#[test]
fn handshake_checks_endpoint_subprotocol_and_exact_origin() {
    let server = Running::new();
    for origin in [None, Some("http://localhost:3000")] {
        let mut client = connect(server.ws, PATH, Some(SUBPROTOCOL), origin).unwrap();
        assert_eq!(
            request(&mut client, json!({"id":1,"op":"hello"}))["version"],
            1
        );
    }
    for (path, protocol, origin, status) in [
        ("/wrong", Some(SUBPROTOCOL), None, StatusCode::NOT_FOUND),
        (PATH, None, None, StatusCode::BAD_REQUEST),
        (PATH, Some("wrong"), None, StatusCode::BAD_REQUEST),
        (
            PATH,
            Some(SUBPROTOCOL),
            Some("http://localhost:3001"),
            StatusCode::FORBIDDEN,
        ),
        (
            PATH,
            Some(SUBPROTOCOL),
            Some("http://LOCALHOST:3000"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let error = connect(server.ws, path, protocol, origin).unwrap_err();
        let Error::Http(response) = *error else {
            panic!("expected HTTP rejection: {error}")
        };
        assert_eq!(response.status(), status);
    }
}

#[test]
fn protocol_commands_types_ids_and_json_use_shared_session() {
    let server = Running::new();
    let mut client = server.connect();
    assert_eq!(
        request(&mut client, json!({"id":1,"op":"hello"})),
        json!({"id":1,"status":"ok","protocol":"lab-server","version":1})
    );
    assert_eq!(
        request(&mut client, json!({"id":2,"op":"describe"}))["parameters"]
            .as_array()
            .unwrap()
            .len(),
        12
    );
    let mut id = 3;
    for (key, value) in [
        ("a/count", json!(i64::MIN)),
        ("a/level", json!(-2.5)),
        ("a/enabled", json!(false)),
    ] {
        assert_eq!(
            request(
                &mut client,
                json!({"id":id,"op":"set","key":key,"value":value})
            )["status"],
            "ok"
        );
        id += 1;
        let reply = request(&mut client, json!({"id":id,"op":"get","key":key}));
        assert_eq!(reply["value"], value);
        if key == "a/count" {
            assert_eq!(reply["value"].as_i64(), Some(i64::MIN));
        }
        id += 1;
    }
    client.send(Message::Text("{} {}".into())).unwrap();
    assert_eq!(receive(&mut client)["error"], "invalid_json");
    let first = request(&mut client, json!({"id":id,"op":"ping"}));
    assert_eq!(first["status"], "ok");
    let duplicate = request(&mut client, json!({"id":id,"op":"ping"}));
    assert_eq!(duplicate, json!({"status":"error","error":"duplicate_id"}));
    assert_eq!(
        request(&mut client, json!({"id":id+1,"op":"get","key":"b/count"}))["value"].as_i64(),
        Some(i64::MAX)
    );
}

#[test]
fn fragmented_text_ping_pong_and_close_are_standard_websocket_messages() {
    let server = Running::new();
    let mut client = server.connect();
    client
        .send(Message::Frame(Frame::message(
            b"{\"id\":1,".to_vec(),
            OpCode::Data(Data::Text),
            false,
        )))
        .unwrap();
    client
        .send(Message::Ping(b"probe".to_vec().into()))
        .unwrap();
    assert_eq!(
        client.read().unwrap(),
        Message::Pong(b"probe".to_vec().into())
    );
    client
        .send(Message::Frame(Frame::message(
            b"\"op\":\"ping\"}".to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .unwrap();
    assert_eq!(receive(&mut client), json!({"id":1,"status":"ok"}));
    client.close(None).unwrap();
    assert!(matches!(client.read().unwrap(), Message::Close(_)));
    assert!(matches!(client.read(), Err(Error::ConnectionClosed)));
}

#[test]
fn binary_oversized_frame_and_fragmented_message_are_rejected_without_json() {
    let server = Running::new();
    for (messages, code) in [
        (
            vec![Message::Binary(
                b"{\"id\":1,\"op\":\"ping\"}".to_vec().into(),
            )],
            CloseCode::Unsupported,
        ),
        (
            vec![Message::Text("x".repeat(16385).into())],
            CloseCode::Size,
        ),
        (
            vec![
                Message::Frame(Frame::message(
                    vec![b' '; 9000],
                    OpCode::Data(Data::Text),
                    false,
                )),
                Message::Frame(Frame::message(
                    vec![b' '; 9000],
                    OpCode::Data(Data::Continue),
                    true,
                )),
            ],
            CloseCode::Size,
        ),
    ] {
        let mut client = server.connect();
        for message in messages {
            client.send(message).unwrap();
        }
        let Message::Close(Some(close)) = client.read().unwrap() else {
            panic!("expected transport Close")
        };
        assert_eq!(close.code, code);
    }
}

#[test]
fn exact_message_limit_and_transport_framing_errors_are_distinct() {
    let server = Running::new();
    let mut client = server.connect();
    let mut text = String::from("{\"id\":1,\"op\":\"ping\"}");
    text.push_str(&" ".repeat(16 * 1024 - text.len()));
    client.send(Message::Text(text.into())).unwrap();
    assert_eq!(receive(&mut client)["id"], 1);
    client
        .send(Message::Frame(Frame::message(
            vec![0xff],
            OpCode::Data(Data::Text),
            true,
        )))
        .unwrap();
    let Message::Close(Some(close)) = client.read().unwrap() else {
        panic!("expected UTF-8 Close")
    };
    assert_eq!(close.code, CloseCode::Invalid);
    let mut unmasked = server.connect();
    unmasked.get_mut().write_all(&[0x81, 0]).unwrap();
    let Message::Close(Some(close)) = unmasked.read().unwrap() else {
        panic!("expected unmasked-frame Close")
    };
    assert_eq!(close.code, CloseCode::Protocol);
}

#[test]
fn oversized_upgrade_is_bounded_before_a_session_is_created() {
    let server = Running::new();
    let mut stream = TcpStream::connect(server.ws).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let mut header = b"GET /application/v1 HTTP/1.1\r\nX-Padding: ".to_vec();
    header.extend(vec![b'x'; lab_server::websocket::MAX_UPGRADE_BYTES]);
    stream.write_all(&header).unwrap();
    let result = stream.read(&mut [0]);
    assert!(
        matches!(result, Ok(0))
            || result.is_err_and(|e| e.kind() == io::ErrorKind::ConnectionReset)
    );
    let mut client = server.connect();
    assert_eq!(
        request(&mut client, json!({"id":1,"op":"ping"}))["status"],
        "ok"
    );
}

#[test]
fn concurrent_tcp_ws_subscriptions_match_exactly_and_unsubscribe_keeps_other_clients() {
    let server = Running::new();
    let mut first = server.connect();
    let mut second = server.connect();
    let mut tcp = server.tcp();
    let subscription =
        json!({"id":1,"op":"subscribe","keys":["a/flag", "a/sample", "a/temperature"]});
    publish(&server, "p", "a/sample", Value::Integer(40)); // pre-activation boundary
    assert_eq!(
        request(&mut first, subscription.clone())["keys"],
        json!(["a/flag", "a/sample", "a/temperature"])
    );
    assert_eq!(
        request(&mut second, subscription.clone())["keys"],
        json!(["a/flag", "a/sample", "a/temperature"])
    );
    assert_eq!(
        tcp_request(&mut tcp, subscription)["keys"],
        json!(["a/flag", "a/sample", "a/temperature"])
    );
    for (key, value) in [
        ("a/sample", Value::Integer(i64::MAX)),
        ("a/temperature", Value::Float(1.25)),
        ("a/flag", Value::Boolean(true)),
    ] {
        publish(&server, "p", key, value);
        let expected = tcp_receive(&mut tcp);
        assert_eq!(receive(&mut first), expected);
        assert_eq!(receive(&mut second), expected);
    }
    assert_eq!(
        request(
            &mut first,
            json!({"id":2,"op":"unsubscribe","keys":["a/flag", "a/sample", "a/temperature"]})
        )["keys"],
        json!([])
    );
    publish(&server, "p", "a/sample", Value::Integer(42));
    assert_eq!(receive(&mut second), tcp_receive(&mut tcp));
    assert_eq!(
        request(&mut first, json!({"id":3,"op":"ping"})),
        json!({"id":3,"status":"ok"})
    );
}

#[test]
fn concurrent_port_loss_and_close_are_isolated_between_ws_subscribers() {
    let server = Running::new();
    let mut a = server.connect();
    let mut b = server.connect();
    request(&mut a, json!({"id":1,"op":"subscribe","keys":["a/sample"]}));
    request(&mut b, json!({"id":1,"op":"subscribe","keys":["b/sample"]}));
    server
        .service
        .apply_event(AggregatorEvent::DataLost {
            port_id: "p".into(),
            loss_epoch: 1,
        })
        .unwrap();
    assert_eq!(receive(&mut a)["error"], "observation_data_lost");
    publish(&server, "q", "b/sample", Value::Integer(9));
    assert_eq!(receive(&mut b)["value"], 9);
    server
        .service
        .apply_event(AggregatorEvent::PortStopped {
            port_id: "q".into(),
            reason: StopReason::Shutdown,
        })
        .unwrap();
    assert_eq!(receive(&mut b)["error"], "observation_stream_closed");
}

#[test]
fn concurrent_pending_set_has_one_final_ack_after_independent_ping() {
    let server = Running::new();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *server.gate.lock().unwrap() = Some(Gate {
        entered: entered_tx,
        resume: resume_rx,
    });
    let mut client = server.connect();
    client
        .send(Message::Text(
            json!({"id":1,"op":"set","key":"a/count","value":13})
                .to_string()
                .into(),
        ))
        .unwrap();
    entered_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(
        request(&mut client, json!({"id":2,"op":"ping"})),
        json!({"id":2,"status":"ok"})
    );
    resume_tx.send(()).unwrap();
    assert_eq!(receive(&mut client), json!({"id":1,"status":"ok"}));
    assert_eq!(
        request(&mut client, json!({"id":3,"op":"ping"})),
        json!({"id":3,"status":"ok"})
    );
}

#[test]
fn websocket_started_set_timeout_stays_outcome_unknown_without_late_reply() {
    let server = Running::new();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    *server.gate.lock().unwrap() = Some(Gate {
        entered: entered_tx,
        resume: resume_rx,
    });
    let mut client = server.connect();
    client
        .send(Message::Text(
            json!({"id":1,"op":"set","key":"a/count","value":13})
                .to_string()
                .into(),
        ))
        .unwrap();
    entered_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(
        receive(&mut client),
        json!({"id":1,"status":"error","error":"outcome_unknown"})
    );
    resume_tx.send(()).unwrap();
    // An on-demand get waits behind the released write and observes its value.
    assert_eq!(
        request(&mut client, json!({"id":2,"op":"get","key":"a/count"}))["value"],
        13
    );
    assert_eq!(
        request(&mut client, json!({"id":3,"op":"ping"})),
        json!({"id":3,"status":"ok"})
    );
}

#[test]
fn concurrent_shared_limit_counts_tcp_ws_and_releases_disconnected_slots() {
    let server = Running::new();
    let mut tcp_clients = Vec::new();
    for _ in 0..20 {
        let mut client = server.tcp();
        assert_eq!(
            tcp_request(&mut client, json!({"id":1,"op":"hello"}))["version"],
            1
        );
        tcp_clients.push(client);
    }
    let mut ws_clients: Vec<_> = (0..12).map(|_| server.connect()).collect();
    assert!(connect(server.ws, PATH, Some(SUBPROTOCOL), None).is_err());
    let mut leaving = tcp_clients.pop().unwrap();
    leaving.get_mut().shutdown(Shutdown::Write).unwrap();
    // EOF confirms the server has released its socket and permit, not a delay.
    assert_eq!(leaving.read_line(&mut String::new()).unwrap(), 0);
    ws_clients.push(server.connect());
    assert_eq!(
        request(ws_clients.last_mut().unwrap(), json!({"id":1,"op":"ping"}))["status"],
        "ok"
    );
    assert!(connect(server.ws, PATH, Some(SUBPROTOCOL), None).is_err());
}

#[test]
fn failed_handshakes_release_slots_and_upgrade_has_a_deadline() {
    let server = Running::new();
    for _ in 0..40 {
        assert!(connect(server.ws, "/wrong", Some(SUBPROTOCOL), None).is_err());
    }
    let mut idle = TcpStream::connect(server.ws).unwrap();
    idle.set_read_timeout(Some(WAIT)).unwrap();
    idle.write_all(b"GET /application/v1 HTTP/1.1\r\n").unwrap();
    let began = Instant::now();
    assert_eq!(idle.read(&mut [0]).unwrap(), 0);
    assert!(began.elapsed() < lab_server::websocket::HANDSHAKE_TIMEOUT + Duration::from_secs(2));
    let mut client = server.connect();
    assert_eq!(
        request(&mut client, json!({"id":1,"op":"ping"}))["status"],
        "ok"
    );
}

#[test]
fn concurrent_shutdown_bounds_idle_ws_and_unfinished_upgrades_and_releases_listeners() {
    let mut server = Running::new();
    let mut tcp = server.tcp();
    tcp_request(&mut tcp, json!({"id":1,"op":"hello"}));
    let mut ws = server.connect();
    request(&mut ws, json!({"id":1,"op":"hello"}));
    let mut upgrades = Vec::new();
    for _ in 0..30 {
        let mut stream = TcpStream::connect(server.ws).unwrap();
        stream
            .write_all(b"GET /application/v1 HTTP/1.1\r\n")
            .unwrap();
        upgrades.push(stream);
    }
    // All 32 slots are occupied; no extra session can be admitted.
    assert!(connect(server.ws, PATH, Some(SUBPROTOCOL), None).is_err());
    let began = Instant::now();
    server.stop(); // WS peer deliberately does not read/respond during shutdown
    assert!(began.elapsed() < lab_server::websocket::CLOSE_TIMEOUT + Duration::from_secs(2));
    let Message::Close(Some(close)) = ws.read().unwrap() else {
        panic!("expected server Close")
    };
    assert_eq!(close.code, CloseCode::Away);
    assert_eq!(tcp.read_line(&mut String::new()).unwrap(), 0);
    TcpListener::bind(server.tcp).unwrap();
    TcpListener::bind(server.ws).unwrap();
}

#[test]
fn websocket_bind_failure_happens_before_opening_mock_equipment() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = configuration();
    config.websocket.as_mut().unwrap().bind = Some(occupied.local_addr().unwrap());
    let polls = Arc::new(AtomicU64::new(0));
    let error = start_with(
        &config,
        |_| {
            Ok(DriverSetup {
                driver: Box::new(MockDriver::new(polls.clone(), Arc::new(Mutex::new(None)))),
                post_open_delay: Duration::ZERO,
            })
        },
        |_, _| -> io::Result<MemoryIo> { panic!("equipment opened before WS bind") },
    )
    .err()
    .unwrap();
    assert!(error.downcast_ref::<io::Error>().is_some());
}

#[test]
fn concurrent_mock_polling_reaches_tcp_ws_and_sqlite_identically() {
    let database = db::Database::new();
    let mut config = configuration();
    config.recording = Some(lab_server::config::RecordingConfig {
        enabled: true,
        path: Some(database.0.clone()),
        queue_capacity: Some(1024),
    });
    let mut keys = Vec::new();
    for device in &mut config.devices {
        for parameter in ["sample", "temperature", "flag"] {
            device.poll_periods_ms.insert(parameter.into(), 20);
            keys.push(format!("{}/{parameter}", device.id));
        }
    }
    let mut server = Running::with_config(config);
    let mut ws = server.connect();
    let mut tcp = server.tcp();
    let subscribe = json!({"id":1,"op":"subscribe","keys":keys});
    assert_eq!(request(&mut ws, subscribe.clone())["status"], "ok");
    assert_eq!(tcp_request(&mut tcp, subscribe)["status"], "ok");
    // Compare an interval strictly after BOTH activation ACKs. This is an
    // observation boundary, not a sleep or a scheduling assumption.
    let cutoff = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1;
    let mut ws_events = Vec::new();
    let mut tcp_events = Vec::new();
    while ws_events.len() < 18 {
        let event = receive(&mut ws);
        assert_eq!(event["event"], "measurement");
        if event["completed_at_unix_ms"].as_u64().unwrap() >= cutoff {
            ws_events.push(event);
        }
    }
    while tcp_events.len() < 18 {
        let event = tcp_receive(&mut tcp);
        assert_eq!(event["event"], "measurement");
        if event["completed_at_unix_ms"].as_u64().unwrap() >= cutoff {
            tcp_events.push(event);
        }
    }
    assert_eq!(ws_events, tcp_events); // includes FIFO, values, units, keys and times
    drop(ws);
    drop(tcp);
    server.stop();
    let sql = database.connect();
    for event in ws_events {
        let key = event["key"].as_str().unwrap();
        let time = event["completed_at_unix_ms"].as_i64().unwrap();
        let mut rows = sql.prepare("SELECT value_type, integer_value, real_value, boolean_value, unit FROM measurements WHERE global_key=? AND completed_at_ns/1000000=?").unwrap();
        let values: Vec<Json> = rows
            .query_map(rusqlite::params![key, time], |row| {
                let kind: String = row.get(0)?;
                let value = match kind.as_str() {
                    "integer" => json!(row.get::<_, i64>(1)?),
                    "float" => json!(row.get::<_, f64>(2)?),
                    "boolean" => json!(row.get::<_, bool>(3)?),
                    other => panic!("unexpected stored type {other}"),
                };
                assert_eq!(
                    kind,
                    match key.split_once('/').unwrap().1 {
                        "sample" => "integer",
                        "temperature" => "float",
                        "flag" => "boolean",
                        _ => unreachable!(),
                    }
                );
                assert_eq!(row.get::<_, String>(4)?, event["unit"].as_str().unwrap());
                Ok(value)
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(values, vec![event["value"].clone()]); // exactly once in SQLite
    }
    assert_eq!(
        db::scalar(&sql, "SELECT recorder_incomplete FROM sessions"),
        0
    );
    assert_eq!(
        db::scalar(&sql, "SELECT COUNT(*) FROM measurements") as u64,
        server.polls.load(Ordering::SeqCst)
    );
}

#[test]
fn concurrent_sqlite_records_without_tcp_or_ws_subscribers_and_drains_on_stop() {
    let database = db::Database::new();
    let mut config = configuration();
    config.recording = Some(lab_server::config::RecordingConfig {
        enabled: true,
        path: Some(database.0.clone()),
        queue_capacity: Some(1024),
    });
    for device in &mut config.devices {
        device.poll_periods_ms.insert("sample".into(), 5);
    }
    let mut server = Running::with_config(config);
    // Connected but not subscribed. Do not answer the server's Close, forcing
    // the full grace interval while the two real mock workers keep polling.
    let mut idle_ws = server.connect();
    assert_eq!(
        request(&mut idle_ws, json!({"id":1,"op":"hello"}))["version"],
        1
    );
    let deadline = Instant::now() + WAIT;
    while server.polls.load(Ordering::SeqCst) < 12 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    server.stop();
    assert!(matches!(idle_ws.read().unwrap(), Message::Close(_)));
    let sql = database.connect();
    assert_eq!(
        db::scalar(&sql, "SELECT COUNT(*) FROM measurements") as u64,
        server.polls.load(Ordering::SeqCst)
    );
    assert_eq!(
        db::scalar(&sql, "SELECT COUNT(DISTINCT global_key) FROM measurements"),
        6
    );
    assert_eq!(
        db::scalar(
            &sql,
            "SELECT COUNT(*) FROM sessions WHERE ended_at_ns IS NOT NULL AND recorder_incomplete=0 AND recorder_dropped_events='0'"
        ),
        1
    );
    assert_eq!(
        db::scalar(
            &sql,
            "SELECT COUNT(*) FROM events WHERE event_type='PortStopped'"
        ),
        2
    );
    assert_eq!(
        db::scalar(
            &sql,
            "SELECT COUNT(*) FROM events WHERE event_type IN ('DataLost','RecorderDataLost')"
        ),
        0
    );
    assert_eq!(
        sql.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}
