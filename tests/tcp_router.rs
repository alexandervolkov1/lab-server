use std::collections::VecDeque;
use std::io::{self, BufRead, Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use lab_server::command_router::CommandRouter;
use lab_server::connection::TcpConnection;
use lab_server::device_registry::DeviceRegistry;
use lab_server::driver::{DeviceIo, Driver, DriverError, PortState};
use lab_server::fanout::ObservationReceiver;
use lab_server::model::{Observation, ParameterDescription, Spec, Value};
use lab_server::observation_aggregator::{AggregatorEvent, MappingError, ObservationAggregator};
use lab_server::port_worker::{
    DeviceInstance, DeviceObservation, PortHandle, PortWorker, StopReason,
};
use lab_server::tcp::MultiDeviceTcp;
use serde_json::{Value as Json, json};

const WAIT: Duration = Duration::from_secs(10);
type Calls = Arc<Mutex<Vec<String>>>;

struct FakeIo {
    input: Cursor<Vec<u8>>,
    calls: Calls,
}
impl Read for FakeIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.input.read(buf)
    }
}
impl BufRead for FakeIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.input.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.input.consume(n);
    }
}
impl DeviceIo for FakeIo {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        assert!(
            self.input.fill_buf()?.is_empty(),
            "transactions interleaved"
        );
        self.calls
            .lock()
            .unwrap()
            .push(String::from_utf8(request.to_vec()).unwrap());
        self.input = Cursor::new(b"ok\n".to_vec());
        Ok(())
    }
}

struct Gate {
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
fn gate() -> (Gate, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, calls) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    (Gate { entered, resume }, calls, release)
}
struct MockDriver<const KIND: u8> {
    integer: i64,
    level: f64,
    enabled: bool,
    gate: Option<Gate>,
    failure: Option<DriverError>,
}
impl<const KIND: u8> MockDriver<KIND> {
    fn new() -> Self {
        Self {
            integer: 5,
            level: 1.25,
            enabled: false,
            gate: None,
            failure: None,
        }
    }
    fn transaction(
        &mut self,
        key: &str,
        write: bool,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        io.send(format!("{KIND}:{key}:{write}").as_bytes())?;
        if let Some(gate) = &self.gate {
            gate.entered.send(()).unwrap();
            gate.resume.recv_timeout(WAIT).unwrap();
        }
        let mut reply = String::new();
        io.read_line(&mut reply)?;
        assert_eq!(reply, "ok\n");
        self.failure.take().map_or(Ok(()), Err)
    }
}
impl<const KIND: u8> Driver for MockDriver<KIND> {
    fn parameters(&self) -> Vec<ParameterDescription> {
        [
            (
                "sample",
                Spec::Integer {
                    min: i64::MIN,
                    max: i64::MAX,
                },
                "count",
                false,
                Some(Duration::from_secs(3600)),
            ),
            (
                "nested/value",
                Spec::Integer {
                    min: i64::MIN,
                    max: i64::MAX,
                },
                "count",
                true,
                None,
            ),
            (
                "level",
                Spec::Float {
                    min: -5.0,
                    max: 5.0,
                },
                "V",
                true,
                None,
            ),
            ("enabled", Spec::Boolean, "", true, None),
        ]
        .into_iter()
        .map(
            |(id, spec, unit, writable, default_poll_period)| ParameterDescription {
                id: id.into(),
                spec,
                unit: unit.into(),
                writable,
                default_poll_period,
            },
        )
        .collect()
    }
    fn read(&mut self, key: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.transaction(key, false, io)?;
        Ok(match key {
            "sample" | "nested/value" => Value::Integer(self.integer),
            "level" => Value::Float(self.level),
            "enabled" => Value::Boolean(self.enabled),
            _ => panic!("global name reached driver: {key}"),
        })
    }
    fn write(
        &mut self,
        key: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        self.transaction(key, true, io)?;
        match (key, value) {
            ("nested/value", Value::Integer(value)) => self.integer = *value,
            ("level", Value::Float(value)) => self.level = *value,
            ("enabled", Value::Boolean(value)) => self.enabled = *value,
            _ => panic!("invalid write"),
        }
        Ok(())
    }
}
fn device(id: &str) -> DeviceInstance {
    DeviceInstance::new(id, MockDriver::<1>::new()).unwrap()
}

struct Port {
    id: String,
    handle: PortHandle,
    source: Option<mpsc::Receiver<DeviceObservation>>,
    join: Option<JoinHandle<StopReason>>,
    calls: Calls,
}
impl Port {
    fn source(&mut self) -> (String, mpsc::Receiver<DeviceObservation>) {
        (self.id.clone(), self.source.take().unwrap())
    }
    fn stop(&mut self) -> StopReason {
        self.handle.shutdown();
        self.join.take().unwrap().join().unwrap()
    }
}
impl Drop for Port {
    fn drop(&mut self) {
        self.handle.shutdown();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
fn port(
    registry: &mut DeviceRegistry,
    id: &str,
    devices: Vec<DeviceInstance>,
    command_capacity: usize,
    observation_capacity: usize,
) -> Port {
    let calls = Calls::default();
    let io = FakeIo {
        input: Cursor::new(vec![]),
        calls: calls.clone(),
    };
    let (worker, channels) =
        PortWorker::new(io, devices, command_capacity, observation_capacity).unwrap();
    registry
        .register_port(id, channels.commands.clone())
        .unwrap();
    for device in worker.devices() {
        registry.register_device(id, device).unwrap();
    }
    Port {
        id: id.into(),
        handle: channels.commands,
        source: Some(channels.observations),
        join: Some(thread::spawn(move || worker.run())),
        calls,
    }
}

// Each client connects to the same real loopback endpoint. The test drives the
// existing nonblocking server loop explicitly, making publication/ACK races
// reproducible without depending on socket buffer sizes or thread timing.
struct Client {
    connection: TcpConnection,
    input_probe: TcpStream,
    wire: TcpStream,
    observations: ObservationReceiver,
    buffer: Vec<u8>,
    messages: VecDeque<Json>,
    id: u64,
}
impl Client {
    fn connect(listener: &TcpListener, server: &MultiDeviceTcp) -> Self {
        let wire = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        wire.set_nodelay(true).unwrap();
        wire.set_nonblocking(true).unwrap();
        let (stream, _) = listener.accept().unwrap();
        stream.set_nodelay(true).unwrap();
        let input_probe = stream.try_clone().unwrap();
        Self {
            connection: TcpConnection::new(stream, 16, 64 * 1024).unwrap(),
            input_probe,
            wire,
            observations: server.register_client().unwrap(),
            buffer: vec![],
            messages: VecDeque::new(),
            id: 0,
        }
    }
    fn send(&mut self, mut request: Json) -> u64 {
        self.id += 1;
        request["id"] = json!(self.id);
        self.wire
            .write_all(format!("{request}\n").as_bytes())
            .unwrap();
        self.id
    }
    fn wait_input(&self) {
        let end = Instant::now() + WAIT;
        let mut bytes = [0; 4096];
        loop {
            match self.input_probe.peek(&mut bytes) {
                Ok(n) if bytes[..n].contains(&b'\n') => return,
                Ok(0) => panic!("socket closed before request arrived"),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("peek failed: {e}"),
            }
            assert!(Instant::now() < end, "request did not reach server socket");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn step(&mut self, server: &MultiDeviceTcp) {
        server
            .drive_connection(&mut self.connection, &self.observations)
            .unwrap();
        let mut bytes = [0; 8192];
        loop {
            match self.wire.read(&mut bytes) {
                Ok(0) => break,
                Ok(n) => self.buffer.extend_from_slice(&bytes[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("wire read: {e}"),
            }
        }
        while let Some(end) = self.buffer.iter().position(|b| *b == b'\n') {
            let line: Vec<_> = self.buffer.drain(..=end).collect();
            self.messages
                .push_back(serde_json::from_slice(&line).unwrap());
        }
    }
    fn next(&mut self, server: &MultiDeviceTcp) -> Json {
        let end = Instant::now() + WAIT;
        loop {
            self.step(server);
            if let Some(message) = self.messages.pop_front() {
                return message;
            }
            assert!(Instant::now() < end, "TCP message timed out");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn request(&mut self, server: &MultiDeviceTcp, request: Json) -> Json {
        let id = self.send(request);
        let reply = self.next(server);
        assert_eq!(reply["id"], id, "{reply}");
        reply
    }
    fn admitted(&mut self, server: &MultiDeviceTcp) {
        let limit = Instant::now() + WAIT;
        while self.connection.session.pending.is_empty() {
            self.step(server);
            assert!(Instant::now() < limit);
            thread::yield_now();
        }
    }
}
fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").unwrap()
}
fn publish(server: &MultiDeviceTcp, port: &str, key: &str, reading: Result<Value, String>) {
    server
        .apply_event(AggregatorEvent::Measurement {
            port_id: port.into(),
            global_key: key.into(),
            observation: Observation {
                source: key.split_once('/').unwrap().1.into(),
                unit: "count".into(),
                completed_at: SystemTime::UNIX_EPOCH + Duration::from_millis(1234),
                reading,
            },
        })
        .unwrap();
}

#[test]
fn message_and_tcp_sessions_share_cache_fanout_and_port_loss_isolation() {
    use lab_server::service::DeviceService;
    use lab_server::session::Session;

    fn take(session: &mut Session) -> Json {
        serde_json::from_str(&session.next_message().unwrap().unwrap()).unwrap()
    }
    fn send(service: &DeviceService, session: &mut Session, request: Json) {
        service
            .receive_message(session, &request.to_string(), Instant::now())
            .unwrap();
    }

    let mut registry = DeviceRegistry::new();
    let _p = port(&mut registry, "p", vec![device("a")], 8, 8);
    let _q = port(&mut registry, "q", vec![device("b")], 8, 8);
    let service = DeviceService::new(CommandRouter::new(registry), 32);
    let a_receiver = service.register_client().unwrap();
    let b_receiver = service.register_client().unwrap();
    let mut a = Session::new(64, 64 * 1024);
    let mut b = Session::new(64, 64 * 1024);
    a.bind_observations(&a_receiver);
    b.bind_observations(&b_receiver);
    let endpoint = listener();
    let mut tcp_b = Client::connect(&endpoint, &service);
    tcp_b.request(&service, json!({"op":"subscribe","keys":["b/sample"]}));
    send(
        &service,
        &mut a,
        json!({"id":1,"op":"subscribe","keys":["a/sample"]}),
    );
    send(
        &service,
        &mut b,
        json!({"id":1,"op":"subscribe","keys":["b/sample"]}),
    );
    publish(&service, "p", "a/sample", Ok(Value::Integer(i64::MAX)));
    publish(&service, "q", "b/sample", Ok(Value::Integer(i64::MIN)));
    a.collect_observations(&a_receiver).unwrap();
    b.collect_observations(&b_receiver).unwrap();
    assert_eq!(take(&mut a)["keys"], json!(["a/sample"]));
    assert_eq!(take(&mut a)["value"].as_i64(), Some(i64::MAX));
    assert_eq!(take(&mut b)["keys"], json!(["b/sample"]));
    let event = take(&mut b);
    assert_eq!(event["value"].as_i64(), Some(i64::MIN));
    assert_eq!(event, tcp_b.next(&service));
    send(
        &service,
        &mut b,
        json!({"id":2,"op":"get","key":"b/sample"}),
    );
    assert_eq!(take(&mut b)["value"].as_i64(), Some(i64::MIN));

    service
        .apply_event(AggregatorEvent::DataLost {
            port_id: "p".into(),
            loss_epoch: 1,
        })
        .unwrap();
    send(
        &service,
        &mut a,
        json!({"id":2,"op":"unsubscribe","keys":["a/sample"]}),
    );
    a.collect_observations(&a_receiver).unwrap();
    assert_eq!(take(&mut a)["keys"], json!([]));
    assert_eq!(take(&mut a)["error"], "observation_data_lost");
    assert!(a.is_finished());
    publish(&service, "q", "b/sample", Ok(Value::Integer(7)));
    b.collect_observations(&b_receiver).unwrap();
    assert_eq!(take(&mut b), tcp_b.next(&service));
    assert!(!b.is_terminal());

    service
        .apply_event(AggregatorEvent::PortStopped {
            port_id: "p".into(),
            reason: StopReason::Shutdown,
        })
        .unwrap();
    let mut commands = Session::new(64, 4096);
    send(
        &service,
        &mut commands,
        json!({"id":1,"op":"get","key":"a/sample"}),
    );
    assert_eq!(take(&mut commands)["error"], "runtime_unavailable");
    send(
        &service,
        &mut commands,
        json!({"id":2,"op":"get","key":"b/sample"}),
    );
    assert_eq!(take(&mut commands)["value"], 7);
}

#[test]
fn endpoint_routes_shared_and_independent_ports_and_preserves_catalog_and_types() {
    let mut registry = DeviceRegistry::new();
    let mut p = port(
        &mut registry,
        "p",
        vec![
            device("a"),
            DeviceInstance::new("b", MockDriver::<2>::new()).unwrap(),
        ],
        8,
        8,
    );
    let mut q = port(&mut registry, "q", vec![device("c")], 8, 8);
    registry
        .register_unavailable_port("offline", "COM9 access denied")
        .unwrap();
    registry.register_device("offline", &device("d")).unwrap();
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 32);
    let mut aggregator =
        ObservationAggregator::new(server.registry(), [p.source(), q.source()]).unwrap();
    let endpoint = listener();
    let mut client = Client::connect(&endpoint, &server);
    assert_eq!(client.request(&server, json!({"op":"hello"}))["version"], 1);
    let catalog = client.request(&server, json!({"op":"describe"}));
    let parameters = catalog["parameters"].as_array().unwrap();
    assert_eq!(parameters.len(), 16);
    let lookup = |key| parameters.iter().find(|p| p["key"] == key).unwrap();
    assert_eq!(lookup("a/nested/value")["max"].as_i64(), Some(i64::MAX));
    assert_eq!(lookup("a/nested/value")["min"].as_i64(), Some(i64::MIN));
    assert_eq!(lookup("a/level")["unit"], "V");
    assert_eq!(lookup("a/enabled")["type"], "boolean");
    assert!(lookup("a/enabled")["min"].is_null());
    assert!(lookup("a/enabled")["max"].is_null());
    assert_eq!(lookup("d/sample")["poll_period_ns"], 3_600_000_000_000_u64);
    assert_eq!(
        client.request(&server, json!({"op":"get", "key":"a/sample"}))["status"],
        "no_data"
    );
    for (key, value) in [
        ("a/nested/value", json!(i64::MAX)),
        ("b/nested/value", json!(i64::MIN)),
        ("c/level", json!(-2.5)),
        ("a/enabled", json!(true)),
        ("b/enabled", json!(false)),
    ] {
        assert_eq!(
            client.request(&server, json!({"op":"set", "key":key, "value":value}))["status"],
            "ok"
        );
        assert_eq!(
            client.request(&server, json!({"op":"get", "key":key}))["value"],
            value
        );
    }
    for op in ["get", "set"] {
        assert_eq!(
            client.request(&server, json!({"op":op, "key":"d/enabled", "value":true}))["error"],
            "runtime_unavailable"
        );
        assert_eq!(
            client.request(&server, json!({"op":op, "key":"nested/value", "value":1}))["error"],
            "unknown_key"
        );
    }
    for value in [json!(1), json!(0), json!("true")] {
        assert_eq!(
            client.request(
                &server,
                json!({"op":"set", "key":"a/enabled", "value":value})
            )["status"],
            "error"
        );
    }
    assert_eq!(
        client.request(&server, json!({"op":"set", "key":"a/level", "value":6.0}))["error"],
        "Value does not match parameter spec"
    );
    assert_eq!(
        client.request(&server, json!({"op":"set", "key":"a/sample", "value":1}))["error"],
        "Parameter is read-only"
    );
    assert_eq!(
        client.request(&server, json!({"op":"get", "key":"c/nested/value"}))["value"],
        5
    );
    // Aggregate actual worker polling, then use the unchanged periodic cache path.
    let limit = Instant::now() + WAIT;
    loop {
        server.collect(&mut aggregator, 8).unwrap();
        let cached = client.request(&server, json!({"op":"get", "key":"a/sample"}));
        if cached["status"] == "ok" {
            break;
        }
        assert!(Instant::now() < limit);
    }
    p.stop(); // do not pump the aggregator: status must invalidate cached get now
    assert_eq!(
        client.request(&server, json!({"op":"get", "key":"a/sample"}))["error"],
        "runtime_unavailable"
    );
    assert_eq!(
        client.request(&server, json!({"op":"get", "key":"c/enabled"}))["value"],
        false
    );
    assert!(
        p.calls
            .lock()
            .unwrap()
            .iter()
            .all(|s| !s.contains("a/") && !s.contains("b/"))
    );
}

#[test]
fn concurrent_subscriptions_filter_before_queue_and_ack_precedes_global_measurements() {
    let mut registry = DeviceRegistry::new();
    let _p = port(&mut registry, "p", vec![device("a"), device("same")], 8, 8);
    let _q = port(&mut registry, "q", vec![device("b")], 8, 8);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 2);
    let endpoint = listener();
    let mut a = Client::connect(&endpoint, &server);
    let mut b = Client::connect(&endpoint, &server);
    let mut both = Client::connect(&endpoint, &server);
    a.request(&server, json!({"op":"subscribe", "keys":["a/sample"]}));
    b.request(&server, json!({"op":"subscribe", "keys":["b/sample"]}));
    both.request(
        &server,
        json!({"op":"subscribe", "keys":["a/sample", "b/sample"]}),
    );
    for n in 0..5 {
        publish(&server, "p", "a/sample", Ok(Value::Integer(n)));
    }
    // Neither port B nor another key on the same port can fill B's queue.
    publish(&server, "p", "same/sample", Ok(Value::Integer(9)));
    publish(&server, "q", "b/sample", Ok(Value::Integer(17)));
    let message = b.next(&server);
    assert_eq!(message["key"], "b/sample");
    assert_eq!(message["value"], 17);
    assert_eq!(message["unit"], "count");
    assert_eq!(message["completed_at_unix_ms"], 1234);
    assert_eq!(b.request(&server, json!({"op":"ping"}))["status"], "ok");
    for client in [&mut a, &mut both] {
        assert_eq!(client.next(&server)["value"], 0);
        assert_eq!(client.next(&server)["value"], 1);
        assert_eq!(client.next(&server)["error"], "observation_data_lost");
        assert!(client.connection.session.is_terminal());
    }
    let mut fresh = Client::connect(&endpoint, &server);
    let id = fresh.send(json!({"op":"subscribe", "keys":["b/sample"]}));
    // Drive the real command to activation, retaining ACK bytes at the client.
    while fresh.connection.session.subscriptions.is_empty() {
        fresh.step(&server);
    }
    publish(&server, "q", "b/sample", Err("complete read error".into()));
    assert_eq!(fresh.next(&server)["id"], id);
    let measurement = fresh.next(&server);
    assert_eq!(measurement["event"], "measurement");
    assert_eq!(measurement["error"], "complete read error");
    assert_eq!(measurement["completed_at_unix_ms"], 1234);
    fresh.request(&server, json!({"op":"unsubscribe", "keys":["b/sample"]}));
    publish(&server, "q", "b/sample", Ok(Value::Integer(99)));
    assert_eq!(fresh.request(&server, json!({"op":"ping"}))["status"], "ok");
}

#[test]
fn concurrent_port_loss_boundaries_and_delayed_aggregation_are_isolated_over_tcp() {
    let (gate, entered, release) = gate();
    let mut driver = MockDriver::<1>::new();
    driver.gate = Some(gate);
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("sample", Some(Duration::from_nanos(1)))
        .unwrap();
    let mut registry = DeviceRegistry::new();
    let mut p = port(&mut registry, "p", vec![a], 8, 1);
    let _q = port(&mut registry, "q", vec![device("b")], 8, 8);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 32);
    let mut aggregator = ObservationAggregator::new(server.registry(), [p.source()]).unwrap();
    let endpoint = listener();
    let mut only_a = Client::connect(&endpoint, &server);
    let mut only_b = Client::connect(&endpoint, &server);
    let mut both = Client::connect(&endpoint, &server);
    let mut commands = Client::connect(&endpoint, &server);
    entered.recv_timeout(WAIT).unwrap();
    for _ in 0..2 {
        release.send(()).unwrap();
        entered.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(p.handle.loss_epoch(), 1); // loss BEFORE first subscription
    for (client, keys) in [
        (&mut only_a, json!(["a/sample"])),
        (&mut only_b, json!(["b/sample"])),
        (&mut both, json!(["a/sample", "b/sample"])),
    ] {
        assert_eq!(
            client.request(&server, json!({"op":"subscribe", "keys":keys}))["status"],
            "ok"
        );
    }
    // Subscription transitions complete while the driver's physical I/O is held.
    server.collect(&mut aggregator, 1).unwrap();
    assert_eq!(only_a.next(&server)["key"], "a/sample");
    assert_eq!(both.next(&server)["key"], "a/sample");
    assert_eq!(
        only_b.request(&server, json!({"op":"ping"}))["status"],
        "ok"
    );
    for _ in 0..2 {
        release.send(()).unwrap();
        entered.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(p.handle.loss_epoch(), 2); // loss DURING active subscriptions
    // No aggregator turn: unsubscribe itself must capture the physical loss.
    only_a.send(json!({"op":"unsubscribe", "keys":["a/sample"]}));
    only_a.wait_input(); // don't race socket arrival against an already fatal stream
    assert_eq!(only_a.next(&server)["status"], "ok");
    assert_eq!(only_a.next(&server)["error"], "observation_data_lost");
    assert!(only_a.connection.session.subscriptions.is_empty());
    assert_eq!(both.next(&server)["error"], "observation_data_lost");
    assert_eq!(
        only_b.request(&server, json!({"op":"ping"}))["status"],
        "ok"
    );
    assert_eq!(
        commands.request(&server, json!({"op":"get", "key":"b/nested/value"}))["value"],
        5
    );
    // An old aggregator notification cannot poison the new baseline.
    let mut late = Client::connect(&endpoint, &server);
    late.request(&server, json!({"op":"subscribe", "keys":["a/sample"]}));
    server.collect(&mut aggregator, 1).unwrap();
    assert_eq!(late.next(&server)["event"], "measurement");
    p.handle.shutdown();
    release.send(()).unwrap();
    assert_eq!(p.stop(), StopReason::Shutdown);
    server.collect(&mut aggregator, 1).unwrap();
    server.collect(&mut aggregator, 1).unwrap();
    assert_eq!(late.next(&server)["event"], "measurement");
    assert_eq!(late.next(&server)["error"], "observation_stream_closed");
    assert_eq!(
        only_b.request(&server, json!({"op":"ping"}))["status"],
        "ok"
    );
}

#[test]
fn concurrent_queued_set_timeout_busy_queue_and_async_ids_preserve_safety() {
    let (gate, entered, release) = gate();
    let mut driver = MockDriver::<1>::new();
    driver.gate = Some(gate);
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("sample", None).unwrap();
    let mut registry = DeviceRegistry::new();
    let p = port(&mut registry, "p", vec![a], 1, 1);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
    let endpoint = listener();
    let mut client = Client::connect(&endpoint, &server);
    let read_id = client.send(json!({"op":"get", "key":"a/enabled"}));
    client.admitted(&server);
    entered.recv_timeout(WAIT).unwrap();
    let set_id = client.send(json!({"op":"set", "key":"a/enabled", "value":true}));
    let ping_id = client.send(json!({"op":"ping"}));
    assert_eq!(client.next(&server)["id"], ping_id); // earlier operations remain pending
    assert_eq!(
        client.request(
            &server,
            json!({"op":"set", "key":"a/enabled", "value":false})
        )["error"],
        "runtime_busy"
    );
    // Exercise the real Pending cancellation with a deterministic clock advance.
    client
        .connection
        .session
        .collect_ready(Instant::now() + Duration::from_secs(20))
        .unwrap();
    for id in [read_id, set_id] {
        let reply = client.next(&server);
        assert_eq!(reply["id"], id);
        assert_eq!(reply["error"], "runtime_timeout");
    }
    release.send(()).unwrap();
    // Cancellation forbids execution but does not remove the queued command.
    // Wait for admission of this read-only fence; transient Full is legitimate
    // until the worker dequeues the cancelled set. Never retry a write.
    let limit = Instant::now() + WAIT;
    loop {
        client.send(json!({"op":"get", "key":"a/enabled"}));
        client.wait_input();
        client.step(&server);
        if !client.connection.session.pending.is_empty() {
            break;
        }
        assert_eq!(client.next(&server)["error"], "runtime_busy");
        assert!(Instant::now() < limit);
        thread::yield_now();
    }
    entered.recv_timeout(WAIT).unwrap();
    release.send(()).unwrap();
    assert_eq!(client.next(&server)["value"], false); // cancelled write never ran
    assert!(
        p.calls
            .lock()
            .unwrap()
            .iter()
            .all(|call| !call.ends_with(":true"))
    );
    // Old ids cannot be reused and must not produce a second final result for id 1.
    client
        .wire
        .write_all(format!("{}\n", json!({"id":read_id,"op":"ping"})).as_bytes())
        .unwrap();
    let duplicate = client.next(&server);
    assert_eq!(duplicate["error"], "duplicate_id");
    assert!(duplicate.get("id").is_none());
    assert_eq!(
        client.request(&server, json!({"op":"ping"}))["status"],
        "ok"
    );
}

#[test]
fn concurrent_started_set_timeout_remains_outcome_unknown_without_retry() {
    let (gate, entered, release) = gate();
    let mut driver = MockDriver::<1>::new();
    driver.gate = Some(gate);
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("sample", None).unwrap();
    let mut registry = DeviceRegistry::new();
    let p = port(&mut registry, "p", vec![a], 4, 1);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
    let endpoint = listener();
    let mut client = Client::connect(&endpoint, &server);
    let id = client.send(json!({"op":"set", "key":"a/enabled", "value":true}));
    client.admitted(&server);
    entered.recv_timeout(WAIT).unwrap();
    client
        .connection
        .session
        .collect_ready(Instant::now() + Duration::from_secs(20))
        .unwrap();
    let response = client.next(&server);
    assert_eq!(response["id"], id);
    assert_eq!(response["error"], "outcome_unknown");
    release.send(()).unwrap();
    client.send(json!({"op":"get", "key":"a/enabled"}));
    client.admitted(&server);
    entered.recv_timeout(WAIT).unwrap();
    release.send(()).unwrap();
    assert_eq!(client.next(&server)["value"], true);
    assert_eq!(
        p.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.ends_with(":true"))
            .count(),
        1
    );
    assert_eq!(
        client.request(&server, json!({"op":"ping"}))["status"],
        "ok"
    );
}

#[test]
fn physical_set_errors_keep_outcome_unknown_and_port_failures_are_independent() {
    for state in [
        PortState::Synchronized,
        PortState::Desynchronized,
        PortState::Disconnected,
    ] {
        let mut driver = MockDriver::<1>::new();
        driver.failure = Some(DriverError::new("write acknowledgement lost", state));
        let mut a = DeviceInstance::new("a", driver).unwrap();
        a.set_poll_period("sample", None).unwrap();
        let mut registry = DeviceRegistry::new();
        let _p = port(&mut registry, "p", vec![a], 4, 1);
        let _q = port(&mut registry, "q", vec![device("b")], 4, 4);
        let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
        let endpoint = listener();
        let mut client = Client::connect(&endpoint, &server);
        assert_eq!(
            client.request(
                &server,
                json!({"op":"set", "key":"a/enabled", "value":true})
            )["error"],
            "outcome_unknown"
        );
        if state != PortState::Synchronized {
            assert_eq!(
                client.request(&server, json!({"op":"get", "key":"a/enabled"}))["error"],
                "runtime_unavailable"
            );
        }
        assert_eq!(
            client.request(&server, json!({"op":"get", "key":"b/enabled"}))["value"],
            false
        );
    }
}

#[test]
fn invalid_observation_and_unexplained_eof_fail_only_affected_streams() {
    for corrupt in [true, false] {
        let mut registry = DeviceRegistry::new();
        let _p = port(&mut registry, "p", vec![device("a")], 4, 4);
        let _q = port(&mut registry, "q", vec![device("b")], 4, 4);
        let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
        let endpoint = listener();
        let mut a = Client::connect(&endpoint, &server);
        let mut b = Client::connect(&endpoint, &server);
        a.request(&server, json!({"op":"subscribe", "keys":["a/sample"]}));
        b.request(&server, json!({"op":"subscribe", "keys":["b/sample"]}));
        if corrupt {
            server
                .apply_event(AggregatorEvent::InvalidObservation {
                    port_id: "p".into(),
                    error: MappingError::WrongPort {
                        registered_port_id: "q".into(),
                    },
                    observation: DeviceObservation {
                        device_id: "b".into(),
                        observation: Observation {
                            source: "sample".into(),
                            unit: "count".into(),
                            completed_at: SystemTime::UNIX_EPOCH,
                            reading: Ok(Value::Integer(1)),
                        },
                    },
                })
                .unwrap();
        } else {
            server
                .apply_event(AggregatorEvent::SourceClosedWithoutStop {
                    port_id: "p".into(),
                })
                .unwrap();
        }
        assert_eq!(
            a.next(&server)["error"],
            if corrupt {
                "observation_data_lost"
            } else {
                "observation_stream_closed"
            }
        );
        publish(&server, "q", "b/sample", Ok(Value::Integer(9)));
        assert_eq!(b.next(&server)["value"], 9);
        assert_eq!(
            b.request(&server, json!({"op":"get", "key":"a/sample"}))["error"],
            "runtime_unavailable"
        );
        assert_eq!(
            b.request(&server, json!({"op":"get", "key":"b/nested/value"}))["value"],
            5
        );
    }
}

#[test]
fn aggregator_delivers_boolean_float_local_slashes_and_synchronized_read_errors() {
    let mut a = device("a");
    for key in ["nested/value", "level", "enabled"] {
        a.set_poll_period(key, Some(Duration::from_secs(3600)))
            .unwrap();
    }
    let mut failing = MockDriver::<2>::new();
    failing.failure = Some(DriverError::new(
        "complete negative reply",
        PortState::Synchronized,
    ));
    let b = DeviceInstance::new("b", failing).unwrap();
    let mut registry = DeviceRegistry::new();
    let mut p = port(&mut registry, "p", vec![a, b], 8, 16);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 16);
    let mut aggregator = ObservationAggregator::new(server.registry(), [p.source()]).unwrap();
    let endpoint = listener();
    let mut client = Client::connect(&endpoint, &server);
    let keys = [
        "a/sample",
        "a/nested/value",
        "a/level",
        "a/enabled",
        "b/sample",
    ];
    client.request(&server, json!({"op":"subscribe", "keys":keys}));
    let limit = Instant::now() + WAIT;
    let mut measurements = vec![];
    while measurements.len() < keys.len() {
        server.collect(&mut aggregator, 1).unwrap();
        client.step(&server);
        measurements.extend(client.messages.drain(..));
        assert!(Instant::now() < limit);
        thread::yield_now();
    }
    let lookup = |key| measurements.iter().find(|m| m["key"] == key).unwrap();
    assert_eq!(lookup("a/nested/value")["value"], 5);
    assert_eq!(lookup("a/level")["value"], 1.25);
    assert_eq!(lookup("a/level")["unit"], "V");
    assert_eq!(lookup("a/enabled")["value"], false);
    assert_eq!(lookup("a/enabled")["unit"], "");
    assert_eq!(lookup("b/sample")["error"], "complete negative reply");
    for key in keys {
        let get = client.request(&server, json!({"op":"get", "key":key}));
        let event = lookup(key);
        assert_eq!(get["completed_at_unix_ms"], event["completed_at_unix_ms"]);
        assert_eq!(get["value"], event["value"]);
        assert_eq!(get["error"], event["error"]);
    }
    assert!(p.handle.stop_reason().is_none());
}

#[test]
fn saturated_port_stream_cannot_be_restored_by_a_new_tcp_subscription() {
    let mut registry = DeviceRegistry::new();
    let _p = port(&mut registry, "p", vec![device("a")], 4, 4);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
    let endpoint = listener();
    server
        .apply_event(AggregatorEvent::DataLost {
            port_id: "p".into(),
            loss_epoch: u64::MAX,
        })
        .unwrap();
    for _ in 0..2 {
        let mut client = Client::connect(&endpoint, &server);
        assert_eq!(
            client.request(&server, json!({"op":"subscribe", "keys":["a/sample"]}))["status"],
            "ok"
        );
        assert_eq!(client.next(&server)["error"], "observation_data_lost");
    }
}

#[test]
fn concurrent_tcp_senders_use_one_endpoint_and_the_production_connection_loop() {
    let mut registry = DeviceRegistry::new();
    let _p = port(
        &mut registry,
        "p",
        vec![
            device("d0"),
            DeviceInstance::new("d1", MockDriver::<2>::new()).unwrap(),
        ],
        8,
        8,
    );
    let _q = port(&mut registry, "q", vec![device("d2"), device("d3")], 8, 8);
    let server = Arc::new(MultiDeviceTcp::new(CommandRouter::new(registry), 8));
    let endpoint = listener();
    let address = endpoint.local_addr().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let clients: Vec<_> = (0..4).map(|device| {
        let barrier = barrier.clone();
        thread::spawn(move || {
            let stream = TcpStream::connect(address).unwrap();
            stream.set_read_timeout(Some(WAIT)).unwrap(); stream.set_write_timeout(Some(WAIT)).unwrap(); stream.set_nodelay(true).unwrap();
            let mut wire = io::BufReader::new(stream); barrier.wait();
            let mut id = 0_u64;
            for round in 0..8 {
                let value = i64::MAX - device * 100 - round;
                for op in ["set", "get"] {
                    id += 1;
                    writeln!(wire.get_mut(), "{}", json!({"id":id,"op":op,"key":format!("d{device}/nested/value"),"value":value})).unwrap();
                    let mut reply = String::new(); assert!(wire.read_line(&mut reply).unwrap() > 0);
                    let reply: Json = serde_json::from_str(&reply).unwrap(); assert_eq!(reply["id"], id); assert_eq!(reply["status"], "ok");
                    if op == "get" { assert_eq!(reply["value"].as_i64(), Some(value)); }
                }
            }
        })
    }).collect();
    let slots = lab_server::client_limit::ClientSlots::new();
    let mut connections = vec![];
    for _ in 0..4 {
        let (socket, _) = endpoint.accept().unwrap();
        let server = server.clone();
        let permit = slots.try_acquire().unwrap();
        connections.push(thread::spawn(move || {
            let _permit = permit;
            server.serve_connection(socket).unwrap();
        }));
    }
    for client in clients {
        client.join().unwrap();
    }
    for connection in connections {
        connection.join().unwrap();
    }
}

#[test]
fn concurrent_fatal_set_rejects_queued_request_without_reclassifying_started_write() {
    let (gate, entered, release) = gate();
    let mut driver = MockDriver::<1>::new();
    driver.gate = Some(gate);
    driver.failure = Some(DriverError::new(
        "ambiguous physical reply",
        PortState::Desynchronized,
    ));
    let mut a = DeviceInstance::new("a", driver).unwrap();
    a.set_poll_period("sample", None).unwrap();
    let mut registry = DeviceRegistry::new();
    let p = port(&mut registry, "p", vec![a], 4, 1);
    let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
    let endpoint = listener();
    let mut client = Client::connect(&endpoint, &server);
    let started = client.send(json!({"op":"set", "key":"a/enabled", "value":true}));
    client.admitted(&server);
    entered.recv_timeout(WAIT).unwrap();
    let queued = client.send(json!({"op":"set", "key":"a/enabled", "value":false}));
    let ping = client.send(json!({"op":"ping"}));
    assert_eq!(client.next(&server)["id"], ping);
    release.send(()).unwrap();
    let mut replies = [client.next(&server), client.next(&server)];
    replies.sort_by_key(|reply| reply["id"].as_u64().unwrap());
    assert_eq!(replies[0]["id"], started);
    assert_eq!(replies[0]["error"], "outcome_unknown");
    assert_eq!(replies[1]["id"], queued);
    assert_eq!(replies[1]["error"], "runtime_unavailable");
    assert_eq!(p.calls.lock().unwrap().len(), 1);
}

#[test]
fn physical_port_stop_drains_error_observation_before_tcp_stream_closed() {
    for state in [PortState::Desynchronized, PortState::Disconnected] {
        let mut driver = MockDriver::<1>::new();
        driver.failure = Some(DriverError::new("physical read failed", state));
        let a = DeviceInstance::new("a", driver).unwrap();
        let mut registry = DeviceRegistry::new();
        let mut p = port(&mut registry, "p", vec![a], 4, 4);
        let _q = port(&mut registry, "q", vec![device("b")], 4, 4);
        let reason = p.join.take().unwrap().join().unwrap();
        assert_eq!(
            reason,
            StopReason::Driver(DriverError::new("physical read failed", state))
        );
        let server = MultiDeviceTcp::new(CommandRouter::new(registry), 8);
        let endpoint = listener();
        let mut aggregator = ObservationAggregator::new(server.registry(), [p.source()]).unwrap();
        let mut client = Client::connect(&endpoint, &server);
        let mut other = Client::connect(&endpoint, &server);
        client.request(&server, json!({"op":"subscribe", "keys":["a/sample"]}));
        other.request(&server, json!({"op":"subscribe", "keys":["b/sample"]}));
        server.collect(&mut aggregator, 1).unwrap();
        server.collect(&mut aggregator, 1).unwrap();
        let measurement = client.next(&server);
        assert_eq!(measurement["event"], "measurement");
        assert_eq!(measurement["error"], "physical read failed");
        assert_eq!(client.next(&server)["error"], "observation_stream_closed");
        assert_eq!(
            other.request(&server, json!({"op":"get", "key":"a/sample"}))["error"],
            "runtime_unavailable"
        );
        assert_eq!(
            other.request(&server, json!({"op":"get", "key":"b/nested/value"}))["value"],
            5
        );
    }
}
