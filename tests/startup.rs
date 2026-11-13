use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use lab_server::config::{DeviceConfig, ServerConfig};
use lab_server::device_registry::Availability;
use lab_server::driver::{DeviceIo, Driver, DriverError};
use lab_server::model::{ParameterDescription, Spec, Value};
use lab_server::startup::{DriverSetup, StartedServer, create_driver, parse_config_path, start_with};
use lab_server::tcp::MultiDeviceTcp;
use serde_json::{Value as Json, json};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct PortProbe {
    disconnected: AtomicBool,
    dropped: AtomicBool,
    calls: Mutex<Vec<String>>,
}

struct FakeIo {
    input: Cursor<Vec<u8>>,
    probe: Arc<PortProbe>,
}

impl Drop for FakeIo {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::Release);
    }
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
    fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.probe.disconnected.load(Ordering::Acquire) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        assert!(
            self.input.fill_buf()?.is_empty(),
            "interleaved transactions"
        );
        let request = String::from_utf8(bytes.to_vec()).unwrap();
        self.probe.calls.lock().unwrap().push(request.clone());
        // Also support the actual Arduino factory/driver without hardware.
        let parts: Vec<_> = request.split_whitespace().collect();
        let reply = if parts.first() == Some(&"o") {
            if parts[2] == "get" {
                format!("@{} ok 17\n", parts[1])
            } else {
                format!("@{} ok\n", parts[1])
            }
        } else {
            "ok\n".into()
        };
        self.input = Cursor::new(reply.into_bytes());
        Ok(())
    }
}

struct FakeDriver<const KIND: u8> {
    value: i64,
}
impl<const KIND: u8> FakeDriver<KIND> {
    fn exchange(&self, key: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        io.send(format!("{KIND}:{key}").as_bytes())?;
        let mut reply = String::new();
        io.read_line(&mut reply)?;
        assert_eq!(reply, "ok\n");
        Ok(())
    }
}
impl<const KIND: u8> Driver for FakeDriver<KIND> {
    fn parameters(&self) -> Vec<ParameterDescription> {
        ["sample", "value"]
            .into_iter()
            .map(|id| ParameterDescription {
                id: id.into(),
                spec: Spec::Integer { min: 0, max: 100 },
                unit: "".into(),
                writable: true,
                default_poll_period: (id == "sample").then_some(Duration::from_millis(100)),
            })
            .collect()
    }
    fn read(&mut self, key: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.exchange(key, io)?;
        Ok(Value::Integer(self.value))
    }
    fn write(
        &mut self,
        key: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        self.exchange(key, io)?;
        let Value::Integer(value) = value else {
            panic!("unexpected value")
        };
        self.value = *value;
        Ok(())
    }
}

fn factory(config: &DeviceConfig) -> Result<DriverSetup, String> {
    // Nested TOML reaches the factory intact, rather than being flattened.
    let value = config
        .options
        .get("initial")
        .and_then(|v| v.get("value"))
        .and_then(toml::Value::as_integer)
        .unwrap_or(7);
    let driver: Box<dyn Driver + Send> = match config.driver.as_str() {
        "fake_a" => Box::new(FakeDriver::<1> { value }),
        "fake_b" => Box::new(FakeDriver::<2> { value }),
        _ => return Err("unknown test driver".into()),
    };
    Ok(DriverSetup {
        driver,
        post_open_delay: Duration::ZERO,
    })
}

fn config(ports: usize, devices: &[(&str, &str, usize)]) -> ServerConfig {
    let mut text = "[tcp]\nbind = '127.0.0.1:0'\n".to_owned();
    for i in 0..ports {
        text += &format!(
            "\n[[ports]]\nid = 'p{i}'\nname = 'FAKE{i}'\nbaud = 57600\ndata_bits = 8\nparity = 'none'\nstop_bits = 1\nflow_control = 'none'\ntimeout_ms = 3000\n"
        );
    }
    for (id, driver, port) in devices {
        text += &format!("\n[[devices]]\nid = '{id}'\ndriver = '{driver}'\nport = 'p{port}'\n");
    }
    ServerConfig::from_toml(&text).unwrap()
}

struct Running {
    addr: SocketAddr,
    tcp: Arc<MultiDeviceTcp>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<io::Result<()>>>,
}
impl Running {
    fn new(server: StartedServer) -> Self {
        let addr = server.local_addr().unwrap();
        let tcp = server.backend().clone();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let join = Some(thread::spawn(move || server.run_until(&worker_stop)));
        Self {
            addr,
            tcp,
            stop,
            join,
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.join.take().unwrap().join().unwrap().unwrap();
    }
}

struct Client {
    reader: BufReader<TcpStream>,
    next_id: u64,
    events: VecDeque<Json>,
}
impl Client {
    fn new(addr: SocketAddr) -> Self {
        let socket = TcpStream::connect_timeout(&addr, WAIT).unwrap();
        socket.set_read_timeout(Some(WAIT)).unwrap();
        socket.set_write_timeout(Some(WAIT)).unwrap();
        Self {
            reader: BufReader::new(socket),
            next_id: 0,
            events: VecDeque::new(),
        }
    }
    fn read(&mut self) -> Json {
        let mut line = String::new();
        assert_ne!(
            self.reader.read_line(&mut line).unwrap(),
            0,
            "unexpected EOF"
        );
        serde_json::from_str(&line).unwrap()
    }
    fn request(&mut self, mut request: Json) -> Json {
        self.next_id += 1;
        request["id"] = json!(self.next_id);
        writeln!(self.reader.get_mut(), "{request}").unwrap();
        loop {
            let reply = self.read();
            if reply.get("event").is_some() {
                self.events.push_back(reply);
            } else {
                assert_eq!(reply["id"], self.next_id);
                return reply;
            }
        }
    }
    fn event(&mut self, key: &str) -> Json {
        let end = Instant::now() + WAIT;
        loop {
            let event = self.events.pop_front().unwrap_or_else(|| self.read());
            if event["key"] == key {
                return event;
            }
            assert!(Instant::now() < end, "missing event for {key}");
        }
    }
}

fn launch(config: &ServerConfig, failed: &[usize]) -> (Running, Vec<Arc<PortProbe>>) {
    let probes: Vec<_> = config
        .ports
        .iter()
        .map(|_| Arc::new(PortProbe::default()))
        .collect();
    let mut opened = Vec::new();
    let server = start_with(config, factory, |port, delay| {
        let index = config.ports.iter().position(|p| p.id == port.id).unwrap();
        assert!(!opened.contains(&index), "physical port opened twice");
        opened.push(index);
        assert_eq!(port, &config.ports[index]);
        assert_eq!(delay, Duration::ZERO);
        if failed.contains(&index) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "test access denied",
            ));
        }
        Ok(FakeIo {
            input: Cursor::new(vec![]),
            probe: probes[index].clone(),
        })
    })
    .unwrap();
    assert_eq!(opened.len(), config.ports.len());
    (Running::new(server), probes)
}

fn exercise(client: &mut Client, device: &str, value: i64) {
    assert_eq!(client.request(json!({"op":"hello"}))["version"], 1);
    let key = format!("{device}/value");
    assert_eq!(
        client.request(json!({"op":"set", "key":key, "value":value}))["status"],
        "ok"
    );
    assert_eq!(
        client.request(json!({"op":"get", "key":key}))["value"],
        value
    );
    let sample = format!("{device}/sample");
    assert_eq!(
        client.request(json!({"op":"subscribe", "key":sample}))["status"],
        "ok"
    );
    let event = client.event(&sample);
    assert_eq!(event["event"], "measurement");
    assert_eq!(event["value"], value);
    assert_eq!(
        client.request(json!({"op":"get", "key":sample}))["value"],
        value
    );
    assert_eq!(
        client.request(json!({"op":"unsubscribe", "keys":[sample]}))["status"],
        "ok"
    );
}

#[test]
fn one_port_tcp_commands_and_subscriptions() {
    let (running, probes) = launch(&config(1, &[("a", "fake_a", 0)]), &[]);
    let mut client = Client::new(running.addr);
    assert_eq!(
        client.request(json!({"op":"describe"}))["parameters"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    exercise(&mut client, "a", 31);
    // Keep the client connected during shutdown: server must close/join it.
    drop(running);
    assert!(probes[0].dropped.load(Ordering::Acquire));
}

#[test]
fn same_driver_instances_share_one_open_and_keep_independent_state_and_periods() {
    let mut config = config(1, &[("a", "fake_a", 0), ("b", "fake_a", 0)]);
    config.devices[0]
        .poll_periods_ms
        .insert("sample".into(), 50);
    config.devices[1].options = toml::from_str("initial = { value = 23 }").unwrap();
    let (running, _) = launch(&config, &[]);
    let mut client = Client::new(running.addr);
    let catalog = client.request(json!({"op":"describe"}));
    let params = catalog["parameters"].as_array().unwrap();
    assert_eq!(params.len(), 4);
    for (key, ns) in [("a/sample", 50_000_000), ("b/sample", 100_000_000)] {
        assert_eq!(
            params.iter().find(|p| p["key"] == key).unwrap()["poll_period_ns"],
            ns
        );
    }
    exercise(&mut client, "a", 42);
    assert_eq!(
        client.request(json!({"op":"get", "key":"b/value"}))["value"],
        23
    );
    exercise(&mut client, "b", 12);
    assert_eq!(
        client.request(json!({"op":"get", "key":"a/value"}))["value"],
        42
    );
}

#[test]
fn different_drivers_on_shared_port_work_for_concurrent_clients() {
    let (running, probes) = launch(&config(1, &[("a", "fake_a", 0), ("b", "fake_b", 0)]), &[]);
    thread::scope(|scope| {
        for (id, value) in [("a", 13), ("b", 27)] {
            let addr = running.addr;
            scope.spawn(move || exercise(&mut Client::new(addr), id, value));
        }
    });
    let calls = probes[0].calls.lock().unwrap();
    assert!(calls.iter().any(|c| c.starts_with("1:")));
    assert!(calls.iter().any(|c| c.starts_with("2:")));
}

#[test]
fn independent_ports_survive_neighbor_failure_and_listener_survives_all_workers() {
    let (running, probes) = launch(&config(2, &[("a", "fake_a", 0), ("b", "fake_b", 1)]), &[]);
    let mut client = Client::new(running.addr);
    exercise(&mut client, "a", 10);
    probes[0].disconnected.store(true, Ordering::Release);
    let deadline = Instant::now() + WAIT;
    while !matches!(
        running.tcp.registry().availability("a"),
        Some(Availability::Stopped { .. })
    ) {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    exercise(&mut client, "b", 29);
    for op in ["get", "set"] {
        assert_eq!(
            client.request(json!({"op":op, "key":"a/value", "value":1}))["error"],
            "runtime_unavailable"
        );
    }
    running
        .tcp
        .registry()
        .port("p1")
        .unwrap()
        .handle()
        .unwrap()
        .shutdown();
    let mut reconnect = Client::new(running.addr);
    assert_eq!(reconnect.request(json!({"op":"hello"}))["version"], 1);
    assert_eq!(
        reconnect.request(json!({"op":"describe"}))["parameters"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        reconnect.request(json!({"op":"get", "key":"b/sample"}))["error"],
        "runtime_unavailable"
    );
    drop(running);
    assert!(probes.iter().all(|p| p.dropped.load(Ordering::Acquire)));
}

#[test]
fn partial_and_total_open_failure_preserve_catalog_and_tcp() {
    for failed in [vec![1], vec![0, 1]] {
        let (running, _) = launch(
            &config(
                2,
                &[("a", "fake_a", 0), ("b", "fake_b", 1), ("c", "fake_a", 1)],
            ),
            &failed,
        );
        let mut client = Client::new(running.addr);
        assert_eq!(client.request(json!({"op":"hello"}))["version"], 1);
        assert_eq!(
            client.request(json!({"op":"describe"}))["parameters"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
        for device in ["a", "b", "c"] {
            if let Some(Availability::Unavailable { reason }) =
                running.tcp.registry().availability(device)
            {
                assert!(reason.contains("test access denied"));
                for key in [format!("{device}/sample"), format!("{device}/value")] {
                    for op in ["get", "set"] {
                        assert_eq!(
                            client.request(json!({"op":op,"key":key,"value":1}))["error"],
                            "runtime_unavailable"
                        );
                    }
                }
            } else {
                exercise(&mut client, device, 55);
            }
        }
    }
}

#[test]
fn configuration_errors_precede_every_hardware_open() {
    let mut cases = Vec::new();
    let mut unknown = config(2, &[("a", "arduino", 0), ("b", "future", 1)]);
    cases.push((unknown.clone(), "unknown driver"));
    unknown.devices[1].driver = "arduino".into();
    unknown.devices[1].options = toml::from_str("nested = { address = [1, 2] }").unwrap();
    cases.push((unknown.clone(), "arduino accepts no options"));
    unknown.devices[1].options.clear();
    unknown.devices[1]
        .poll_periods_ms
        .insert("typo".into(), 100);
    cases.push((unknown.clone(), "unknown parameter"));
    unknown.devices[1].poll_periods_ms.clear();
    unknown.devices[1].poll_periods_ms.insert("power".into(), 0);
    cases.push((unknown.clone(), "greater than zero"));
    unknown.devices[1].poll_periods_ms.clear();
    unknown.ports[1].name = unknown.ports[0].name.clone();
    cases.push((unknown, "duplicate physical port"));
    for (config, expected) in cases {
        let opens = AtomicUsize::new(0);
        let result = start_with(&config, create_driver, |_, _| -> io::Result<FakeIo> {
            opens.fetch_add(1, Ordering::Relaxed);
            panic!("hardware accessed before validation");
        });
        let error = result.err().unwrap().to_string();
        assert!(error.contains(expected), "{error}");
        assert_eq!(opens.load(Ordering::Relaxed), 0);
    }
    assert!(ServerConfig::from_toml("[malformed").is_err());
}

#[test]
fn tcp_bind_failure_is_fatal_before_hardware_open() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = config(1, &[("a", "arduino", 0)]);
    config.tcp.bind = listener.local_addr().unwrap();
    assert!(
        start_with(&config, create_driver, |_, _| -> io::Result<FakeIo> {
            panic!("should not open equipment when TCP bind failed")
        })
        .is_err()
    );
}

#[test]
fn real_arduino_factory_uses_one_reset_delay_and_global_keys() {
    let mut config = ServerConfig::from_toml(include_str!("../examples/arduino.toml")).unwrap();
    config.tcp.bind.set_port(0);
    let mut second = config.devices[0].clone();
    second.id = "second".into();
    config.devices.push(second);
    let probe = Arc::new(PortProbe::default());
    let mut opens = 0;
    let server = start_with(&config, create_driver, |port, delay| {
        opens += 1;
        assert_eq!(port.name, "COM3");
        assert_eq!(port.baud, 57_600);
        assert_eq!(delay, Duration::from_secs(2));
        Ok(FakeIo {
            input: Cursor::new(vec![]),
            probe: probe.clone(),
        })
    })
    .unwrap();
    assert_eq!(opens, 1);
    let running = Running::new(server);
    let mut client = Client::new(running.addr);
    let catalog = client.request(json!({"op":"describe"}));
    assert_eq!(catalog["parameters"].as_array().unwrap().len(), 46);
    assert_eq!(catalog["actions"].as_array().unwrap().len(), 4);
    for id in [&config.devices[0].id, &config.devices[1].id] {
        let key = format!("{id}/power");
        assert_eq!(
            client.request(json!({"op":"set", "key":key, "value":30}))["status"],
            "ok"
        );
        assert_eq!(
            client.request(json!({"op":"subscribe", "key":key}))["status"],
            "ok"
        );
        assert_eq!(client.event(&key)["value"], 17);
        assert_eq!(client.request(json!({"op":"get", "key":key}))["value"], 17);
    }
    assert_eq!(
        client.request(json!({"op":"get", "key":"power"}))["error"],
        "unknown_key"
    );
}

#[test]
fn cli_requires_config_and_rejects_ambiguous_arguments() {
    assert_eq!(
        parse_config_path(["--config".into(), "путь с пробелами.toml".into()]).unwrap(),
        std::path::PathBuf::from("путь с пробелами.toml")
    );
    for args in [
        vec![],
        vec!["--config"],
        vec!["--other"],
        vec!["--config", ""],
        vec!["--config", "a", "b"],
    ] {
        let error = parse_config_path(args.into_iter().map(Into::into)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "usage: lab-server --config <path.toml>");
    }
}

#[path = "startup/recording.rs"]
mod recording;
