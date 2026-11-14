//! Real factory, driver, PortWorker, DeviceService and TCP; no serial port access.
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use lab_server::config::ServerConfig;
use lab_server::driver::{DeviceIo, PortState, metakon::Metakon};
use lab_server::model::Value;
use lab_server::pending::{Completion, Pending, Poll};
use lab_server::port_worker::{CommandError, DeviceInstance, PortChannels, PortWorker, StopReason};
use lab_server::startup::{create_driver, start_with};
use serde_json::{Value as Json, json};

const WAIT: Duration = Duration::from_secs(10);
// Independent emulator implementation of the manufacturer's bit algorithm.
fn crc(bytes: &[u8]) -> u8 {
    let mut result = 255;
    for byte in bytes {
        let mut data = *byte;
        for _ in 0..8 {
            let feedback = (data ^ result) & 1;
            if feedback != 0 {
                result ^= 0x18;
            }
            result = (result >> 1) | (feedback << 7);
            data >>= 1;
        }
    }
    result
}
fn frame(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(crc(&bytes));
    bytes
}

#[derive(Clone, Copy)]
enum Fault {
    AckTimeout,
    AckDisconnect,
    AckCrc,
    ReadbackTimeout,
    ReadbackMismatch,
    PartialRead,
    BadReadCrc,
    LongRead,
}
struct Gate {
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
#[derive(Default)]
struct State {
    sent: Vec<Vec<u8>>,
    registers: HashMap<[u8; 3], Vec<u8>>,
    fault: Option<Fault>,
    sensor_fault: bool,
    gate: Option<Gate>,
    dropped: bool,
}
type Shared = Arc<Mutex<State>>;
struct Serial {
    state: Shared,
    input: Cursor<Vec<u8>>,
    error: Option<io::ErrorKind>,
}
impl Serial {
    fn new(state: &Shared) -> Self {
        Self {
            state: state.clone(),
            input: Cursor::new(vec![]),
            error: None,
        }
    }
}
impl Drop for Serial {
    fn drop(&mut self) {
        self.state.lock().unwrap().dropped = true;
    }
}
impl Read for Serial {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let available = self.fill_buf()?;
        let count = output.len().min(available.len());
        output[..count].copy_from_slice(&available[..count]);
        self.consume(count);
        Ok(count)
    }
}
impl BufRead for Serial {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.input.position() as usize == self.input.get_ref().len()
            && let Some(kind) = self.error
        {
            return Err(kind.into());
        }
        self.input.fill_buf()
    }
    fn consume(&mut self, size: usize) {
        self.input.consume(size);
    }
}
impl DeviceIo for Serial {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        assert_eq!(
            self.input.position() as usize,
            self.input.get_ref().len(),
            "response tail / interleaving"
        );
        assert_eq!(
            crc(&request[..request.len() - 1]),
            request[request.len() - 1]
        );
        let mut state = self.state.lock().unwrap();
        state.sent.push(request.to_vec());
        let register = request[2];
        let address = [request[0], request[1], register];
        let flags = [
            0x41, 0x44, 0xc4, 0xc3, 0xc3, 0xc1, 0xc2, 0x40, 0x40, 0xc4, 0xc1, 0xc0, 0xc4, 0xc1,
            0xc0,
        ][register as usize];
        let default = match register {
            0 => vec![3],
            1 if state.sensor_fault => vec![0, 0x80],
            1 => vec![0xd2, 4],
            2 | 3 | 4 | 9 | 12 => vec![100, 0],
            6 => vec![25],
            _ => vec![0],
        };
        let data = state.registers.entry(address).or_insert(default);
        let mut response = request[..4].to_vec();
        if request[3] == 1 {
            assert_eq!(request[4], flags & 15);
            assert_eq!(
                request.len(),
                if matches!(flags & 15, 3 | 4) { 8 } else { 7 }
            );
            *data = request[5..request.len() - 1].to_vec();
        } else {
            assert_eq!(request.len(), 5);
            response.push(flags);
            response.extend_from_slice(data);
        }
        response = frame(response);
        self.error = None;
        match (request[3], register, state.fault) {
            (1, _, Some(Fault::AckTimeout)) | (0, 2, Some(Fault::ReadbackTimeout)) => {
                response.clear();
                self.error = Some(io::ErrorKind::TimedOut);
            }
            (1, _, Some(Fault::AckDisconnect)) => {
                response.clear();
                self.error = Some(io::ErrorKind::BrokenPipe);
            }
            (1, _, Some(Fault::AckCrc)) | (0, 0, Some(Fault::BadReadCrc)) => {
                *response.last_mut().unwrap() ^= 1;
            }
            (1, _, Some(Fault::ReadbackMismatch)) => {
                state.registers.get_mut(&address).unwrap()[0] ^= 1;
            }
            (0, 0, Some(Fault::PartialRead)) => {
                response.truncate(3);
                self.error = Some(io::ErrorKind::TimedOut);
            }
            (0, 0, Some(Fault::LongRead)) => {
                response.extend_from_slice(&[0xde, 0xad]);
            }
            _ => {}
        }
        let gate = if request[3] == 1 {
            state.gate.take()
        } else {
            None
        };
        drop(state);
        self.input = Cursor::new(response);
        if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.resume.recv_timeout(WAIT).unwrap();
        }
        Ok(())
    }
}

fn options(address: u8, channel: u8, enabled: bool) -> toml::Table {
    toml::from_str(&format!("address={address}\nchannel={channel}\nprocess_value_scale=0.1\nprocess_value_unit='degC'\nwrite_enabled={enabled}")).unwrap()
}
fn instance(id: &str, address: u8, channel: u8) -> DeviceInstance {
    let mut instance = DeviceInstance::new(
        id,
        Metakon::from_options(&options(address, channel, true)).unwrap(),
    )
    .unwrap();
    for key in ["measurement", "output_power"] {
        instance.set_poll_period(key, None).unwrap();
    }
    instance
}
fn build_worker(
    state: &Shared,
    devices: Vec<DeviceInstance>,
) -> (PortWorker<Serial>, PortChannels) {
    PortWorker::new(Serial::new(state), devices, 8, 32).unwrap()
}
fn received<T>(mut pending: Pending<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        match pending.poll(Instant::now()) {
            Poll::Finished(Completion::Received { value, .. }) => return value,
            Poll::Waiting(next) => pending = next,
            _ => panic!("missing explicit reply"),
        }
        assert!(Instant::now() < deadline, "reply timeout");
        thread::yield_now();
    }
}
fn config(enabled: bool) -> ServerConfig {
    let mut config =
        ServerConfig::from_toml(include_str!("../examples/metakon-readonly.toml")).unwrap();
    config.tcp.bind = "127.0.0.1:0".parse().unwrap();
    config.devices[0].options = options(1, 0, enabled);
    config.devices[0]
        .poll_periods_ms
        .insert("measurement".into(), 20);
    config.devices[0]
        .poll_periods_ms
        .insert("output_power".into(), 20);
    config
}
struct Running {
    address: SocketAddr,
    state: Shared,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<io::Result<()>>>,
}
impl Running {
    fn new(enabled: bool) -> Self {
        let state = Shared::default();
        let server = start_with(&config(enabled), create_driver, |_, delay| {
            assert_eq!(delay, Duration::ZERO);
            Ok(Serial::new(&state))
        })
        .unwrap();
        let address = server.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let join = Some(thread::spawn(move || server.run_until(&flag)));
        Self {
            address,
            state,
            stop,
            join,
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.join.take().unwrap().join().unwrap().unwrap();
        assert!(
            self.state.lock().unwrap().dropped,
            "port not released at shutdown"
        );
    }
}
struct Client(BufReader<TcpStream>);
impl Client {
    fn new(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        stream.set_write_timeout(Some(WAIT)).unwrap();
        Self(BufReader::new(stream))
    }
    fn next(&mut self) -> Json {
        let mut line = String::new();
        assert!(
            self.0.read_line(&mut line).unwrap() > 0,
            "unexpected TCP EOF"
        );
        serde_json::from_str(&line).unwrap()
    }
    fn request(&mut self, request: Json) -> Json {
        writeln!(self.0.get_mut(), "{request}").unwrap();
        loop {
            let reply = self.next();
            if reply.get("id").is_some() {
                assert_eq!(reply["id"], request["id"]);
                return reply;
            }
        }
    }
}

#[test]
fn real_factory_validates_before_any_port_is_opened() {
    let template = config(false);
    assert!(!template.recording.as_ref().unwrap().enabled);
    for change in [
        "channel=3",
        "address=256",
        "process_value_scale=nan",
        "write_enabled=1",
        "extra=true",
    ] {
        let mut config = template.clone();
        config.devices[0]
            .options
            .extend(toml::from_str::<toml::Table>(change).unwrap());
        let result = start_with(&config, create_driver, |_, _| -> io::Result<Serial> {
            panic!("invalid configuration opened equipment")
        });
        assert!(result.is_err(), "accepted {change}");
    }
    let setup = create_driver(&template.devices[0]).unwrap();
    assert_eq!(setup.post_open_delay, Duration::ZERO);
    assert_eq!(setup.driver.parameters().len(), 15);
    assert!(setup.driver.parameters().iter().all(|p| !p.writable));
}

#[test]
fn tcp_readonly_hello_catalog_get_subscriptions_sensor_fault_and_shutdown() {
    let server = Running::new(false);
    let mut client = Client::new(server.address);
    assert_eq!(client.request(json!({"id":1,"op":"hello"}))["version"], 1);
    let description = client.request(json!({"id":2,"op":"describe"}));
    let parameters = description["parameters"].as_array().unwrap();
    assert_eq!(parameters.len(), 15);
    assert!(parameters.iter().all(|p| p["writable"] == false));
    assert_eq!(
        client.request(json!({"id":3,"op":"get","key":"metakon1/channel_type"}))["value"],
        3
    );
    assert_eq!(
        client.request(json!({"id":4,"op":"set","key":"metakon1/setpoint","value":10.0}))["status"],
        "error"
    );
    writeln!(
        client.0.get_mut(),
        "{}",
        json!({"id":5,"op":"subscribe","keys":["metakon1/measurement","metakon1/output_power"]})
    )
    .unwrap();
    let ack = client.next();
    assert_eq!(ack["id"], 5, "measurement preceded subscribe ACK");
    assert_eq!(ack["status"], "ok");
    let mut seen = Vec::new();
    let deadline = Instant::now() + WAIT;
    while seen.len() < 2 {
        assert!(Instant::now() < deadline, "missing polled register");
        let event = client.next();
        assert_eq!(event["event"], "measurement");
        assert_eq!(event["status"], "ok");
        let key = event["key"].as_str().unwrap().to_owned();
        if key.ends_with("/measurement") {
            assert_eq!(event["value"], 123.4);
            assert_eq!(event["unit"], "degC");
        } else {
            assert_eq!(key, "metakon1/output_power");
            assert_eq!(event["value"], 25);
            assert_eq!(event["unit"], "%");
        }
        if !seen.contains(&key) {
            seen.push(key);
        }
    }
    assert_eq!(
        client.request(json!({"id":6,"op":"get","key":"metakon1/measurement"}))["value"],
        123.4
    );
    // Remove the emulator's cached raw sample so its next poll reports a sensor fault.
    {
        let mut state = server.state.lock().unwrap();
        state.sensor_fault = true;
        state.registers.remove(&[1, 0, 1]);
    }
    let deadline = Instant::now() + WAIT;
    loop {
        assert!(Instant::now() < deadline, "sensor fault was not published");
        let event = client.next();
        if event["key"] == "metakon1/measurement" && event["status"] == "error" {
            assert!(event["error"].as_str().unwrap().contains("sensor fault"));
            break;
        }
    }
    assert_eq!(
        client.request(json!({"id":7,"op":"get","key":"metakon1/channel_type"}))["value"],
        3
    );
    assert_eq!(client.request(json!({"id":8,"op":"unsubscribe","keys":["metakon1/measurement","metakon1/output_power"]}))["keys"],json!([]));
    assert_eq!(
        client.request(json!({"id":9,"op":"ping"})),
        json!({"id":9,"status":"ok"})
    );
    assert!(server.state.lock().unwrap().sent.iter().all(|f| f[3] == 0));
}

#[test]
fn tcp_set_uses_one_write_and_one_readback_with_strict_types_and_boolean_ff() {
    let server = Running::new(true);
    let mut client = Client::new(server.address);
    for (id, key, value) in [
        (1, "setpoint", json!(12.3)),
        (2, "upper_output", json!(true)),
        (3, "upper_output", json!(false)),
    ] {
        assert_eq!(
            client
                .request(json!({"id":id,"op":"set","key":format!("metakon1/{key}"),"value":value})),
            json!({"id":id,"status":"ok"})
        );
    }
    for (id, key, value) in [
        (4, "setpoint", json!(12)),
        (5, "output_power", json!(20.0)),
        (6, "upper_output", json!(1)),
        (7, "upper_output", json!(1.0)),
    ] {
        assert_eq!(
            client
                .request(json!({"id":id,"op":"set","key":format!("metakon1/{key}"),"value":value}))
                ["status"],
            "error"
        );
    }
    let state = server.state.lock().unwrap();
    let writes: Vec<_> = state
        .sent
        .iter()
        .enumerate()
        .filter(|(_, f)| f[3] == 1)
        .collect();
    assert_eq!(writes.len(), 3);
    for (index, write) in &writes {
        assert_eq!(
            &state.sent[index + 1][..4],
            &[write[0], write[1], write[2], 0]
        );
    }
    assert_eq!(&writes[0].1[5..7], &[123, 0]);
    assert_eq!(writes[1].1[5], 0xff);
    assert_eq!(writes[2].1[5], 0);
}

#[test]
fn tcp_ambiguous_writes_never_ack_or_retry_even_if_the_emulator_applied_them() {
    for fault in [
        Fault::AckTimeout,
        Fault::AckDisconnect,
        Fault::AckCrc,
        Fault::ReadbackTimeout,
        Fault::ReadbackMismatch,
    ] {
        let server = Running::new(true);
        let mut client = Client::new(server.address);
        server.state.lock().unwrap().fault = Some(fault);
        let reply =
            client.request(json!({"id":1,"op":"set","key":"metakon1/setpoint","value":12.3}));
        assert_eq!(
            reply,
            json!({"id":1,"status":"error","error":"outcome_unknown"})
        );
        // A ping detects a duplicate / late command ACK as well as a dead listener.
        assert_eq!(
            client.request(json!({"id":2,"op":"ping"})),
            json!({"id":2,"status":"ok"})
        );
        let state = server.state.lock().unwrap();
        assert_eq!(state.sent.iter().filter(|f| f[3] == 1).count(), 1);
        assert!(
            state.registers.contains_key(&[1, 0, 2]),
            "write was not exercised"
        );
    }
}

#[test]
fn multiple_addresses_and_channels_share_one_worker_without_interleaving() {
    let state = Shared::default();
    let (worker, channels) = build_worker(
        &state,
        vec![
            instance("a", 1, 0),
            instance("b", 2, 1),
            instance("c", 1, 2),
        ],
    );
    let pending: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            channels
                .commands
                .set(
                    i as u64 + 1,
                    key,
                    "setpoint",
                    Value::Float(12.3 + i as f64),
                    Instant::now() + WAIT,
                )
                .unwrap()
        })
        .collect();
    let join = thread::spawn(move || worker.run());
    for request in pending {
        assert_eq!(received(request), Ok(()));
    }
    channels.commands.shutdown();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    let state = state.lock().unwrap();
    assert_eq!(state.sent.len(), 6);
    for (pair, address) in
        state
            .sent
            .as_chunks::<2>()
            .0
            .iter()
            .zip([[1, 0, 2], [2, 1, 2], [1, 2, 2]])
    {
        assert_eq!(&pair[0][..3], &address);
        assert_eq!(&pair[1][..3], &address);
        assert_eq!(pair[0][3], 1);
        assert_eq!(pair[1][3], 0);
    }
}

#[test]
fn incomplete_corrupt_and_long_reads_stop_shared_port_before_another_device() {
    for fault in [Fault::PartialRead, Fault::BadReadCrc, Fault::LongRead] {
        let state = Shared::default();
        state.lock().unwrap().fault = Some(fault);
        let (worker, channels) =
            build_worker(&state, vec![instance("a", 1, 0), instance("b", 2, 0)]);
        let first = channels
            .commands
            .read(1, "a", "channel_type", Instant::now() + WAIT)
            .unwrap();
        let second = channels
            .commands
            .read(2, "b", "channel_type", Instant::now() + WAIT)
            .unwrap();
        let reason = worker.run();
        assert!(
            matches!(reason,StopReason::Driver(ref e) if e.port_state==PortState::Desynchronized)
        );
        assert!(received(first).unwrap().observation.reading.is_err());
        assert_eq!(
            received(second).unwrap_err(),
            CommandError::PortStopped(reason)
        );
        assert_eq!(state.lock().unwrap().sent.len(), 1);
    }
}

#[test]
fn polling_one_register_emits_only_its_observations_and_survives_sensor_fault() {
    for sensor_fault in [false, true] {
        let state = Shared::default();
        state.lock().unwrap().sensor_fault = sensor_fault;
        let mut device = instance("a", 1, 0);
        device
            .set_poll_period("measurement", Some(Duration::from_millis(5)))
            .unwrap();
        let (worker, channels) = build_worker(&state, vec![device]);
        let join = thread::spawn(move || worker.run());
        for _ in 0..3 {
            let observation = channels.observations.recv_timeout(WAIT).unwrap();
            assert_eq!(observation.device_id, "a");
            assert_eq!(observation.observation.source, "measurement");
            if sensor_fault {
                assert!(
                    observation
                        .observation
                        .reading
                        .unwrap_err()
                        .contains("sensor fault")
                );
            } else {
                assert_eq!(
                    observation.observation.reading.unwrap(),
                    Value::Float(123.4)
                );
            }
        }
        channels.commands.shutdown();
        assert_eq!(join.join().unwrap(), StopReason::Shutdown);
        for observation in channels.observations.try_iter() {
            assert_eq!(observation.observation.source, "measurement");
        }
        assert!(
            state
                .lock()
                .unwrap()
                .sent
                .iter()
                .all(|f| f[2] == 1 && f[3] == 0)
        );
    }
}

#[test]
fn set_timeout_before_execution_sends_nothing_and_after_start_is_unknown() {
    let state = Shared::default();
    let (worker, channels) = build_worker(&state, vec![instance("a", 1, 0)]);
    let deadline = Instant::now() + WAIT;
    let pending = channels
        .commands
        .set(1, "a", "setpoint", Value::Float(12.3), deadline)
        .unwrap();
    assert!(matches!(
        pending.poll(deadline),
        Poll::Finished(Completion::Timeout { id: 1 })
    ));
    let fence = channels
        .commands
        .read(2, "a", "channel_type", Instant::now() + WAIT)
        .unwrap();
    let join = thread::spawn(move || worker.run());
    assert!(received(fence).is_ok());
    channels.commands.shutdown();
    join.join().unwrap();
    assert!(state.lock().unwrap().sent.iter().all(|f| f[3] == 0));

    let state = Shared::default();
    let (entered, started) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    state.lock().unwrap().gate = Some(Gate { entered, resume });
    let (worker, channels) = build_worker(&state, vec![instance("a", 1, 0)]);
    let deadline = Instant::now() + WAIT;
    let pending = channels
        .commands
        .set(3, "a", "setpoint", Value::Float(12.3), deadline)
        .unwrap();
    let join = thread::spawn(move || worker.run());
    started.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        pending.poll(deadline),
        Poll::Finished(Completion::OutcomeUnknown { id: 3 })
    ));
    release.send(()).unwrap();
    let fence = channels
        .commands
        .read(4, "a", "channel_type", Instant::now() + WAIT)
        .unwrap();
    assert!(received(fence).is_ok());
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(
        state
            .lock()
            .unwrap()
            .sent
            .iter()
            .filter(|f| f[3] == 1)
            .count(),
        1
    );
}
