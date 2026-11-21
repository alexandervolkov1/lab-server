use std::collections::VecDeque;
use std::io::{self, BufRead, Read};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use lab_server::config::{DeviceConfig, Parity, ServerConfig};
use lab_server::device_registry::DeviceRegistry;
use lab_server::driver::modbus_rtu::{ReadRegisters, RegisterTable, crc16, interframe_silence};
use lab_server::driver::{DeviceIo, Driver, DriverError, PortState};
use lab_server::model::{ParameterDescription, Spec, Value};
use lab_server::pending::{Completion, Poll};
use lab_server::port_worker::{DeviceInstance, PortHandle, PortWorker, StopReason};
use lab_server::session::Session;
use lab_server::startup::{create_driver, start_with};

struct Exchange {
    request: Vec<u8>,
    reply: Vec<u8>,
    chunk_size: usize,
    chunk_delay: Duration,
    read_error: Option<io::ErrorKind>,
    minimum_idle: Duration,
}

impl Exchange {
    fn new(request: impl Into<Vec<u8>>, reply: impl Into<Vec<u8>>) -> Self {
        Self {
            request: request.into(),
            reply: reply.into(),
            chunk_size: usize::MAX,
            chunk_delay: Duration::ZERO,
            read_error: None,
            minimum_idle: Duration::ZERO,
        }
    }
}

struct Bus {
    script: Arc<Mutex<VecDeque<Exchange>>>,
    exchange: Option<Exchange>,
    offset: usize,
    last_receive: Instant,
}

impl Bus {
    fn new(script: Arc<Mutex<VecDeque<Exchange>>>) -> Self {
        Self {
            script,
            exchange: None,
            offset: 0,
            last_receive: Instant::now(),
        }
    }
}

impl Read for Bus {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let bytes = self.fill_buf()?;
        let count = bytes.len().min(dst.len());
        dst[..count].copy_from_slice(&bytes[..count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for Bus {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        let exchange = self.exchange.as_ref().expect("read without send");
        let data = &exchange.reply[self.offset..];
        if data.is_empty()
            && let Some(kind) = exchange.read_error
        {
            return Err(io::Error::from(kind));
        }
        if self.offset != 0 && !exchange.chunk_delay.is_zero() {
            thread::sleep(exchange.chunk_delay);
        }
        Ok(&data[..data.len().min(exchange.chunk_size)])
    }

    fn consume(&mut self, count: usize) {
        self.offset += count;
        self.last_receive = Instant::now();
    }
}

impl DeviceIo for Bus {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        assert!(
            self.exchange
                .as_ref()
                .is_none_or(|last| last.reply.len() == self.offset)
        );
        let exchange = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected send");
        assert_eq!(request, exchange.request);
        assert!(self.last_receive.elapsed() >= exchange.minimum_idle);
        self.exchange = Some(exchange);
        self.offset = 0;
        Ok(())
    }
}

fn config(id: &str, driver: &str, options: &str) -> DeviceConfig {
    DeviceConfig {
        id: id.into(),
        driver: driver.into(),
        port: "bus".into(),
        options: options.parse().unwrap(),
        poll_periods_ms: Default::default(),
    }
}

fn instance(config: &DeviceConfig) -> DeviceInstance {
    let mut device =
        DeviceInstance::new_boxed(&config.id, create_driver(config).unwrap().driver).unwrap();
    let polled: Vec<_> = device
        .parameters()
        .iter()
        .filter(|p| p.default_poll_period.is_some())
        .map(|p| p.id.clone())
        .collect();
    for parameter in polled {
        // Issue explicit reads in the chosen order, without scheduled polls.
        device.set_poll_period(&parameter, None).unwrap();
    }
    device
}

fn read(handle: &PortHandle, id: &str, parameter: &str) -> Result<Value, String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut pending = handle.read(1, id, parameter, deadline).unwrap();
    loop {
        match pending.poll(Instant::now()) {
            Poll::Waiting(next) => pending = next,
            Poll::Finished(Completion::Received { value, .. }) => {
                return value.unwrap().observation.reading;
            }
            _ => panic!("expected read reply"),
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn rtu_frame(body: &[u8]) -> Vec<u8> {
    [body, &crc16(body).to_le_bytes()].concat()
}

fn rtu_read(slave: u8, function: u8, register: u16) -> Vec<u8> {
    let [hi, lo] = register.to_be_bytes();
    rtu_frame(&[slave, function, hi, lo, 0, 1])
}

fn metakon_crc(bytes: &[u8]) -> u8 {
    bytes.iter().fold(0xff, |mut crc, byte| {
        crc ^= byte;
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 == 0 { 0 } else { 0x8c };
        }
        crc
    })
}

fn metakon_frame(body: &[u8]) -> Vec<u8> {
    [body, &[metakon_crc(body)]].concat()
}

fn v1_frame(body: &[u8]) -> Vec<u8> {
    let sum: u16 = body.iter().map(|&b| u16::from(b)).sum();
    [body, &[(sum % 64 + 64) as u8, b'\r']].concat()
}

#[test]
fn startup_paces_mixed_protocols_and_metakon_readback_on_one_port() {
    let config = ServerConfig::from_toml(
        r#"
[tcp]
bind = "127.0.0.1:0"
[recording]
enabled = false
[[ports]]
id = "bus"
name = "emulated"
baud = 9600
data_bits = 8
parity = "none"
stop_bits = 1
flow_control = "none"
timeout_ms = 1000
bus_silence_ms = 35
[[devices]]
id = "rrg"
driver = "rrg12"
port = "bus"
[devices.options]
address = 3
[devices.poll_periods_ms]
flow_percent = 60000
[[devices]]
id = "meta"
driver = "metakon"
port = "bus"
[devices.options]
address = 10
channel = 0
process_value_scale = 1.0
write_enabled = true
[devices.poll_periods_ms]
measurement = 60000
output_power = 60000
[[devices]]
id = "vac"
driver = "erstevak"
port = "bus"
[devices.options]
address = 1
[devices.poll_periods_ms]
pressure = 60000
"#,
    )
    .unwrap();
    let mut ack = Exchange::new(
        metakon_frame(&[10, 0, 2, 1, 4, 123, 0]),
        metakon_frame(&[10, 0, 2, 1]),
    );
    // The final ACK byte arrives after the guard measured from the request
    // or the first ACK byte would have expired.
    ack.chunk_size = 1;
    ack.chunk_delay = Duration::from_millis(12);
    let mut exchanges = VecDeque::from([
        Exchange::new(rtu_read(3, 3, 5), rtu_frame(&[3, 3, 2, 0, 100])),
        Exchange::new(
            metakon_frame(&[10, 0, 1, 0]),
            metakon_frame(&[10, 0, 1, 0, 0x44, 42, 0]),
        ),
        Exchange::new(b"001M^\r", b"001M982122V\r"),
        Exchange::new(
            metakon_frame(&[10, 0, 6, 0]),
            metakon_frame(&[10, 0, 6, 0, 0xc2, 15]),
        ),
        Exchange::new(rtu_read(3, 3, 1), rtu_frame(&[3, 3, 2, 0, 90])),
        ack,
        Exchange::new(
            metakon_frame(&[10, 0, 2, 0]),
            metakon_frame(&[10, 0, 2, 0, 0xc4, 123, 0]),
        ),
        Exchange::new(b"001M^\r", b"001M982122V\r"),
    ]);
    for exchange in exchanges.iter_mut().skip(1) {
        exchange.minimum_idle = Duration::from_millis(35);
    }
    let script = Arc::new(Mutex::new(exchanges));
    let mut opened = 0;
    let server = start_with(&config, create_driver, |port, delay| {
        opened += 1;
        assert_eq!(delay, Duration::ZERO);
        assert_eq!(
            (port.baud, port.data_bits, port.parity, port.stop_bits),
            (9600, 8, Parity::None, 1)
        );
        Ok(Bus::new(script.clone()))
    })
    .unwrap();
    assert_eq!(opened, 1);
    let registry = server.backend().registry();
    assert_eq!(
        registry.parameter("rrg/flow_percent").unwrap().port_id,
        "bus"
    );
    assert_eq!(registry.parameter("vac/pressure").unwrap().port_id, "bus");
    assert_eq!(
        registry.parameter("meta/measurement").unwrap().port_id,
        "bus"
    );
    let handle = registry.port("bus").unwrap().handle().unwrap();
    let limit = Instant::now() + Duration::from_secs(5);
    while script.lock().unwrap().len() > 4 {
        assert!(Instant::now() < limit, "initial polls did not run");
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(read(handle, "rrg", "serial_number"), Ok(Value::Integer(90)));

    let mut session = Session::new(8, 4096);
    server
        .backend()
        .receive_message(
            &mut session,
            r#"{"id":1,"op":"set","key":"meta/setpoint","value":123}"#,
            Instant::now(),
        )
        .unwrap();
    let result = loop {
        session.collect_ready(Instant::now()).unwrap();
        if let Some(message) = session.next_message().unwrap() {
            break serde_json::from_str::<serde_json::Value>(&message).unwrap();
        }
        assert!(Instant::now() < limit, "set and readback did not complete");
        thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(result["status"], "ok", "{result}");
    assert_eq!(read(handle, "vac", "pressure"), Ok(Value::Float(982.1)));
    assert!(script.lock().unwrap().is_empty());
}

struct FutureModbusDevice;

impl Driver for FutureModbusDevice {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "raw".into(),
            spec: Spec::Integer { min: 0, max: 65535 },
            unit: String::new(),
            writable: false,
            default_poll_period: None,
        }]
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        assert_eq!(parameter, "raw");
        let timing = interframe_silence(9600, 8, false, 1)?;
        let registers =
            ReadRegisters::new(42, RegisterTable::Input, 123, 1)?.exchange(io, timing)?;
        Ok(Value::Integer(i64::from(registers[0])))
    }

    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        panic!("read-only fixture");
    }
}

#[test]
fn real_drivers_and_two_logical_modbus_devices_share_one_worker() {
    let mut devices = vec![
        instance(&config("rrg", "rrg12", "address = 3")),
        instance(&config("vac", "erstevak", "address = 1")),
        DeviceInstance::new("future", FutureModbusDevice).unwrap(),
    ];
    for address in [10, 11, 12] {
        devices.push(instance(&config(
            &format!("meta{address}"),
            "metakon",
            &format!("address = {address}\nchannel = 0\nprocess_value_scale = 1.0"),
        )));
    }

    let mut script = VecDeque::new();
    for address in [10, 11, 12] {
        let mut flow = Exchange::new(rtu_read(3, 3, 5), rtu_frame(&[3, 3, 2, 0x80, address]));
        flow.chunk_size = usize::from(address - 9);
        flow.minimum_idle = Duration::from_millis(20);
        script.push_back(flow);

        let mut meta = Exchange::new(
            metakon_frame(&[address, 0, 1, 0]),
            metakon_frame(&[address, 0, 1, 0, 0x44, address, 0]),
        );
        meta.minimum_idle = Duration::from_millis(20);
        script.push_back(meta);

        let mut vacuum = Exchange::new(b"001M^\r", b"001M982122V\r");
        vacuum.chunk_size = 2;
        vacuum.minimum_idle = Duration::from_millis(9);
        script.push_back(vacuum);

        let mut future = Exchange::new(rtu_read(42, 4, 123), rtu_frame(&[42, 4, 2, 2, address]));
        future.minimum_idle = Duration::from_millis(9);
        script.push_back(future);
    }
    let script = Arc::new(Mutex::new(script));
    let (worker, channels) = PortWorker::new(Bus::new(script.clone()), devices, 8, 8).unwrap();
    let handle = &channels.commands;
    let mut registry = DeviceRegistry::new();
    registry.register_port("bus", handle.clone()).unwrap();
    for device in worker.devices() {
        registry.register_device("bus", device).unwrap();
    }
    assert_eq!(registry.devices().count(), 6);
    assert_eq!(
        registry.parameter("vac/pressure").unwrap().description.unit,
        "mbar"
    );
    assert!(
        !registry
            .parameter("rrg/setpoint_percent")
            .unwrap()
            .description
            .writable
    );
    assert!(registry.device("rrg").unwrap().actions.is_empty());

    let run = thread::spawn(move || worker.run());
    for address in [10_u8, 11, 12] {
        assert_eq!(
            read(handle, "rrg", "flow_percent"),
            Ok(Value::Float(-f64::from(address) / 100.0))
        );
        assert_eq!(
            read(handle, &format!("meta{address}"), "measurement"),
            Ok(Value::Integer(i64::from(address)))
        );
        assert_eq!(read(handle, "vac", "pressure"), Ok(Value::Float(982.1)));
        assert_eq!(
            read(handle, "future", "raw"),
            Ok(Value::Integer(512 + i64::from(address)))
        );
    }
    assert!(script.lock().unwrap().is_empty());
    handle.shutdown();
    assert_eq!(run.join().unwrap(), StopReason::Shutdown);
}

#[test]
fn rrg_catalog_decodes_vendor_registers_and_refuses_writes_before_io() {
    for options in [
        "address = 0",
        "address = 255",
        "address = 3\nwrite_enabled = true",
    ] {
        assert!(create_driver(&config("rrg", "rrg12", options)).is_err());
    }
    let cases = [
        ("network_address", 0xab03, Value::Integer(3)),
        ("serial_number", 90, Value::Integer(90)),
        ("status_flags_1", 0x100, Value::Integer(0x100)),
        ("status_flags_2", 0x40, Value::Integer(0x40)),
        ("setpoint_percent", 9, Value::Float(0.09)),
        ("flow_percent", 0x8004, Value::Float(-0.04)),
        ("baud_code", 0xff, Value::Integer(0xff)),
    ];
    let script = cases
        .iter()
        .enumerate()
        .map(|(register, (_, raw, _))| {
            let [hi, lo] = u16::to_be_bytes(*raw);
            Exchange::new(
                rtu_read(3, 3, register as u16),
                rtu_frame(&[3, 3, 2, hi, lo]),
            )
        })
        .collect();
    let script = Arc::new(Mutex::new(script));
    let mut bus = Bus::new(script.clone());
    let mut rrg = create_driver(&config(
        "rrg",
        "rrg12",
        "address = 3\nwrite_enabled = false",
    ))
    .unwrap()
    .driver;
    assert_eq!(
        rrg.write("setpoint_percent", &Value::Float(1.0), &mut bus)
            .unwrap_err()
            .port_state,
        PortState::Synchronized
    );
    assert_eq!(
        rrg.read("missing", &mut bus).unwrap_err().port_state,
        PortState::Synchronized
    );
    for (name, _, expected) in cases {
        assert_eq!(rrg.read(name, &mut bus).unwrap(), expected);
    }
    assert!(script.lock().unwrap().is_empty());
}

#[test]
fn complete_device_errors_leave_the_bus_available_to_the_next_driver() {
    let devices = vec![
        instance(&config("rrg", "rrg12", "address = 3")),
        instance(&config("vac", "erstevak", "address = 1")),
    ];
    let script = Arc::new(Mutex::new(VecDeque::from([
        Exchange::new(rtu_read(3, 3, 5), rtu_frame(&[3, 0x83, 4])),
        Exchange::new(b"001M^\r", v1_frame(b"001M000000")),
        Exchange::new(rtu_read(3, 3, 5), rtu_frame(&[3, 3, 2, 0, 100])),
    ])));
    let (worker, channels) = PortWorker::new(Bus::new(script.clone()), devices, 8, 8).unwrap();
    let run = thread::spawn(move || worker.run());
    let handle = &channels.commands;
    assert!(
        read(handle, "rrg", "flow_percent")
            .unwrap_err()
            .contains("exception 0x04")
    );
    assert!(
        read(handle, "vac", "pressure")
            .unwrap_err()
            .contains("underrange")
    );
    assert_eq!(read(handle, "rrg", "flow_percent"), Ok(Value::Float(1.0)));
    assert_eq!(handle.port_state(), PortState::Synchronized);
    assert!(script.lock().unwrap().is_empty());
    handle.shutdown();
    assert_eq!(run.join().unwrap(), StopReason::Shutdown);
}

#[test]
fn corrupt_truncated_and_disconnected_transactions_stop_the_worker_without_retry() {
    for (driver, parameter, request, good, wrong_address) in [
        (
            "rrg12",
            "flow_percent",
            rtu_read(1, 3, 5),
            rtu_frame(&[1, 3, 2, 0, 2]),
            rtu_frame(&[2, 3, 2, 0, 2]),
        ),
        (
            "erstevak",
            "pressure",
            b"001M^\r".to_vec(),
            b"001M982122V\r".to_vec(),
            v1_frame(b"002M982122"),
        ),
    ] {
        let mut bad_checksum = good.clone();
        let checksum_byte = bad_checksum.len() - 2;
        bad_checksum[checksum_byte] ^= 1;
        let mut tail = good.clone();
        tail.push(b'X');
        let partial = good[..good.len() - 1].to_vec();
        for (response, kind, expected) in [
            (bad_checksum, None, PortState::Desynchronized),
            (wrong_address, None, PortState::Desynchronized),
            (tail, None, PortState::Desynchronized),
            (
                vec![],
                Some(io::ErrorKind::TimedOut),
                PortState::Desynchronized,
            ),
            (
                partial.clone(),
                Some(io::ErrorKind::TimedOut),
                PortState::Desynchronized,
            ),
            (partial, None, PortState::Disconnected),
            (
                vec![],
                Some(io::ErrorKind::BrokenPipe),
                PortState::Disconnected,
            ),
        ] {
            let mut exchange = Exchange::new(request.clone(), response);
            exchange.read_error = kind;
            let script = Arc::new(Mutex::new(VecDeque::from([exchange])));
            let device = instance(&config("device", driver, "address = 1"));
            let (worker, channels) =
                PortWorker::new(Bus::new(script.clone()), vec![device], 8, 8).unwrap();
            let run = thread::spawn(move || worker.run());
            let handle = &channels.commands;
            assert!(read(handle, "device", parameter).is_err());
            assert!(
                matches!(run.join().unwrap(), StopReason::Driver(e) if e.port_state == expected)
            );
            assert_eq!(handle.port_state(), expected);
            assert!(
                handle
                    .read(
                        2,
                        "device",
                        parameter,
                        Instant::now() + Duration::from_secs(1)
                    )
                    .is_err()
            );
            assert!(script.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn cross_protocol_crc_overlap_is_possible_for_metakon_reads() {
    let rtu_crc_matches = |frame: &[u8]| {
        let (body, tail) = frame.split_at(frame.len() - 2);
        crc16(body).to_le_bytes() == tail
    };
    let mut overlaps = 0;
    for address in 0..=255 {
        for channel in 0..=2 {
            for register in 0..=14 {
                let frame = metakon_frame(&[address, channel, register, 0]);
                if rtu_crc_matches(&frame) {
                    assert_eq!(channel, 2);
                    assert_eq!(frame.len(), 5); // FC02 requires 8 bytes as a request.
                    overlaps += 1;
                }
            }
        }
    }
    assert_eq!(overlaps, 4);
    for address in 1..=999 {
        assert!(!rtu_crc_matches(&v1_frame(
            format!("{address:03}M").as_bytes()
        )));
    }
}

#[test]
fn misaligned_rtu_flow_replies_can_match_metakon_read_frames() {
    let metakon_read = |frame: &[u8]| {
        frame[1] <= 2 && frame[2] <= 14 && frame[3] == 0 && metakon_crc(&frame[..4]) == frame[4]
    };
    let mut overlaps = 0;
    for address in 1..=247 {
        for register in 0..=6 {
            assert!(!rtu_read(address, 3, register).windows(5).any(metakon_read));
        }
        // Normal seven-byte FC03 replies can yield five-byte windows at offsets
        // 0, 1 or 2. Offset 0 has a channel byte of 3. Negative flow encodings
        // have a high data byte >= 128, ruling out offsets 1 and 2.
        for raw in 0_u16..=13_000 {
            let [high, low] = raw.to_be_bytes();
            if !(high <= 14 && low == 0 || high <= 2 && low <= 14) {
                continue;
            }
            let reply = rtu_frame(&[address, 3, 2, high, low]);
            assert!(!metakon_read(&reply[..5]));
            for frame in reply.windows(5).filter(|frame| metakon_read(frame)) {
                assert!(matches!(frame[0], 2 | 3));
                overlaps += 1;
            }
        }
    }
    assert_eq!(overlaps, 28);
    for address in 1..=999 {
        let request = v1_frame(format!("{address:03}M").as_bytes());
        assert!(!request.windows(5).any(metakon_read));
    }
}
