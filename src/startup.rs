//! TOML composition root. Validate every driver before touching hardware, then
//! open each configured physical port once and retain catalogs on open failure.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{self, BufReader};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::client_limit::{ClientSlots, MAX_CLIENTS};
use crate::command_router::CommandRouter;
use crate::config::{ConfigError, DeviceConfig, FlowControl, Parity, PortConfig, ServerConfig};
use crate::device_registry::DeviceRegistry;
use crate::driver::{DeviceIo, Driver, arduino::Arduino};
use crate::observation_aggregator::ObservationAggregator;
use crate::port_worker::{DeviceInstance, DeviceObservation, PortHandle, PortWorker, StopReason};
use crate::runtime::{COMMAND_QUEUE_CAPACITY, OBSERVATION_QUEUE_CAPACITY};
use crate::tcp::MultiDeviceTcp;

type StartupResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const IDLE_WAIT: Duration = Duration::from_millis(5);

#[derive(Debug, PartialEq, Eq)]
pub enum LaunchMode {
    Legacy,
    Config(PathBuf),
}

impl LaunchMode {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut args = args.into_iter();
        match (args.next(), args.next(), args.next()) {
            (None, None, None) => Ok(Self::Legacy),
            (Some(flag), Some(path), None) if flag == "--config" && !path.is_empty() => {
                Ok(Self::Config(path.into()))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: lab-server [--config <path.toml>]",
            )),
        }
    }
}

/// Driver-specific preparation, independent of TCP and physical port grouping.
pub struct DriverSetup {
    pub driver: Box<dyn Driver + Send>,
    /// Wait once after opening a shared port, using the largest requested delay.
    pub post_open_delay: Duration,
}

/// Central factory. New drivers validate their own typed/nested options here;
/// adding one does not change port opening, worker or TCP lifecycle code.
pub fn create_driver(device: &DeviceConfig) -> Result<DriverSetup, String> {
    match device.driver.as_str() {
        "arduino" => {
            if !device.options.is_empty() {
                return Err(format!(
                    "arduino accepts no options; unknown options: {}",
                    device
                        .options
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            Ok(DriverSetup {
                driver: Box::new(Arduino::new()),
                post_open_delay: Duration::from_secs(2),
            })
        }
        name => Err(format!("unknown driver {name:?}")),
    }
}

fn serial_builder(port: &PortConfig) -> io::Result<serialport::SerialPortBuilder> {
    let data_bits = match port.data_bits {
        5 => serialport::DataBits::Five,
        6 => serialport::DataBits::Six,
        7 => serialport::DataBits::Seven,
        8 => serialport::DataBits::Eight,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid data_bits",
            ));
        }
    };
    let stop_bits = match port.stop_bits {
        1 => serialport::StopBits::One,
        2 => serialport::StopBits::Two,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid stop_bits",
            ));
        }
    };
    Ok(serialport::new(&port.name, port.baud)
        .data_bits(data_bits)
        .parity(match port.parity {
            Parity::None => serialport::Parity::None,
            Parity::Odd => serialport::Parity::Odd,
            Parity::Even => serialport::Parity::Even,
        })
        .stop_bits(stop_bits)
        .flow_control(match port.flow_control {
            FlowControl::None => serialport::FlowControl::None,
            FlowControl::Software => serialport::FlowControl::Software,
            FlowControl::Hardware => serialport::FlowControl::Hardware,
        })
        .timeout(Duration::from_millis(port.timeout_ms)))
}

pub fn open_serial(
    port: &PortConfig,
    delay: Duration,
) -> io::Result<BufReader<Box<dyn serialport::SerialPort>>> {
    let stream = serial_builder(port)?.open()?;
    thread::sleep(delay);
    Ok(BufReader::new(stream))
}

pub fn start(config: &ServerConfig) -> StartupResult<StartedServer> {
    start_with(config, create_driver, open_serial)
}

/// Dependency injection for hardware-free integration tests. The factory must
/// only build/validate a fresh driver; the opener configures I/O and applies the
/// requested preparation delay before returning it. No retries are performed.
pub fn start_with<P: DeviceIo + Send + 'static>(
    config: &ServerConfig,
    mut factory: impl FnMut(&DeviceConfig) -> Result<DriverSetup, String>,
    mut open: impl FnMut(&PortConfig, Duration) -> io::Result<P>,
) -> StartupResult<StartedServer> {
    config.validate()?;
    let mut grouped = HashMap::<&str, (Vec<DeviceInstance>, Duration)>::new();
    for (index, device) in config.devices.iter().enumerate() {
        let invalid = |message| ConfigError::Validation {
            field: format!("devices[{index}] ({:?})", device.id),
            message,
        };
        let setup = factory(device).map_err(&invalid)?;
        let mut instance = DeviceInstance::new_boxed(&device.id, setup.driver)
            .map_err(|error| invalid(error.to_string()))?;
        for (parameter, millis) in &device.poll_periods_ms {
            instance
                .set_poll_period(parameter, Some(Duration::from_millis(*millis)))
                .map_err(|error| invalid(format!("poll_periods_ms: {error}")))?;
        }
        let (devices, delay) = grouped.entry(&device.port).or_default();
        devices.push(instance);
        *delay = (*delay).max(setup.post_open_delay);
    }

    // Binding failure is fatal and happens before opening any equipment.
    let listener = TcpListener::bind(config.tcp.bind)?;
    listener.set_nonblocking(true)?;
    let mut registry = DeviceRegistry::new();
    let mut prepared = Vec::new();
    let mut sources = Vec::new();
    for port in &config.ports {
        let (devices, delay) = grouped.remove(port.id.as_str()).unwrap_or_default();
        match open(port, delay) {
            Ok(io) => {
                let (worker, channels) = PortWorker::new(
                    io,
                    devices,
                    COMMAND_QUEUE_CAPACITY,
                    OBSERVATION_QUEUE_CAPACITY,
                )?;
                registry.register_port(&port.id, channels.commands.clone())?;
                for device in worker.devices() {
                    registry.register_device(&port.id, device)?;
                }
                sources.push((port.id.clone(), channels.observations));
                prepared.push((port.id.clone(), channels.commands, worker));
            }
            Err(error) => {
                let reason = format!("cannot open {:?}: {error}", port.name);
                eprintln!("Port {:?} unavailable: {reason}", port.id);
                registry.register_unavailable_port(&port.id, reason)?;
                for device in &devices {
                    registry.register_device(&port.id, device)?;
                }
            }
        }
    }
    let tcp = Arc::new(MultiDeviceTcp::new(
        CommandRouter::new(registry),
        OBSERVATION_QUEUE_CAPACITY,
    ));
    let mut workers = Workers::default();
    // All ports are prepared before polling begins (including Arduino resets).
    for (id, handle, worker) in prepared {
        let join = thread::Builder::new()
            .name(format!("port-{id}"))
            .spawn(move || worker.run())?;
        workers.0.push((handle, join));
    }
    Ok(StartedServer {
        listener,
        tcp,
        sources,
        _workers: workers,
    })
}

#[derive(Default)]
struct Workers(Vec<(PortHandle, JoinHandle<StopReason>)>);

impl Drop for Workers {
    fn drop(&mut self) {
        for (handle, _) in &self.0 {
            handle.shutdown();
        }
        for (_, join) in self.0.drain(..) {
            let _ = join.join();
        }
    }
}

struct Client {
    socket: TcpStream,
    join: JoinHandle<()>,
}

#[derive(Default)]
struct Clients(Vec<Client>);

impl Drop for Clients {
    fn drop(&mut self) {
        for client in &self.0 {
            let _ = client.socket.shutdown(Shutdown::Both);
        }
        for client in self.0.drain(..) {
            let _ = client.join.join();
        }
    }
}

pub struct StartedServer {
    listener: TcpListener,
    tcp: Arc<MultiDeviceTcp>,
    sources: Vec<(String, mpsc::Receiver<DeviceObservation>)>,
    _workers: Workers,
}

impl StartedServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn backend(&self) -> &Arc<MultiDeviceTcp> {
        &self.tcp
    }

    pub fn run(self) -> io::Result<()> {
        self.run_until(&AtomicBool::new(false))
    }

    /// Own the accept/aggregation loop. Worker EOF never terminates the listener.
    /// On return sockets close and workers are stopped/joined; current driver
    /// transactions must finish within their configured I/O bounds.
    pub fn run_until(mut self, stop: &AtomicBool) -> io::Result<()> {
        let mut aggregator =
            ObservationAggregator::new(self.tcp.registry(), std::mem::take(&mut self.sources))
                .map_err(io::Error::other)?;
        let slots = ClientSlots::new();
        let mut clients = Clients::default();
        while !stop.load(Ordering::Acquire) {
            // Reap finished clients so repeated reconnects cannot retain sockets.
            let mut index = 0;
            while index < clients.0.len() {
                if clients.0[index].join.is_finished() {
                    let _ = clients.0.swap_remove(index).join.join();
                } else {
                    index += 1;
                }
            }
            // Bound accepts so connection floods cannot starve observations.
            for _ in 0..MAX_CLIENTS {
                let stream = match self.listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                };
                let Some(slot) = slots.try_acquire() else {
                    continue;
                };
                let socket = stream.try_clone()?;
                let tcp = self.tcp.clone();
                let join = thread::Builder::new()
                    .name("tcp-client".into())
                    .spawn(move || {
                        let _slot = slot;
                        if let Err(error) = tcp.serve_connection(stream) {
                            eprintln!("TCP client error: {error}");
                        }
                    })?;
                clients.0.push(Client { socket, join });
            }
            if self.tcp.collect(&mut aggregator, 64)? == 0 {
                // Also sleeps when no ports opened, or every receiver reached EOF.
                thread::sleep(IDLE_WAIT);
            }
        }
        Ok(())
    }
}
