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
use crate::observation_aggregator::{AggregatorEvent, ObservationAggregator};
use crate::port_worker::{
    COMMAND_QUEUE_CAPACITY, DeviceInstance, DeviceObservation, OBSERVATION_QUEUE_CAPACITY,
    PortHandle, PortWorker, StopReason,
};
use crate::recorder::{
    DataError, Recorder, RecorderHandle, RecorderOptions, RecorderState, SubmitError,
};
use crate::tcp::MultiDeviceTcp;

type StartupResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const IDLE_WAIT: Duration = Duration::from_millis(5);

/// Configuration is mandatory; the former implicit COM3/TCP-only startup is gone.
pub fn parse_config_path(args: impl IntoIterator<Item = OsString>) -> io::Result<PathBuf> {
    let mut args = args.into_iter();
    match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(path), None) if flag == "--config" && !path.is_empty() => Ok(path.into()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: lab-server --config <path.toml>",
        )),
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
        let setup = factory(device).map_err(invalid)?;
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
    let websocket = config.websocket.as_ref().filter(|ws| ws.enabled);
    let websocket_listener = websocket
        .map(|ws| -> io::Result<TcpListener> {
            let listener = TcpListener::bind(ws.bind.expect("validated WebSocket bind"))?;
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .transpose()?;
    let websocket_origins =
        Arc::new(websocket.map_or_else(Vec::new, |ws| ws.allowed_origins.clone()));
    // Open and validate SQLite before touching physical ports. An enabled
    // recorder is mandatory; construction failure also releases the listener.
    let mut recording = Recording::open(config)?;
    let prepared_server = (|| -> StartupResult<_> {
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
        Ok((tcp, sources, workers))
    })();
    match prepared_server {
        Ok((tcp, sources, workers)) => Ok(StartedServer {
            workers,
            listener,
            websocket_listener,
            websocket_origins,
            tcp,
            sources,
            recording,
        }),
        Err(error) => {
            let mut errors = ServerErrors(vec![error]);
            recording.finish(&mut errors);
            Err(Box::new(errors))
        }
    }
}

#[derive(Default)]
struct Workers(Vec<(PortHandle, JoinHandle<StopReason>)>);

impl Workers {
    fn request_shutdown(&self) {
        for (handle, _) in &self.0 {
            handle.shutdown();
        }
    }

    fn join(&mut self) -> io::Result<()> {
        let mut panicked = false;
        for (_, join) in self.0.drain(..) {
            panicked |= join.join().is_err();
        }
        if panicked {
            Err(io::Error::other("port worker thread panicked"))
        } else {
            Ok(())
        }
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.request_shutdown();
        let _ = self.join();
    }
}

struct Client {
    socket: TcpStream,
    websocket_stop: Option<Arc<AtomicBool>>,
    join: JoinHandle<()>,
}

#[derive(Default)]
struct Clients(Vec<Client>);

impl Clients {
    fn request_close(&self) {
        for client in &self.0 {
            if let Some(stop) = &client.websocket_stop {
                stop.store(true, Ordering::Release);
            } else {
                let _ = client.socket.shutdown(Shutdown::Both);
            }
        }
    }

    fn is_finished(&self) -> bool {
        self.0.iter().all(|client| client.join.is_finished())
    }
}

impl Drop for Clients {
    fn drop(&mut self) {
        self.request_close();
        for client in self.0.drain(..) {
            let _ = client.join.join();
        }
    }
}

pub struct StartedServer {
    // Fallback Drop must join workers before dropping their receivers.
    workers: Workers,
    listener: TcpListener,
    websocket_listener: Option<TcpListener>,
    websocket_origins: Arc<Vec<String>>,
    tcp: Arc<MultiDeviceTcp>,
    sources: Vec<(String, mpsc::Receiver<DeviceObservation>)>,
    recording: Recording,
}

impl StartedServer {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn websocket_addr(&self) -> io::Result<Option<SocketAddr>> {
        self.websocket_listener
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()
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
        let tcp = self.tcp.clone();
        let mut aggregator =
            ObservationAggregator::new(tcp.registry(), std::mem::take(&mut self.sources))
                .map_err(io::Error::other)?;
        let slots = ClientSlots::new();
        let mut clients = Clients::default();
        let result = (|| -> io::Result<()> {
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
                // Bound EACH listener's accepts; one shared permit covers TCP,
                // WS and unfinished upgrades. Handshake never runs in this loop.
                for (listener, websocket) in std::iter::once((&self.listener, false)).chain(
                    self.websocket_listener
                        .as_ref()
                        .map(|listener| (listener, true)),
                ) {
                    for _ in 0..MAX_CLIENTS {
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        let stream = match listener.accept() {
                            Ok((stream, _)) => stream,
                            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            Err(error) => return Err(error),
                        };
                        let Some(slot) = slots.try_acquire() else {
                            continue;
                        };
                        let socket = stream.try_clone()?;
                        let service = self.tcp.clone();
                        let origins = self.websocket_origins.clone();
                        let websocket_stop = websocket.then(|| Arc::new(AtomicBool::new(false)));
                        let client_stop = websocket_stop.clone();
                        let join = thread::Builder::new()
                            .name(if websocket { "ws-client" } else { "tcp-client" }.into())
                            .spawn(move || {
                                let _slot = slot;
                                let result = match client_stop {
                                    Some(stop) => crate::websocket::serve_connection(
                                        stream, &service, &origins, &stop,
                                    ),
                                    None => service.serve_connection(stream),
                                };
                                if let Err(error) = result {
                                    eprintln!(
                                        "{} client error: {error}",
                                        if websocket { "WS" } else { "TCP" }
                                    );
                                }
                            })?;
                        clients.0.push(Client {
                            socket,
                            websocket_stop,
                            join,
                        });
                    }
                }
                if dispatch(&tcp, &mut aggregator, &mut self.recording)? == 0 {
                    // Also sleeps when no ports opened, or every receiver reached EOF.
                    thread::sleep(IDLE_WAIT);
                }
            }
            Ok(())
        })();
        let mut errors = ServerErrors::default();
        errors.capture(result);
        // No more accepts. WS closing is bounded but may take seconds. Keep
        // draining the SAME aggregator while clients finish, so this wait cannot
        // cause upstream overflow. Workers are stopped only after clients end.
        clients.request_close();
        let mut drain_error = None;
        while !clients.is_finished() {
            match dispatch(&tcp, &mut aggregator, &mut self.recording) {
                Ok(0) => thread::sleep(IDLE_WAIT),
                Ok(_) => {}
                Err(error) => {
                    drain_error.get_or_insert(error);
                }
            }
        }
        drop(clients);
        self.workers.request_shutdown();
        // Keep freeing queue space while drivers finish their current reads.
        // EOF includes the final counters and terminal event of each worker;
        // only then is it safe to wait for thread completion without draining.
        while !aggregator.is_finished() {
            match dispatch(&tcp, &mut aggregator, &mut self.recording) {
                Ok(0) => thread::sleep(IDLE_WAIT),
                Ok(_) => {}
                Err(error) => {
                    drain_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = drain_error {
            errors.capture(Err(error));
        }
        errors.capture(self.workers.join());
        self.recording.finish(&mut errors);
        errors.result()
    }
}

/// Poll once and fan out the same FIFO events, preserving the local source for
/// SQLite before TCP replaces it with the global key. Finish the whole batch
/// even if TCP fails, so events already removed from the aggregator aren't lost.
fn dispatch(
    tcp: &MultiDeviceTcp,
    aggregator: &mut ObservationAggregator<'_>,
    recording: &mut Recording,
) -> io::Result<usize> {
    let events = aggregator.poll(64);
    let count = events.len();
    let mut first_error = None;
    for event in events {
        recording.record(&event);
        if let Err(error) = tcp.apply_event(event) {
            first_error.get_or_insert(error);
        }
    }
    recording.check_state();
    first_error.map_or(Ok(count), Err)
}

#[derive(Default)]
struct Recording {
    recorder: Option<Recorder>,
    handle: Option<RecorderHandle>,
    invalid: Option<DataError>,
    terminal: Option<SubmitError>,
    overflow_warned: bool,
    // Observe the actual diagnostic path without redirecting process stderr
    // (which would interfere with concurrently running tests).
    #[cfg(test)]
    diagnostics: Vec<String>,
}
impl Recording {
    fn open(config: &ServerConfig) -> StartupResult<Self> {
        let Some(options) = config.recording.as_ref().filter(|r| r.enabled) else {
            return Ok(Self::default());
        };
        let recorder = Recorder::open(
            options.path.as_ref().expect("validated recording path"),
            options
                .queue_capacity
                .unwrap_or_else(|| RecorderOptions::default().queue_capacity),
        )?;
        Ok(Self {
            handle: Some(recorder.handle()),
            recorder: Some(recorder),
            ..Self::default()
        })
    }
    fn record(&mut self, event: &AggregatorEvent) {
        if let Some(handle) = &self.handle {
            let result = handle.try_record(event.clone());
            self.submitted(result);
        }
    }
    fn submitted(&mut self, result: Result<(), SubmitError>) {
        match result {
            Ok(()) => {}
            Err(SubmitError::Full) => {
                if !self.overflow_warned {
                    self.report(format_args!("Warning: recorder queue is full; recording is incomplete; polling and TCP continue"));
                    self.overflow_warned = true;
                }
                // Recorder alone accounts overflow; this flag only limits logs.
            }
            Err(SubmitError::InvalidData(error)) => {
                if self.invalid.is_none() {
                    self.report(format_args!("Recording incomplete: invalid data: {error}"));
                    self.invalid = Some(error);
                }
            }
            Err(error @ (SubmitError::Closed | SubmitError::Failed(_))) => {
                if self.terminal.is_none() {
                    self.report(format_args!("Recording unavailable: {error}"));
                    self.terminal = Some(error);
                }
            }
        }
    }
    fn report(&mut self, message: std::fmt::Arguments<'_>) {
        eprintln!("{message}");
        #[cfg(test)]
        self.diagnostics.push(message.to_string());
    }
    fn check_state(&mut self) {
        // A writer can fail after its last submission, including an idle server.
        if let Some(handle) = &self.handle
            && let RecorderState::Failed(error) = handle.state()
        {
            self.submitted(Err(SubmitError::Failed(error)));
        }
    }
    fn finish(&mut self, errors: &mut ServerErrors) {
        self.check_state();
        if let Some(error) = self.invalid.take() {
            errors.capture(Err(io::Error::other(format!(
                "recording incomplete: invalid data: {error}"
            ))));
        }
        if let Some(error) = self.terminal.take() {
            errors.capture(Err(io::Error::other(error)));
        }
        if let Some(recorder) = self.recorder.take() {
            match recorder.shutdown() {
                Ok(summary) => self.report(format_args!(
                    "Recording summary: session_id={} accepted_records={} dropped_events={} dropped_data_lost_events={} rejected_invalid_data={} incomplete={}",
                    summary.session_id,
                    summary.accepted_records,
                    summary.losses.dropped_events,
                    summary.losses.dropped_data_lost_events,
                    summary.losses.rejected_invalid_data,
                    summary.losses.is_incomplete(),
                )),
                Err(error) => errors.capture(Err(io::Error::other(error))),
            }
        }
    }
}

/// Keep the original cause and every distinct cleanup stage's failure.
#[derive(Debug, Default)]
struct ServerErrors(Vec<Box<dyn std::error::Error + Send + Sync>>);
impl ServerErrors {
    fn capture(&mut self, result: io::Result<()>) {
        if let Err(error) = result {
            self.0.push(Box::new(error));
        }
    }
    fn result(self) -> io::Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(self))
        }
    }
}
impl std::fmt::Display for ServerErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; additionally: ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for ServerErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| &**error as _)
    }
}

#[cfg(test)]
mod tests;
