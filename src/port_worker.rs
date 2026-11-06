//! One owner of a physical stream, with independent driver instances.
//!
//! Callers supply an already configured, synchronized DeviceIo. The worker never
//! opens, clears or reconnects it. Driver calls are synchronous and indivisible;
//! shutdown waits for the current call to return. A driver must bound its I/O.

use std::collections::HashSet;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime};

use crate::driver::{DeviceIo, Driver, DriverError, PortState};
use crate::model::{Observation, ParameterDescription, Value};
use crate::pending::{Pending, SetExecution};

mod scheduler;
use scheduler::{Scheduler, Task};

pub struct DeviceInstance {
    id: String,
    driver: Box<dyn Driver + Send>,
    parameters: Vec<ParameterDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildError(pub String);

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for BuildError {}

impl DeviceInstance {
    pub fn new(
        id: impl Into<String>,
        driver: impl Driver + Send + 'static,
    ) -> Result<Self, BuildError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(BuildError("device id must not be empty".into()));
        }
        let parameters = driver.parameters();
        let mut names = HashSet::new();
        for parameter in &parameters {
            if parameter.id.trim().is_empty()
                || !names.insert(&parameter.id)
                || !parameter.spec.valid()
            {
                return Err(BuildError(format!(
                    "device {id:?}: invalid or duplicate parameter {:?}",
                    parameter.id
                )));
            }
            validate_period(parameter.default_poll_period)?;
        }
        Ok(Self {
            id,
            driver: Box::new(driver),
            parameters,
        })
    }

    /// Configure local polling without coupling this core to TOML or global keys.
    pub fn set_poll_period(
        &mut self,
        parameter: &str,
        period: Option<Duration>,
    ) -> Result<(), BuildError> {
        validate_period(period)?;
        let description = self
            .parameters
            .iter_mut()
            .find(|p| p.id == parameter)
            .ok_or_else(|| {
                BuildError(format!(
                    "device {:?}: unknown parameter {parameter:?}",
                    self.id
                ))
            })?;
        description.default_poll_period = period;
        Ok(())
    }
}

fn validate_period(period: Option<Duration>) -> Result<(), BuildError> {
    if period.is_some_and(|period| period.is_zero() || Instant::now().checked_add(period).is_none())
    {
        return Err(BuildError(
            "poll period must be positive and representable by Instant".into(),
        ));
    }
    Ok(())
}

/// Keep device identity separate; Observation.source remains the local parameter.
#[derive(Debug, Clone)]
pub struct DeviceObservation {
    pub device_id: String,
    pub observation: Observation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    Shutdown,
    NoCommandSenders,
    Driver(DriverError),
    ObservationReceiverDropped,
    ScheduleOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    UnknownDevice,
    UnknownParameter,
    ReadOnly,
    InvalidValue,
    RuntimeTimeout,
    OutcomeUnknown,
    PortStopped(StopReason),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDevice => f.write_str("unknown_device"),
            Self::UnknownParameter => f.write_str("unknown_parameter"),
            Self::ReadOnly => f.write_str("Parameter is read-only"),
            Self::InvalidValue => f.write_str("Value does not match parameter spec"),
            Self::RuntimeTimeout => f.write_str("runtime_timeout"),
            Self::OutcomeUnknown => f.write_str("outcome_unknown"),
            Self::PortStopped(reason) => write!(f, "port stopped: {reason:?}"),
        }
    }
}
impl std::error::Error for CommandError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
    Full,
    Stopped(StopReason),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("port command queue is full"),
            Self::Stopped(reason) => write!(f, "port stopped: {reason:?}"),
        }
    }
}
impl std::error::Error for SubmitError {}

enum Command {
    Read {
        device: String,
        parameter: String,
        deadline: Instant,
        reply: mpsc::SyncSender<Result<DeviceObservation, CommandError>>,
    },
    Set {
        device: String,
        parameter: String,
        value: Value,
        execution: Arc<SetExecution>,
        reply: mpsc::SyncSender<Result<(), CommandError>>,
    },
    Wake,
}

impl Command {
    fn reject(self, reason: &StopReason) {
        let error = CommandError::PortStopped(reason.clone());
        match self {
            Self::Read { reply, .. } => {
                let _ = reply.try_send(Err(error));
            }
            Self::Set {
                reply, execution, ..
            } => execution.complete(|| {
                let _ = reply.try_send(Err(error));
            }),
            Self::Wake => {}
        }
    }
}

#[derive(Clone)]
pub struct PortHandle {
    sender: mpsc::SyncSender<Command>,
    // Admission and closing share a lock: after close the drain has a finite end.
    stopped: Arc<Mutex<Option<StopReason>>>,
    loss_epoch: Arc<AtomicU64>,
}

impl PortHandle {
    fn submit(&self, command: Command) -> Result<(), SubmitError> {
        let stopped = self.stopped.lock().expect("port control lock poisoned");
        if let Some(reason) = stopped.as_ref() {
            return Err(SubmitError::Stopped(reason.clone()));
        }
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::TrySendError::Full(_) => SubmitError::Full,
            mpsc::TrySendError::Disconnected(_) => SubmitError::Stopped(StopReason::Shutdown),
        })
    }

    pub fn read(
        &self,
        id: u64,
        device: impl Into<String>,
        parameter: impl Into<String>,
        deadline: Instant,
    ) -> Result<Pending<Result<DeviceObservation, CommandError>>, SubmitError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.submit(Command::Read {
            device: device.into(),
            parameter: parameter.into(),
            deadline,
            reply,
        })?;
        Ok(Pending::new(id, deadline, receiver))
    }

    /// Use Pending::poll for the existing atomic timeout/start and single-result
    /// semantics. Dropping Pending alone is not cancellation of a queued write.
    pub fn set(
        &self,
        id: u64,
        device: impl Into<String>,
        parameter: impl Into<String>,
        value: Value,
        deadline: Instant,
    ) -> Result<Pending<Result<(), CommandError>>, SubmitError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let execution = SetExecution::new(deadline);
        self.submit(Command::Set {
            device: device.into(),
            parameter: parameter.into(),
            value,
            execution: execution.clone(),
            reply,
        })?;
        Ok(Pending::new(id, deadline, receiver).with_execution(execution))
    }

    /// Bypasses command capacity. A full queue already wakes the worker; otherwise
    /// Wake releases recv even when there are no polling tasks.
    pub fn shutdown(&self) {
        let mut stopped = self.stopped.lock().expect("port control lock poisoned");
        if stopped.is_none() {
            *stopped = Some(StopReason::Shutdown);
            let _ = self.sender.try_send(Command::Wake);
        }
    }

    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stopped
            .lock()
            .expect("port control lock poisoned")
            .clone()
    }

    /// Number of observations dropped because the bounded queue was full.
    /// Independent of queue contents and the control lock; never reset by reads.
    /// Compare with the last observed value (initially zero), including after
    /// draining the queue. A change indicates a gap, not its position among
    /// queued messages: try_send and increment are separate operations, so a gap
    /// may be noticed on a later check. u64::MAX is sticky loss/unknown continuity.
    pub fn loss_epoch(&self) -> u64 {
        self.loss_epoch.load(Ordering::SeqCst)
    }

    pub fn port_state(&self) -> PortState {
        match self.stop_reason() {
            Some(StopReason::Driver(error)) => error.port_state,
            _ => PortState::Synchronized,
        }
    }
}

pub struct PortChannels {
    pub commands: PortHandle,
    pub observations: mpsc::Receiver<DeviceObservation>,
}

pub struct PortWorker<P: DeviceIo> {
    io: P,
    devices: Vec<DeviceInstance>,
    commands: mpsc::Receiver<Command>,
    output: mpsc::SyncSender<DeviceObservation>,
    stopped: Arc<Mutex<Option<StopReason>>>,
    loss_epoch: Arc<AtomicU64>,
    scheduler: Scheduler,
}

impl<P: DeviceIo> PortWorker<P> {
    pub fn new(
        io: P,
        devices: Vec<DeviceInstance>,
        command_capacity: usize,
        observation_capacity: usize,
    ) -> Result<(Self, PortChannels), BuildError> {
        if command_capacity == 0 || observation_capacity == 0 {
            return Err(BuildError("queue capacities must be positive".into()));
        }
        let mut ids = HashSet::new();
        let mut tasks = Vec::new();
        let start = Instant::now();
        for (device, instance) in devices.iter().enumerate() {
            if !ids.insert(&instance.id) {
                return Err(BuildError(format!("duplicate device id {:?}", instance.id)));
            }
            for (parameter, description) in instance.parameters.iter().enumerate() {
                if let Some(period) = description.default_poll_period {
                    tasks.push(Task {
                        device,
                        parameter,
                        period,
                        next_at: start,
                    });
                }
            }
        }
        let scheduler = Scheduler::new(tasks, devices.len());
        let (sender, commands) = mpsc::sync_channel(command_capacity);
        let (output, observations) = mpsc::sync_channel(observation_capacity);
        let stopped = Arc::new(Mutex::new(None));
        let loss_epoch = Arc::new(AtomicU64::new(0));
        let handle = PortHandle {
            sender,
            stopped: stopped.clone(),
            loss_epoch: loss_epoch.clone(),
        };
        Ok((
            Self {
                io,
                devices,
                commands,
                output,
                stopped,
                loss_epoch,
                scheduler,
            },
            PortChannels {
                commands: handle,
                observations,
            },
        ))
    }

    /// Runs on the caller's thread. Moving the worker to a thread requires P: Send.
    pub fn run(mut self) -> StopReason {
        let mut prefer_command = true;
        loop {
            if let Some(reason) = self.stop_reason() {
                return self.finish(reason);
            }
            let due = self
                .scheduler
                .next_deadline()
                .is_some_and(|deadline| deadline <= Instant::now());
            if due && !prefer_command {
                self.poll_one();
                prefer_command = true;
                continue;
            }
            match self.commands.try_recv() {
                Ok(command) => {
                    self.execute(command);
                    prefer_command = false;
                    continue;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return self.finish(StopReason::NoCommandSenders);
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if due {
                self.poll_one();
                prefer_command = true;
                continue;
            }
            let received = match self.scheduler.next_deadline() {
                Some(deadline) => self
                    .commands
                    .recv_timeout(deadline.saturating_duration_since(Instant::now())),
                None => self
                    .commands
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            };
            match received {
                Ok(command) => {
                    self.execute(command);
                    prefer_command = false;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return self.finish(StopReason::NoCommandSenders);
                }
            }
        }
    }

    fn stop_reason(&self) -> Option<StopReason> {
        self.stopped
            .lock()
            .expect("port control lock poisoned")
            .clone()
    }

    fn finish(&mut self, reason: StopReason) -> StopReason {
        let reason = {
            let mut stopped = self.stopped.lock().expect("port control lock poisoned");
            stopped.get_or_insert(reason).clone()
        };
        for command in self.commands.try_iter() {
            command.reject(&reason);
        }
        reason
    }

    fn begin(&self, execution: Option<&SetExecution>) -> Result<(), CommandError> {
        let stopped = self.stopped.lock().expect("port control lock poisoned");
        if let Some(reason) = stopped.as_ref() {
            return Err(CommandError::PortStopped(reason.clone()));
        }
        if execution.is_some_and(|execution| !execution.try_start()) {
            return Err(CommandError::RuntimeTimeout);
        }
        Ok(())
    }

    fn locate(&self, device: &str, parameter: &str) -> Result<(usize, usize), CommandError> {
        let device = self
            .devices
            .iter()
            .position(|item| item.id == device)
            .ok_or(CommandError::UnknownDevice)?;
        let parameter = self.devices[device]
            .parameters
            .iter()
            .position(|item| item.id == parameter)
            .ok_or(CommandError::UnknownParameter)?;
        Ok((device, parameter))
    }

    fn record_error(&self, error: &DriverError) {
        if error.port_state != PortState::Synchronized {
            // A physical failure discovered during an in-flight call takes
            // precedence over a concurrently requested orderly shutdown.
            *self.stopped.lock().expect("port control lock poisoned") =
                Some(StopReason::Driver(error.clone()));
        }
    }

    fn read(&mut self, device: usize, parameter: usize) -> DeviceObservation {
        let description = &self.devices[device].parameters[parameter];
        let name = description.id.clone();
        let result = catch_unwind(AssertUnwindSafe(|| {
            self.devices[device].driver.read(&name, &mut self.io)
        }))
        .unwrap_or_else(|_| {
            Err(DriverError::new(
                "driver panicked",
                PortState::Desynchronized,
            ))
        });
        if let Err(error) = &result {
            self.record_error(error);
        }
        let description = &self.devices[device].parameters[parameter];
        let reading = result.map_err(|error| error.to_string()).and_then(|value| {
            if description.spec.accepts(&value) {
                Ok(value)
            } else {
                Err("Value out of range".into())
            }
        });
        DeviceObservation {
            device_id: self.devices[device].id.clone(),
            observation: Observation {
                source: name,
                unit: description.unit.clone(),
                completed_at: SystemTime::now(),
                reading,
            },
        }
    }

    fn execute(&mut self, command: Command) {
        match command {
            Command::Read {
                device,
                parameter,
                deadline,
                reply,
            } => {
                let result = self
                    .locate(&device, &parameter)
                    .and_then(|(device, parameter)| {
                        self.begin(None)?;
                        if Instant::now() >= deadline {
                            return Err(CommandError::RuntimeTimeout);
                        }
                        Ok(self.read(device, parameter))
                    });
                let _ = reply.try_send(result);
            }
            Command::Set {
                device,
                parameter,
                value,
                execution,
                reply,
            } => {
                let result = self
                    .locate(&device, &parameter)
                    .and_then(|(device, parameter)| {
                        let description = &self.devices[device].parameters[parameter];
                        if !description.writable {
                            return Err(CommandError::ReadOnly);
                        }
                        if !description.spec.accepts(&value) {
                            return Err(CommandError::InvalidValue);
                        }
                        let name = description.id.clone();
                        self.begin(Some(&execution))?;
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            self.devices[device]
                                .driver
                                .write(&name, &value, &mut self.io)
                        }))
                        .unwrap_or_else(|_| {
                            Err(DriverError::new(
                                "driver panicked",
                                PortState::Desynchronized,
                            ))
                        });
                        result.map_err(|error| {
                            self.record_error(&error);
                            CommandError::OutcomeUnknown
                        })
                    });
                execution.complete(|| {
                    let _ = reply.try_send(result);
                });
            }
            Command::Wake => {}
        }
    }

    fn poll_one(&mut self) {
        let Some(index) = self.scheduler.next_due(Instant::now()) else {
            return;
        };
        if self.begin(None).is_err() {
            return;
        }
        let task = &self.scheduler.tasks[index];
        let observation = self.read(task.device, task.parameter);
        let stop = match self.output.try_send(observation) {
            Ok(()) => None,
            Err(mpsc::TrySendError::Full(_)) => {
                // A slow observer must not stop device polling or control. Keep
                // loss visible out of band, including after the queue is drained.
                // Saturate instead of wrapping and making old loss look absent.
                let _ = self
                    .loss_epoch
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |epoch| {
                        Some(epoch.saturating_add(1))
                    });
                None
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Some(StopReason::ObservationReceiverDropped)
            }
        };
        if let Some(reason) = stop {
            self.finish(reason);
        }
        if self.stop_reason().is_none() && self.scheduler.completed(index, Instant::now()).is_none()
        {
            self.finish(StopReason::ScheduleOverflow);
        }
    }
}

impl<P: DeviceIo> Drop for PortWorker<P> {
    fn drop(&mut self) {
        // Also reject accepted requests if the worker is dropped before run.
        self.finish(StopReason::Shutdown);
    }
}
