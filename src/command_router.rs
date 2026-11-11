//! Direct command admission to existing bounded port queues. No I/O, waiting
//! threads, additional queues, retries or observation aggregation.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::device_registry::{Availability, DeviceRegistry, RegisteredAction, RegisteredParameter};
use crate::model::Value;
use crate::pending::Pending;
use crate::port_worker::{CommandError, DeviceObservation, PortHandle, StopReason, SubmitError};

/// Admission errors only. Accepted operations retain the PortWorker result and
/// Pending completion types, including both forms of outcome_unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    UnknownSignal(String),
    UnknownAction(String),
    UnknownDevice(String),
    Unavailable {
        device_id: String,
        port_id: String,
        reason: String,
    },
    Stopped {
        device_id: String,
        port_id: String,
        reason: StopReason,
    },
    QueueFull {
        port_id: String,
    },
    ReadOnly {
        global_key: String,
    },
    InvalidValue {
        global_key: String,
    },
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownAction(key) => write!(f, "unknown global action {key:?}"),
            Self::UnknownSignal(key) => write!(f, "unknown global signal {key:?}"),
            Self::UnknownDevice(id) => write!(f, "unknown device {id:?}"),
            Self::Unavailable {
                device_id,
                port_id,
                reason,
            } => {
                write!(
                    f,
                    "device {device_id:?}, port {port_id:?}: unavailable: {reason}"
                )
            }
            Self::Stopped {
                device_id,
                port_id,
                reason,
            } => {
                write!(
                    f,
                    "device {device_id:?}, port {port_id:?}: stopped: {reason:?}"
                )
            }
            Self::QueueFull { port_id } => write!(f, "port {port_id:?}: command queue is full"),
            Self::ReadOnly { global_key } => write!(f, "signal {global_key:?}: read-only"),
            Self::InvalidValue { global_key } => {
                write!(f, "signal {global_key:?}: invalid type or value")
            }
        }
    }
}
impl std::error::Error for RouteError {}

/// Owns a frozen catalog shared by cheap router clones. Worker status remains
/// live through PortHandle; registry and router do not own worker threads.
#[derive(Clone)]
pub struct CommandRouter {
    registry: Arc<DeviceRegistry>,
}

impl CommandRouter {
    pub fn new(registry: DeviceRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    pub fn registry(&self) -> &DeviceRegistry {
        &self.registry
    }

    pub fn description(&self, global_key: &str) -> Result<&RegisteredParameter, RouteError> {
        self.registry
            .parameter(global_key)
            .ok_or_else(|| RouteError::UnknownSignal(global_key.into()))
    }

    pub fn availability(&self, device_id: &str) -> Result<Availability, RouteError> {
        self.registry
            .availability(device_id)
            .ok_or_else(|| RouteError::UnknownDevice(device_id.into()))
    }

    pub fn read(
        &self,
        id: u64,
        global_key: &str,
        deadline: Instant,
    ) -> Result<Pending<Result<DeviceObservation, CommandError>>, RouteError> {
        let parameter = self.description(global_key)?;
        self.handle(&parameter.device_id, &parameter.port_id)?
            .read(
                id,
                &parameter.device_id,
                &parameter.description.id,
                deadline,
            )
            .map_err(|error| Self::submit_error(&parameter.device_id, &parameter.port_id, error))
    }

    /// Validation happens before queue admission; the worker validates again.
    /// The original Pending/SetExecution is returned without a wrapper. Polling
    /// it handles timeout versus start atomically; merely dropping it does not
    /// cancel a queued write. Request ids and deadlines are passed unchanged.
    pub fn set(
        &self,
        id: u64,
        global_key: &str,
        value: Value,
        deadline: Instant,
    ) -> Result<Pending<Result<(), CommandError>>, RouteError> {
        let parameter = self.description(global_key)?;
        if !parameter.description.writable {
            return Err(RouteError::ReadOnly {
                global_key: global_key.into(),
            });
        }
        if !parameter.description.spec.accepts(&value) {
            return Err(RouteError::InvalidValue {
                global_key: global_key.into(),
            });
        }
        self.handle(&parameter.device_id, &parameter.port_id)?
            .set(
                id,
                &parameter.device_id,
                &parameter.description.id,
                value,
                deadline,
            )
            .map_err(|error| Self::submit_error(&parameter.device_id, &parameter.port_id, error))
    }

    pub fn action_description(&self, global_key: &str) -> Result<&RegisteredAction, RouteError> {
        let device_id = global_key.split_once('/').map_or(global_key, |(id, _)| id);
        if self.registry.device(device_id).is_none() {
            return Err(RouteError::UnknownDevice(device_id.into()));
        }
        self.registry
            .action(global_key)
            .ok_or_else(|| RouteError::UnknownAction(global_key.into()))
    }

    pub fn invoke_action(
        &self,
        id: u64,
        global_key: &str,
        deadline: Instant,
    ) -> Result<Pending<Result<(), CommandError>>, RouteError> {
        let action = self.action_description(global_key)?;
        self.handle(&action.device_id, &action.port_id)?
            .invoke_action(id, &action.device_id, &action.description.name, deadline)
            .map_err(|error| Self::submit_error(&action.device_id, &action.port_id, error))
    }

    fn handle(&self, device_id: &str, port_id: &str) -> Result<&PortHandle, RouteError> {
        let port = self
            .registry
            .port(port_id)
            .expect("registered device must reference a registered port");
        // Let PortHandle check stop_reason atomically with queue admission. A
        // separate availability check would race with shutdown or driver failure.
        port.handle().ok_or_else(|| {
            let Availability::Unavailable { reason } = port.availability() else {
                unreachable!("a registered port without a handle is unavailable")
            };
            RouteError::Unavailable {
                device_id: device_id.into(),
                port_id: port_id.into(),
                reason,
            }
        })
    }

    fn submit_error(device_id: &str, port_id: &str, error: SubmitError) -> RouteError {
        match error {
            SubmitError::Full => RouteError::QueueFull {
                port_id: port_id.into(),
            },
            SubmitError::Stopped(reason) => RouteError::Stopped {
                device_id: device_id.into(),
                port_id: port_id.into(),
                reason,
            },
        }
    }
}
