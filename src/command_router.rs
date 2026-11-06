//! Direct command admission to existing bounded port queues. No I/O, waiting
//! threads, additional queues, retries or observation aggregation.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::device_registry::{Availability, DeviceRegistry, RegisteredParameter};
use crate::model::Value;
use crate::pending::Pending;
use crate::port_worker::{CommandError, DeviceObservation, PortHandle, StopReason, SubmitError};

/// Admission errors only. Accepted operations retain the PortWorker result and
/// Pending completion types, including both forms of outcome_unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    UnknownSignal(String),
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
        self.handle(parameter)?
            .read(
                id,
                &parameter.device_id,
                &parameter.description.id,
                deadline,
            )
            .map_err(|error| Self::submit_error(parameter, error))
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
        self.handle(parameter)?
            .set(
                id,
                &parameter.device_id,
                &parameter.description.id,
                value,
                deadline,
            )
            .map_err(|error| Self::submit_error(parameter, error))
    }

    fn handle(&self, parameter: &RegisteredParameter) -> Result<&PortHandle, RouteError> {
        let port = self
            .registry
            .port(&parameter.port_id)
            .expect("registered device must reference a registered port");
        // Let PortHandle check stop_reason atomically with queue admission. A
        // separate availability check would race with shutdown or driver failure.
        port.handle().ok_or_else(|| {
            let Availability::Unavailable { reason } = port.availability() else {
                unreachable!("a registered port without a handle is unavailable")
            };
            RouteError::Unavailable {
                device_id: parameter.device_id.clone(),
                port_id: parameter.port_id.clone(),
                reason,
            }
        })
    }

    fn submit_error(parameter: &RegisteredParameter, error: SubmitError) -> RouteError {
        match error {
            SubmitError::Full => RouteError::QueueFull {
                port_id: parameter.port_id.clone(),
            },
            SubmitError::Stopped(reason) => RouteError::Stopped {
                device_id: parameter.device_id.clone(),
                port_id: parameter.port_id.clone(),
                reason,
            },
        }
    }
}
