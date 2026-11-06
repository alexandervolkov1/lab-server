//! Logical catalog independent of physical port availability.
//!
//! Configure instances first, construct the worker, then register its handle and
//! catalogs from PortWorker::devices() before starting it. Registration does not
//! start workers or inspect hardware. The caller must associate each handle with
//! its actual port and devices; a handle exposes no driver internals.

use std::collections::BTreeMap;
use std::fmt;

use crate::model::ParameterDescription;
use crate::port_worker::{DeviceInstance, DeviceObservation, PortHandle, StopReason};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    Available,
    Unavailable { reason: String },
    Stopped { reason: StopReason },
}

enum PortBinding {
    Running(PortHandle),
    Unavailable(String),
}

pub struct RegisteredPort {
    id: String,
    binding: PortBinding,
}

impl RegisteredPort {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Live status from the handle, never a second cached worker status.
    pub fn availability(&self) -> Availability {
        match &self.binding {
            PortBinding::Running(handle) => match handle.stop_reason() {
                Some(reason) => Availability::Stopped { reason },
                None => Availability::Available,
            },
            PortBinding::Unavailable(reason) => Availability::Unavailable {
                reason: reason.clone(),
            },
        }
    }

    /// Retained after stop, including its stop reason and loss counter.
    pub fn handle(&self) -> Option<&PortHandle> {
        match &self.binding {
            PortBinding::Running(handle) => Some(handle),
            PortBinding::Unavailable(_) => None,
        }
    }

    /// No counter exists for a port that was never started. See
    /// PortHandle::loss_epoch for gap detection and delivery limitations.
    pub fn loss_epoch(&self) -> Option<u64> {
        self.handle().map(PortHandle::loss_epoch)
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredDevice {
    pub id: String,
    pub port_id: String,
    /// Exact global keys; look up their descriptions through the registry.
    pub parameters: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RegisteredParameter {
    pub global_key: String,
    pub device_id: String,
    pub port_id: String,
    /// The existing model, with the driver's local name in description.id.
    pub description: ParameterDescription,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    InvalidPortId(String),
    DuplicatePort(String),
    InvalidDeviceId(String),
    DuplicateDevice(String),
    UnknownPort(String),
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPortId(id) => write!(f, "port.id {id:?}: must not be empty"),
            Self::DuplicatePort(id) => write!(f, "port.id {id:?}: already registered"),
            Self::InvalidDeviceId(id) => {
                write!(f, "device.id {id:?}: must not be empty or contain '/'")
            }
            Self::DuplicateDevice(id) => write!(f, "device.id {id:?}: already registered"),
            Self::UnknownPort(id) => write!(f, "device.port {id:?}: port is not registered"),
        }
    }
}
impl std::error::Error for RegistryError {}

#[derive(Default)]
pub struct DeviceRegistry {
    ports: BTreeMap<String, RegisteredPort>,
    devices: BTreeMap<String, RegisteredDevice>,
    parameters: BTreeMap<String, RegisteredParameter>,
}

impl DeviceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Associate one physical port with an externally managed worker. The caller
    /// is responsible for running it and retaining its observation receiver.
    pub fn register_port(
        &mut self,
        id: impl Into<String>,
        handle: PortHandle,
    ) -> Result<(), RegistryError> {
        self.insert_port(id.into(), PortBinding::Running(handle))
    }

    pub fn register_unavailable_port(
        &mut self,
        id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<(), RegistryError> {
        self.insert_port(id.into(), PortBinding::Unavailable(reason.into()))
    }

    fn insert_port(&mut self, id: String, binding: PortBinding) -> Result<(), RegistryError> {
        if id.trim().is_empty() {
            return Err(RegistryError::InvalidPortId(id));
        }
        if self.ports.contains_key(&id) {
            return Err(RegistryError::DuplicatePort(id));
        }
        self.ports
            .insert(id.clone(), RegisteredPort { id, binding });
        Ok(())
    }

    /// Snapshot the validated catalog, including polling overrides, without
    /// taking ownership of the driver. Also works for unavailable ports.
    /// Finish configuring the instance before registering it.
    pub fn register_device(
        &mut self,
        port_id: &str,
        device: &DeviceInstance,
    ) -> Result<(), RegistryError> {
        let id = device.id();
        if id.trim().is_empty() || id.contains('/') {
            return Err(RegistryError::InvalidDeviceId(id.into()));
        }
        if self.devices.contains_key(id) {
            return Err(RegistryError::DuplicateDevice(id.into()));
        }
        if !self.ports.contains_key(port_id) {
            return Err(RegistryError::UnknownPort(port_id.into()));
        }
        // DeviceInstance already validates uniqueness of local names and specs.
        // No '/' in the device id makes these keys unique even if a local name
        // contains '/'. Do not normalize or split local names.
        let mut keys = Vec::with_capacity(device.parameters().len());
        for description in device.parameters() {
            let global_key = format!("{id}/{}", description.id);
            self.parameters.insert(
                global_key.clone(),
                RegisteredParameter {
                    global_key: global_key.clone(),
                    device_id: id.into(),
                    port_id: port_id.into(),
                    description: description.clone(),
                },
            );
            keys.push(global_key);
        }
        self.devices.insert(
            id.into(),
            RegisteredDevice {
                id: id.into(),
                port_id: port_id.into(),
                parameters: keys,
            },
        );
        Ok(())
    }

    pub fn port(&self, id: &str) -> Option<&RegisteredPort> {
        self.ports.get(id)
    }

    pub fn ports(&self) -> impl Iterator<Item = &RegisteredPort> {
        self.ports.values()
    }

    pub fn device(&self, id: &str) -> Option<&RegisteredDevice> {
        self.devices.get(id)
    }

    pub fn devices(&self) -> impl Iterator<Item = &RegisteredDevice> {
        self.devices.values()
    }

    pub fn parameter(&self, global_key: &str) -> Option<&RegisteredParameter> {
        self.parameters.get(global_key)
    }

    pub fn parameters(&self) -> impl Iterator<Item = &RegisteredParameter> {
        self.parameters.values()
    }

    pub fn availability(&self, device_id: &str) -> Option<Availability> {
        let device = self.device(device_id)?;
        Some(self.ports[&device.port_id].availability())
    }

    /// Map the exact pair, without changing Observation.source. This does not
    /// establish which physical receiver an observation came from; a future
    /// aggregator must also check the registered port against that receiver.
    pub fn observation_parameter(
        &self,
        observation: &DeviceObservation,
    ) -> Option<&RegisteredParameter> {
        self.device(&observation.device_id)?;
        self.parameter(&format!(
            "{}/{}",
            observation.device_id, observation.observation.source
        ))
    }
}
