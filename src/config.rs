//! Static configuration parsing and validation, without device or network I/O.
//!
//! Ports describe physical connections; devices select drivers independently.
//! Sharing a port imposes no restrictions on driver names or driver options.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub tcp: TcpConfig,
    pub ports: Vec<PortConfig>,
    pub devices: Vec<DeviceConfig>,
    pub recording: Option<RecordingConfig>,
    pub websocket: Option<WebSocketConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub bind: Option<SocketAddr>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordingConfig {
    pub enabled: bool,
    /// Relative paths are resolved against the process working directory.
    pub path: Option<PathBuf>,
    pub queue_capacity: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpConfig {
    /// Numeric IP address and port; hostnames are not resolved during parsing.
    pub bind: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortConfig {
    pub id: String,
    /// Physical port name, compared exactly when checking for duplicates.
    pub name: String,
    pub baud: u32,
    pub data_bits: u8,
    pub parity: Parity,
    pub stop_bits: u8,
    pub flow_control: FlowControl,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Parity {
    None,
    Odd,
    Even,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowControl {
    None,
    Software,
    Hardware,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfig {
    /// Local device ID; `/` is reserved for future device/signal keys.
    pub id: String,
    pub driver: String,
    /// Reference to a PortConfig.id. Multiple devices may share this reference.
    pub port: String,
    /// Opaque driver settings, including nested tables, arrays and datetimes.
    #[serde(default)]
    pub options: toml::Table,
    /// Per-signal overrides. Startup checks local names against the driver
    /// catalog; this parser only validates values. Missing entries keep defaults.
    #[serde(default)]
    pub poll_periods_ms: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub enum ConfigError {
    Io { path: PathBuf, source: io::Error },
    Parse(toml::de::Error),
    Validation { field: String, message: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "cannot read config {}: {source}", path.display())
            }
            Self::Parse(source) => write!(f, "invalid TOML config: {source}"),
            Self::Validation { field, message } => write!(f, "{field}: {message}"),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse(source) => Some(source),
            Self::Validation { .. } => None,
        }
    }
}

impl ServerConfig {
    /// Parse and validate a TOML document. Does not access hardware or bind TCP.
    pub fn from_toml(input: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(input).map_err(ConfigError::Parse)?;
        config.validate()?;
        Ok(config)
    }

    /// Read a UTF-8 file, parse it and validate its contents.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let input = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_owned(),
            source,
        })?;
        Self::from_toml(&input)
    }

    /// Also usable after constructing or editing the public model in memory.
    /// Returns the first error; no driver implementations or ports are consulted.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(ws) = &self.websocket {
            if ws.enabled && ws.bind.is_none() {
                return Err(invalid("websocket.bind", "required when enabled"));
            }
            if let Some(bind) = ws.bind {
                if !bind.ip().is_loopback() {
                    return Err(invalid(
                        "websocket.bind",
                        "only loopback IP addresses are allowed",
                    ));
                }
                // Two port-zero listeners receive distinct ephemeral ports.
                if ws.enabled && bind.port() != 0 && bind == self.tcp.bind {
                    return Err(invalid("websocket.bind", "conflicts with tcp.bind"));
                }
            }
            for origin in &ws.allowed_origins {
                if origin.is_empty()
                    || origin.trim() != origin
                    || origin == "*"
                    || tungstenite::http::HeaderValue::from_str(origin).is_err()
                {
                    return Err(invalid(
                        "websocket.allowed_origins",
                        "expected exact nonempty Origin header values; wildcard is unsupported",
                    ));
                }
            }
        }
        if let Some(recording) = &self.recording {
            if recording.enabled
                && recording
                    .path
                    .as_ref()
                    .is_none_or(|path| path.as_os_str().to_string_lossy().trim().is_empty())
            {
                return Err(invalid(
                    "recording.path",
                    "must be nonempty when recording is enabled",
                ));
            }
            if recording.queue_capacity == Some(0) {
                return Err(invalid(
                    "recording.queue_capacity",
                    "must be greater than zero",
                ));
            }
        }
        if !self.tcp.bind.ip().is_loopback() {
            return Err(invalid(
                "tcp.bind",
                "only loopback IP addresses are allowed",
            ));
        }

        let mut port_ids = HashMap::new();
        let mut port_names = HashMap::new();
        for (index, port) in self.ports.iter().enumerate() {
            let object = format!("ports[{index}]");
            nonempty(&port.id, &format!("{object}.id"))?;
            nonempty(&port.name, &format!("{object}.name"))?;
            if let Some(previous) = port_ids.insert(port.id.as_str(), index) {
                return Err(invalid(
                    format!("{object}.id"),
                    format!(
                        "duplicate {:?}; already declared in ports[{previous}].id",
                        port.id
                    ),
                ));
            }
            if let Some(previous) = port_names.insert(port.name.as_str(), index) {
                return Err(invalid(
                    format!("{object}.name"),
                    format!(
                        "duplicate physical port {:?}; already declared in ports[{previous}].name",
                        port.name
                    ),
                ));
            }
            if port.baud == 0 {
                return Err(invalid(
                    format!("{object}.baud"),
                    "must be greater than zero",
                ));
            }
            if !(5..=8).contains(&port.data_bits) {
                return Err(invalid(
                    format!("{object}.data_bits"),
                    "must be 5, 6, 7 or 8",
                ));
            }
            if !matches!(port.stop_bits, 1 | 2) {
                return Err(invalid(format!("{object}.stop_bits"), "must be 1 or 2"));
            }
            if port.timeout_ms == 0 {
                return Err(invalid(
                    format!("{object}.timeout_ms"),
                    "must be greater than zero",
                ));
            }
        }

        let mut device_ids = HashMap::new();
        for (index, device) in self.devices.iter().enumerate() {
            let object = format!("devices[{index}]");
            nonempty(&device.id, &format!("{object}.id"))?;
            if device.id.contains('/') {
                return Err(invalid(format!("{object}.id"), "must not contain '/'"));
            }
            if let Some(previous) = device_ids.insert(device.id.as_str(), index) {
                return Err(invalid(
                    format!("{object}.id"),
                    format!(
                        "duplicate {:?}; already declared in devices[{previous}].id",
                        device.id
                    ),
                ));
            }
            nonempty(&device.driver, &format!("{object}.driver"))?;
            nonempty(&device.port, &format!("{object}.port"))?;
            if !port_ids.contains_key(device.port.as_str()) {
                return Err(invalid(
                    format!("{object}.port"),
                    format!("unknown port {:?}", device.port),
                ));
            }
            for (signal, period) in &device.poll_periods_ms {
                let field = format!("{object}.poll_periods_ms[{signal:?}]");
                nonempty(signal, &field)?;
                if *period == 0 {
                    return Err(invalid(field, "must be greater than zero"));
                }
            }
        }
        Ok(())
    }
}

fn invalid(field: impl Into<String>, message: impl Into<String>) -> ConfigError {
    ConfigError::Validation {
        field: field.into(),
        message: message.into(),
    }
}

fn nonempty(value: &str, field: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        Err(invalid(field, "must not be empty or whitespace-only"))
    } else {
        Ok(())
    }
}
