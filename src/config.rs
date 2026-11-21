//! Static configuration parsing and validation, without device or network I/O.
//!
//! Physical ports can be shared. A virtual port hosts one model and device.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::virtual_device::{PreparedVirtualModel, VirtualModel, create_model};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub tcp: TcpConfig,
    #[serde(default)]
    pub ports: Vec<PortConfig>,
    #[serde(default)]
    pub virtual_ports: Vec<VirtualPortConfig>,
    pub devices: Vec<DeviceConfig>,
    pub recording: Option<RecordingConfig>,
    pub websocket: Option<WebSocketConfig>,
    pub processing: Option<ProcessingConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(try_from = "ProcessingSection")]
pub struct ProcessingConfig {
    pub components: Vec<StartupComponent>,
    source: ProcessingSource,
}

#[derive(Debug, Clone, PartialEq)]
enum ProcessingSource {
    Inline,
    Unresolved(PathBuf),
    External { path: PathBuf, target: PathBuf },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessingSection {
    components: Option<Vec<StartupComponent>>,
    file: Option<PathBuf>,
}

impl TryFrom<ProcessingSection> for ProcessingConfig {
    type Error = &'static str;

    fn try_from(section: ProcessingSection) -> Result<Self, Self::Error> {
        if section.file.is_some() && section.components.is_some() {
            return Err("processing.file and processing.components are mutually exclusive");
        }
        Ok(Self {
            components: section.components.unwrap_or_default(),
            source: section
                .file
                .map_or(ProcessingSource::Inline, ProcessingSource::Unresolved),
        })
    }
}

const EXPERIMENT_VERSION: u64 = 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExperimentDocument {
    schema_version: u64,
    components: Vec<StartupComponent>,
}

impl ExperimentDocument {
    pub(crate) fn new(components: Vec<StartupComponent>) -> Self {
        Self {
            schema_version: EXPERIMENT_VERSION,
            components,
        }
    }

    pub(crate) fn to_toml(&self) -> io::Result<String> {
        // With arbitrary_precision enabled, JSON numbers serialize to a
        // private struct for non-JSON serializers. Convert them to TOML
        // scalars explicitly while keeping the shared experiment schema.
        let value = serde_json::to_value(self)?;
        toml::to_string_pretty(&experiment_value(value)?).map_err(io::Error::other)
    }
}

fn experiment_value(value: serde_json::Value) -> io::Result<toml::Value> {
    use serde_json::Value::*;
    let invalid_value = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "value cannot be represented in experiment TOML",
        )
    };
    Ok(match value {
        Bool(value) => toml::Value::Boolean(value),
        String(value) => toml::Value::String(value),
        Number(value) => {
            if let Some(integer) = value.as_i64() {
                toml::Value::Integer(integer)
            } else {
                toml::Value::Float(
                    value
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or_else(invalid_value)?,
                )
            }
        }
        Array(values) => toml::Value::Array(
            values
                .into_iter()
                .map(experiment_value)
                .collect::<io::Result<_>>()?,
        ),
        Object(fields) => toml::Value::Table(
            fields
                .into_iter()
                .map(|(key, value)| Ok((key, experiment_value(value)?)))
                .collect::<io::Result<_>>()?,
        ),
        Null => return Err(invalid_value()),
    })
}

impl ProcessingConfig {
    fn resolve(&mut self, main_path: Option<&Path>) -> Result<(), ConfigError> {
        let ProcessingSource::Unresolved(path) = &self.source else {
            return Ok(());
        };
        nonempty(&path.to_string_lossy(), "processing.file")?;
        let path = if path.is_absolute() {
            path.clone()
        } else {
            // On Windows, drive-relative and root-only paths do not have a
            // well-defined interpretation relative to the main file's directory.
            if matches!(
                path.components().next(),
                Some(std::path::Component::Prefix(_) | std::path::Component::RootDir)
            ) {
                return Err(invalid(
                    "processing.file",
                    format!(
                        "{} must be absolute or relative to the main config directory",
                        path.display()
                    ),
                ));
            }
            let base = main_path.and_then(Path::parent).ok_or_else(|| invalid(
                "processing.file",
                format!("relative path {} requires a main configuration path; use ServerConfig::load", path.display()),
            ))?;
            base.join(path)
        };
        // Freeze the actual file's absolute location, including symlink
        // resolution, before reading. Later Save uses this same target.
        let io_error = |source| ConfigError::Io {
            path: path.clone(),
            source,
        };
        let target = fs::canonicalize(&path).map_err(io_error)?;
        let input = fs::read_to_string(&target).map_err(io_error)?;
        let document: ExperimentDocument = toml::from_str(&input)
            .map_err(|error| ConfigError::Parse(error).in_processing_file(&path))?;
        if document.schema_version != EXPERIMENT_VERSION {
            return Err(invalid(
                "schema_version",
                format!(
                    "unsupported version {}; expected {EXPERIMENT_VERSION}",
                    document.schema_version
                ),
            )
            .in_processing_file(&path));
        }
        self.components = document.components;
        self.source = ProcessingSource::External { path, target };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "toml::Table")]
pub struct StartupComponent {
    #[serde(flatten)]
    pub configuration: crate::processing::Configuration,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binding: Option<StartupBinding>,
}

impl TryFrom<toml::Table> for StartupComponent {
    type Error = String;

    fn try_from(mut table: toml::Table) -> Result<Self, Self::Error> {
        let name = table
            .get("name")
            .and_then(toml::Value::as_str)
            .unwrap_or("<unnamed>")
            .to_owned();
        // Remove only the startup extension, then deserialize the existing strict
        // Configuration schema (including rejection of state/expected_revision).
        let parse = || -> Result<Self, toml::de::Error> {
            let binding = table
                .remove("binding")
                .map(toml::Value::try_into)
                .transpose()?;
            Ok(Self {
                configuration: table.try_into()?,
                binding,
            })
        };
        parse().map_err(|error| format!("component {name:?}: {error}"))
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StartupBinding {
    pub target: String,
    pub command_timeout_ms: u64,
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
    #[serde(default)]
    pub audit_enabled: bool,
    pub audit_queue_capacity: Option<usize>,
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
    /// Minimum host-side gap before the next request. Zero disables the guard.
    #[serde(default)]
    pub bus_silence_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualPortConfig {
    pub id: String,
    pub model: String,
    #[serde(default)]
    pub options: toml::Table,
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
    /// Device ID; `/` separates it from the local parameter in global keys.
    pub id: String,
    pub driver: String,
    /// Physical or virtual port ID. Only physical ports can be shared.
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
    Io {
        path: PathBuf,
        source: io::Error,
    },
    Parse(toml::de::Error),
    Validation {
        field: String,
        message: String,
    },
    ProcessingFile {
        path: PathBuf,
        source: Box<ConfigError>,
    },
}

impl ConfigError {
    fn in_processing_file(self, path: &Path) -> Self {
        Self::ProcessingFile {
            path: path.to_owned(),
            source: Box::new(self),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "cannot read config {}: {source}", path.display())
            }
            Self::Parse(source) => write!(f, "invalid TOML config: {source}"),
            Self::Validation { field, message } => write!(f, "{field}: {message}"),
            Self::ProcessingFile { path, source } => {
                write!(f, "processing file {}: {source}", path.display())
            }
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse(source) => Some(source),
            Self::Validation { .. } => None,
            Self::ProcessingFile { source, .. } => Some(source),
        }
    }
}

impl ServerConfig {
    /// Parse and validate a TOML document. A processing file must have an
    /// absolute path here; use `load` for a path relative to the main file.
    pub fn from_toml(input: &str) -> Result<Self, ConfigError> {
        Self::parse(input, None)
    }

    fn parse(input: &str, main_path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut config: Self = toml::from_str(input).map_err(ConfigError::Parse)?;
        if let Some(processing) = &mut config.processing {
            processing.resolve(main_path)?;
        }
        config.validate()?;
        Ok(config)
    }

    /// Read a UTF-8 file, parse it and validate its contents.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        Self::parse(&read_config(path)?, Some(path))
    }

    pub(crate) fn processing_error(&self, error: ConfigError) -> ConfigError {
        match self
            .processing
            .as_ref()
            .map(|processing| &processing.source)
        {
            Some(ProcessingSource::External { path, .. }) => error.in_processing_file(path),
            _ => error,
        }
    }

    pub(crate) fn experiment_target(&self) -> Option<&Path> {
        match &self.processing.as_ref()?.source {
            ProcessingSource::External { target, .. } => Some(target),
            _ => None,
        }
    }

    /// Also usable after constructing or editing the public model in memory.
    /// Returns the first error without opening any I/O resources.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_with_models(create_model).map(drop)
    }

    pub(crate) fn validate_with_models(
        &self,
        mut model_factory: impl FnMut(&str, &toml::Table) -> Result<Box<dyn VirtualModel>, String>,
    ) -> Result<HashMap<&str, PreparedVirtualModel>, ConfigError> {
        if let Some(ProcessingConfig {
            source: ProcessingSource::Unresolved(path),
            ..
        }) = &self.processing
        {
            return Err(invalid(
                "processing.file",
                format!(
                    "{} has not been loaded; use ServerConfig::load or ServerConfig::from_toml",
                    path.display()
                ),
            ));
        }
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
            if (recording.enabled || recording.audit_enabled)
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
            if recording.queue_capacity == Some(0) || recording.audit_queue_capacity == Some(0) {
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
            if let Some((previous, _)) = port_ids.insert(port.id.as_str(), (object.clone(), false))
            {
                return Err(invalid(
                    format!("{object}.id"),
                    format!("duplicate {:?}; already declared in {previous}.id", port.id),
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

        let mut virtual_models = HashMap::new();
        for (index, port) in self.virtual_ports.iter().enumerate() {
            let object = format!("virtual_ports[{index}]");
            nonempty(&port.id, &format!("{object}.id"))?;
            if let Some((previous, _)) = port_ids.insert(port.id.as_str(), (object.clone(), true)) {
                return Err(invalid(
                    format!("{object}.id"),
                    format!("duplicate {:?}; already declared in {previous}.id", port.id),
                ));
            }
            let model = model_factory(&port.model, &port.options)
                .and_then(PreparedVirtualModel::new)
                .map_err(|reason| invalid(&object, format!("model {:?}: {reason}", port.model)))?;
            virtual_models.insert(port.id.as_str(), model);
        }

        let mut device_ids = HashMap::new();
        let mut virtual_devices = HashMap::new();
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
            let Some((_, virtual_port)) = port_ids.get(device.port.as_str()) else {
                return Err(invalid(
                    format!("{object}.port"),
                    format!("unknown port {:?}", device.port),
                ));
            };
            if *virtual_port != (device.driver == "virtual") {
                return Err(invalid(
                    format!("{object}.driver"),
                    "virtual ports and driver = 'virtual' must be used together",
                ));
            }
            if *virtual_port {
                if !device.options.is_empty() {
                    return Err(invalid(
                        format!("{object}.options"),
                        "configure the model in virtual_ports.options",
                    ));
                }
                if let Some(previous) = virtual_devices.insert(device.port.as_str(), index) {
                    return Err(invalid(
                        format!("{object}.port"),
                        format!(
                            "virtual port {:?} already used by devices[{previous}]",
                            device.port
                        ),
                    ));
                }
            }
            for (signal, period) in &device.poll_periods_ms {
                let field = format!("{object}.poll_periods_ms[{signal:?}]");
                nonempty(signal, &field)?;
                if *period == 0 {
                    return Err(invalid(field, "must be greater than zero"));
                }
            }
        }
        for (index, port) in self.virtual_ports.iter().enumerate() {
            if !virtual_devices.contains_key(port.id.as_str()) {
                return Err(invalid(
                    format!("virtual_ports[{index}].id"),
                    format!("virtual port {:?} must have one device", port.id),
                ));
            }
        }
        Ok(virtual_models)
    }
}

fn read_config(path: &Path) -> Result<String, ConfigError> {
    fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })
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

#[cfg(test)]
mod experiment_tests {
    use super::*;
    use crate::processing::Configuration;
    use serde_json::json;

    #[test]
    fn export_round_trips_toml_strings_and_scalar_types() {
        let escaped = "device/path\\with\"quotes\"\nand a tab\tΩ";
        let components = vec![StartupComponent {
            configuration: Configuration {
                name: "controller.alpha".into(),
                type_id: "test-controller".into(),
                inputs: BTreeMap::from([("input".into(), escaped.into())]),
                parameters: BTreeMap::from([
                    ("choice".into(), json!(escaped)),
                    ("count".into(), json!(i64::MIN)),
                    ("enabled".into(), json!(true)),
                    ("gain".into(), json!(1.25)),
                ]),
                max_age_ms: 5000,
            },
            binding: Some(StartupBinding {
                target: escaped.into(),
                command_timeout_ms: 999,
            }),
        }];
        let document = ExperimentDocument::new(components.clone());
        let text = document.to_toml().unwrap();
        assert_eq!(document.to_toml().unwrap(), text);
        let parsed: ExperimentDocument = toml::from_str(&text).unwrap();
        assert_eq!(parsed.schema_version, 1);
        assert_eq!(parsed.components, components);
    }

    #[test]
    fn loading_relative_main_path_freezes_the_absolute_experiment_target() {
        let main = Path::new("examples/virtual-plant-external.toml");
        let config = ServerConfig::load(main).unwrap();
        let target = config.experiment_target().unwrap();
        assert!(target.is_absolute());
        assert_eq!(
            target,
            fs::canonicalize("examples/experiments/thermal-loop.toml").unwrap()
        );
    }
}
