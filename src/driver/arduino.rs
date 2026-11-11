use std::io::{BufRead, Read};
use std::time::Duration;

use crate::driver::{DeviceIo, Driver, DriverError, PortState};
use crate::model::{ActionDescription, ParameterDescription, Spec, Value};

// thermal-plant-uno 337e6a7: staged values have no physical bounds at set time.
// The firmware validates the complete configuration only on apply.
const DESIGNER_FLOAT_KEYS: [&str; 18] = [
    "heater.capacity",
    "sample.capacity",
    "room.capacity",
    "coupling",
    "heater.loss",
    "sample.loss",
    "room.loss",
    "heater.radiation",
    "sample.radiation",
    "heater.max_power",
    "initial.heater",
    "initial.sample",
    "initial.room",
    "background.temp",
    "parameter.spread",
    "power.fluctuation",
    "sensor.noise",
    "sensor.drift",
];

pub struct Arduino {
    next_id: u16,
}

impl Arduino {
    pub fn new() -> Self {
        Self { next_id: 1 }
    }

    /// One exchange, no retry and no input flushing. All supported replies fit
    /// in 64 bytes; an overlong or incomplete frame cannot establish a boundary.
    fn exchange(
        &mut self,
        scope: char,
        command: &str,
        numeric: bool,
        io: &mut dyn DeviceIo,
    ) -> Result<(u16, String), DriverError> {
        let id = self.next_id;
        let request = format!("{scope} {id} {command}\n");
        if request.len() - 1 > 64 {
            return Err(local_error("Arduino command too long"));
        }
        self.next_id = if id == u16::MAX { 1 } else { id + 1 };
        io.send(request.as_bytes())?;
        let mut response = String::new();
        let bytes = io.take(65).read_line(&mut response)?;
        if bytes == 0 {
            return Err(DriverError::new("Empty response", PortState::Disconnected));
        }
        let Some(line) = response.strip_suffix('\n') else {
            return Err(DriverError::new(
                "Incomplete response",
                PortState::Desynchronized,
            ));
        };
        let line = line.strip_suffix('\r').unwrap_or(line);
        let fields: Vec<_> = line.split(' ').collect();
        let expected_id = format!("@{id}");
        if bytes > 64
            || fields.first() != Some(&expected_id.as_str())
            || fields
                .iter()
                .any(|field| field.is_empty() || !field.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(DriverError::new(
                "Unexpected response",
                PortState::Desynchronized,
            ));
        }
        match fields.as_slice() {
            [_, "ok"] if !numeric => Ok((id, line.into())),
            [_, "ok", _] if numeric => Ok((id, line.into())),
            [_, "err", reason]
                if scope == 'o'
                    || matches!(
                        *reason,
                        "syntax" | "key" | "value" | "config" | "unsupported"
                    ) =>
            {
                Err(local_error(format!("Arduino error: {reason}")))
            }
            _ => Err(DriverError::new(
                "Unexpected response",
                PortState::Desynchronized,
            )),
        }
    }
}

impl Default for Arduino {
    fn default() -> Self {
        Self::new()
    }
}

fn local_error(message: impl Into<String>) -> DriverError {
    DriverError::new(message, PortState::Synchronized)
}

fn designer_key(parameter: &str) -> Option<&str> {
    parameter
        .strip_prefix("designer/")
        .filter(|key| *key == "seed" || DESIGNER_FLOAT_KEYS.contains(key))
}

// Convert once to binary32, then emit a short decimal that round-trips to those
// bits. Always include an exponent: room.capacity's plain integer syntax is u32
// and would reject negatives, -0, large floats and inexact integer rounding.
fn designer_float(value: f64) -> Result<f32, DriverError> {
    let rounded = value as f32;
    if !value.is_finite()
        || value.abs() > f64::from(f32::MAX)
        || !rounded.is_finite()
        || (value != 0.0 && rounded == 0.0)
    {
        return Err(local_error(
            "Value cannot be represented as a finite non-underflowing f32",
        ));
    }
    Ok(rounded)
}

fn parse_designer_float(raw: &str) -> Result<Value, DriverError> {
    // Parse directly to f32: the firmware emits up to nine significant decimal
    // digits, not the exact f64 expansion (notably for f32::MAX).
    let value = raw
        .parse::<f32>()
        .map_err(|_| local_error("Invalid float"))?;
    let significand = raw.split(['e', 'E']).next().unwrap_or(raw);
    if !value.is_finite() || (value == 0.0 && significand.bytes().any(|b| matches!(b, b'1'..=b'9')))
    {
        return Err(local_error("Value out of range"));
    }
    Ok(Value::Float(f64::from(value)))
}

impl Driver for Arduino {
    fn parameters(&self) -> Vec<ParameterDescription> {
        let mut parameters = vec![
            ParameterDescription {
                id: "sample_temperature".to_string(),
                spec: Spec::Float {
                    min: -250.0,
                    max: 1500.0,
                },
                unit: "degC".to_string(),
                writable: false,
                default_poll_period: Some(Duration::from_secs(1)),
            },
            ParameterDescription {
                id: "heater_temperature".to_string(),
                spec: Spec::Float {
                    min: -250.0,
                    max: 1500.0,
                },
                unit: "degC".to_string(),
                writable: false,
                default_poll_period: Some(Duration::from_secs(1)),
            },
            ParameterDescription {
                id: "room_temperature".to_string(),
                spec: Spec::Float {
                    min: -250.0,
                    max: 1500.0,
                },
                unit: "degC".to_string(),
                writable: false,
                default_poll_period: Some(Duration::from_secs(1)),
            },
            ParameterDescription {
                id: "power".to_string(),
                spec: Spec::Integer { min: 0, max: 100 },
                unit: "%".to_string(),
                writable: true,
                default_poll_period: Some(Duration::from_secs(1)),
            },
        ];
        parameters.extend(
            DESIGNER_FLOAT_KEYS
                .into_iter()
                .map(|key| ParameterDescription {
                    id: format!("designer/{key}"),
                    spec: Spec::Float {
                        min: f64::from(f32::MIN),
                        max: f64::from(f32::MAX),
                    },
                    unit: String::new(),
                    writable: true,
                    default_poll_period: None,
                }),
        );
        parameters.push(ParameterDescription {
            id: "designer/seed".into(),
            spec: Spec::Integer {
                min: 0,
                max: i64::from(u32::MAX),
            },
            unit: String::new(),
            writable: true,
            default_poll_period: None,
        });
        parameters
    }

    fn actions(&self) -> Vec<ActionDescription> {
        vec![
            ActionDescription {
                name: "apply".into(),
                description: "Apply staged configuration and restart simulation at 0% power".into(),
            },
            ActionDescription {
                name: "reset".into(),
                description: "Discard staged edits and restart active configuration at 0% power"
                    .into(),
            },
        ]
    }

    fn invoke_action(&mut self, action: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        if !matches!(action, "apply" | "reset") {
            return Err(local_error(format!("Unknown Arduino action: {action}")));
        }
        self.exchange('d', action, false, io).map(|_| ())
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        let (scope, key) = if let Some(key) = designer_key(parameter) {
            ('d', key)
        } else {
            (
                'o',
                match parameter {
                    "sample_temperature" => "t.sample",
                    "heater_temperature" => "t.heater",
                    "room_temperature" => "t.room",
                    "power" => "power",
                    _ => {
                        return Err(local_error(format!(
                            "Unknown Arduino parameter: {parameter}"
                        )));
                    }
                },
            )
        };
        let (id, response) = self.exchange(scope, &format!("get {key}"), true, io)?;
        if scope == 'd' {
            let raw = response
                .rsplit(' ')
                .next()
                .expect("exchange validated payload");
            if key == "seed" {
                if !raw.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(local_error("Invalid seed"));
                }
                return raw
                    .parse::<u32>()
                    .map(|v| Value::Integer(i64::from(v)))
                    .map_err(|_| local_error("Invalid seed"));
            }
            return parse_designer_float(raw);
        }
        let description = self
            .parameters()
            .into_iter()
            .find(|p| p.id == parameter)
            .expect("known parameter");
        parse_numeric_reply(&response, id, &description.spec).map_err(local_error)
    }

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        let description = self
            .parameters()
            .into_iter()
            .find(|p| p.id == parameter)
            .ok_or_else(|| local_error(format!("Unknown Arduino parameter: {parameter}")))?;
        if !description.writable {
            return Err(local_error("Parameter is read-only"));
        }
        if !description.spec.accepts(value) {
            return Err(local_error("Value does not match parameter spec"));
        }
        let (scope, key, encoded) = match (designer_key(parameter), value) {
            (Some("seed"), Value::Integer(seed)) => ('d', "seed", seed.to_string()),
            (Some(key), Value::Float(value)) => {
                ('d', key, format!("{:e}", designer_float(*value)?))
            }
            (None, Value::Integer(power)) if parameter == "power" => {
                ('o', "power", power.to_string())
            }
            _ => return Err(local_error("Unsupported Arduino write")),
        };
        self.exchange(scope, &format!("set {key} {encoded}"), false, io)
            .map(|_| ())
    }
}

fn parse_numeric_reply(line: &str, request_id: u16, spec: &Spec) -> Result<Value, &'static str> {
    let mut parts = line.split_whitespace();

    let expected_id = format!("@{request_id}");

    if parts.next() != Some(expected_id.as_str()) {
        return Err("Unexpected response");
    }

    if parts.next() != Some("ok") {
        return Err("Arduino request failed");
    }

    let raw_value = parts.next().ok_or("Missing value")?;

    if parts.next().is_some() {
        return Err("Too many fields");
    }

    let value = match spec {
        Spec::Float { .. } => {
            let number = raw_value.parse::<f64>().map_err(|_| "Invalid float")?;

            Value::Float(number)
        }

        Spec::Integer { .. } => {
            let number = raw_value.parse::<i64>().map_err(|_| "Invalid integer")?;

            Value::Integer(number)
        }
        Spec::Boolean => return Err("Boolean is not supported by Arduino"),
    };

    if !spec.accepts(&value) {
        return Err("Value out of range");
    }

    Ok(value)
}

#[cfg(test)]
mod tests;
