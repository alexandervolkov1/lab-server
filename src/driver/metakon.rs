//! METAKON 5X3 register model over the worker-owned binary stream.
//! Protocol provenance and intentional differences: docs/metakon-driver.md.

use std::time::Duration;

use serde::Deserialize;

use super::{DeviceIo, Driver, DriverError, PortState};
use crate::model::{ParameterDescription, Spec, Value};

mod protocol;
use protocol::{WireType, exchange, read_request, validate_reply, write_request};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    address: u8,
    channel: u8,
    process_value_scale: f64,
    #[serde(default)]
    process_value_unit: String,
    #[serde(default)]
    write_enabled: bool,
}

pub struct Metakon {
    options: Options,
}

#[derive(Clone, Copy)]
struct Register {
    key: &'static str,
    address: u8,
    wire: WireType,
    range: (i64, i64),
    writable: bool,
    physical: bool,
}

impl Register {
    const fn new(
        key: &'static str,
        address: u8,
        wire: WireType,
        range: (i64, i64),
        writable: bool,
        physical: bool,
    ) -> Self {
        Self {
            key,
            address,
            wire,
            range,
            writable,
            physical,
        }
    }
}

use WireType::{Bool, I8, I16, U8, U16};

const REGISTERS: [Register; 15] = [
    Register::new("channel_type", 0x00, U8, (0, 255), false, false),
    Register::new("measurement", 0x01, I16, (-999, 9999), false, true),
    Register::new("setpoint", 0x02, I16, (-999, 9999), true, true),
    Register::new("proportional_band", 0x03, U16, (1, 9999), true, true),
    Register::new("integral_time", 0x04, U16, (1, 30000), true, false),
    Register::new("derivative_time", 0x05, U8, (0, 255), true, false),
    Register::new("output_power", 0x06, I8, (-100, 100), true, false),
    Register::new("pwm_positive", 0x07, Bool, (0, 1), false, false),
    Register::new("pwm_negative", 0x08, Bool, (0, 1), false, false),
    Register::new("upper_setpoint", 0x09, I16, (-999, 9999), true, true),
    Register::new("upper_hysteresis", 0x0a, U8, (0, 255), true, true),
    Register::new("upper_output", 0x0b, Bool, (0, 1), true, false),
    Register::new("lower_setpoint", 0x0c, I16, (-999, 9999), true, true),
    Register::new("lower_hysteresis", 0x0d, U8, (0, 255), true, true),
    Register::new("lower_output", 0x0e, Bool, (0, 1), true, false),
];

fn local_error(message: impl Into<String>) -> DriverError {
    DriverError::new(message, PortState::Synchronized)
}

impl Metakon {
    /// Validates options without I/O. Address, channel and scale are explicit;
    /// neither a COM port nor a guessed physical scale is chosen by this driver.
    pub fn from_options(options: &toml::Table) -> Result<Self, String> {
        let options: Options = toml::Value::Table(options.clone())
            .try_into()
            .map_err(|error| format!("invalid metakon options: {error}"))?;
        if options.channel > 2 {
            return Err("metakon channel must be 0..=2 (and exist on the device)".into());
        }
        if !options.process_value_scale.is_finite()
            || options.process_value_scale <= 0.0
            || !(9999.0 * options.process_value_scale).is_finite()
        {
            return Err(
                "metakon process_value_scale must be positive, finite and not overflow".into(),
            );
        }
        if options.process_value_unit.len() > 64
            || options.process_value_unit.chars().any(char::is_control)
        {
            return Err(
                "metakon process_value_unit must be at most 64 bytes without control characters"
                    .into(),
            );
        }
        Ok(Self { options })
    }

    fn register(&self, parameter: &str) -> Result<Register, DriverError> {
        REGISTERS
            .iter()
            .find(|r| r.key == parameter)
            .copied()
            .ok_or_else(|| local_error(format!("unknown metakon parameter {parameter:?}")))
    }

    fn scale(&self, register: Register) -> f64 {
        if register.physical {
            self.options.process_value_scale
        } else {
            1.0
        }
    }

    fn description(&self, register: Register) -> ParameterDescription {
        let scale = self.scale(register);
        let (min, max) = register.range;
        let spec = if register.wire == Bool {
            Spec::Boolean
        } else if scale == 1.0 {
            Spec::Integer { min, max }
        } else {
            Spec::Float {
                min: min as f64 * scale,
                max: max as f64 * scale,
            }
        };
        let unit = if register.physical {
            self.options.process_value_unit.clone()
        } else {
            match register.address {
                4 | 5 => "s",
                6 => "%",
                _ => "",
            }
            .into()
        };
        ParameterDescription {
            id: register.key.into(),
            spec,
            unit,
            writable: register.writable && self.options.write_enabled,
            default_poll_period: matches!(register.address, 1 | 6)
                .then_some(Duration::from_secs(1)),
        }
    }

    fn read_raw(&self, register: Register, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        let request = read_request(self.options.address, self.options.channel, register.address);
        let response = exchange(io, &request, 6 + register.wire.width())?;
        validate_reply(&response, &request[..4])?;
        // Physical permissions are independent of the software write interlock.
        let flags = register.wire as u8 | 0x40 | if register.writable { 0x80 } else { 0 };
        if response[4] != flags {
            return Err(DriverError::new(
                "metakon response type/access mismatch",
                PortState::Desynchronized,
            ));
        }
        let raw = register.wire.decode(&response[5..response.len() - 1])?;
        if register.address == 1 && raw == Value::Integer(-32768) {
            return Err(local_error("metakon measurement sensor fault (-32768)"));
        }
        if let Value::Integer(value) = raw
            && !(register.range.0..=register.range.1).contains(&value)
        {
            return Err(local_error(format!(
                "metakon {} outside register range: {value}",
                register.key
            )));
        }
        Ok(raw)
    }

    fn raw_write(&self, register: Register, value: &Value) -> Result<Value, DriverError> {
        if !self.description(register).spec.accepts(value) {
            return Err(local_error(format!(
                "invalid type or range for metakon {}",
                register.key
            )));
        }
        match value {
            Value::Float(value) => {
                let raw = value / self.scale(register);
                let rounded = raw.round();
                // Accept floating-point roundoff only, never round a physical setpoint
                // to another device step. Compare in raw units to avoid scale dependence.
                let tolerance = 8.0 * f64::EPSILON * raw.abs().max(1.0);
                if (raw - rounded).abs() > tolerance
                    || rounded < register.range.0 as f64
                    || rounded > register.range.1 as f64
                {
                    return Err(local_error("metakon value is not an exact register step"));
                }
                Ok(Value::Integer(rounded as i64))
            }
            _ => Ok(value.clone()),
        }
    }
}

impl Driver for Metakon {
    fn parameters(&self) -> Vec<ParameterDescription> {
        REGISTERS.iter().map(|r| self.description(*r)).collect()
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        let register = self.register(parameter)?;
        let raw = self.read_raw(register, io)?;
        match raw {
            Value::Integer(value) if self.scale(register) != 1.0 => {
                Ok(Value::Float(value as f64 * self.scale(register)))
            }
            _ => Ok(raw),
        }
    }

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        let register = self.register(parameter)?;
        if !self.options.write_enabled || !register.writable {
            return Err(local_error(format!("metakon {parameter} is read-only")));
        }
        let raw = self.raw_write(register, value)?;
        let request = write_request(
            self.options.address,
            self.options.channel,
            register.address,
            register.wire,
            &raw,
        );
        let response = exchange(io, &request, 5)?;
        validate_reply(&response, &request[..4])?;
        // A valid ACK confirms receipt, not an unchanged value (the instrument may
        // clamp or change it). One readback, no write retry even on mismatch/error.
        let readback = self.read_raw(register, io)?;
        if readback != raw {
            return Err(local_error(format!(
                "metakon {} readback mismatch",
                register.key
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
