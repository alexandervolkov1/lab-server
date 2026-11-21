//! Eltochpribor RRG-12 register map (manufacturer's Modbus manual, §6).

use std::time::Duration;

use serde::Deserialize;

use super::modbus_rtu::{ReadRegisters, RegisterTable};
use super::{DeviceIo, Driver, DriverError, PortState};
use crate::model::{ParameterDescription, Spec, Value};

const SILENCE: Duration = Duration::from_millis(20);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    address: u8,
    #[serde(default)]
    write_enabled: bool,
}

pub struct Rrg12 {
    address: u8,
}

const PARAMETERS: [&str; 7] = [
    "network_address",
    "serial_number",
    "status_flags_1",
    "status_flags_2",
    "setpoint_percent",
    "flow_percent",
    "baud_code",
];

impl Rrg12 {
    pub fn from_options(options: &toml::Table) -> Result<Self, String> {
        let options: Options = toml::Value::Table(options.clone())
            .try_into()
            .map_err(|error| format!("invalid rrg12 options: {error}"))?;
        if options.write_enabled {
            return Err("rrg12 control writes are unavailable".into());
        }
        ReadRegisters::new(options.address, RegisterTable::Holding, 0, 1)
            .map_err(|error| format!("invalid rrg12 address: {error}"))?;
        Ok(Self {
            address: options.address,
        })
    }
}

impl Driver for Rrg12 {
    fn parameters(&self) -> Vec<ParameterDescription> {
        PARAMETERS
            .iter()
            .enumerate()
            .map(|(register, &name)| {
                let spec = match register {
                    0 => Spec::Integer { min: 0, max: 255 },
                    4 => Spec::Float {
                        min: 0.0,
                        max: 655.35,
                    },
                    5 => Spec::Float {
                        min: -0.5,
                        max: 130.0,
                    },
                    _ => Spec::Integer { min: 0, max: 65535 },
                };
                ParameterDescription {
                    id: name.into(),
                    spec,
                    unit: if matches!(register, 4 | 5) { "%" } else { "" }.into(),
                    writable: false,
                    default_poll_period: (register == 5).then_some(Duration::from_secs(1)),
                }
            })
            .collect()
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        let register = PARAMETERS
            .iter()
            .position(|name| *name == parameter)
            .ok_or_else(|| {
                DriverError::new(
                    format!("unknown rrg12 parameter {parameter:?}"),
                    PortState::Synchronized,
                )
            })?;
        let raw = ReadRegisters::new(self.address, RegisterTable::Holding, register as u16, 1)?
            .exchange(io, SILENCE)?[0];
        match register {
            0 => Ok(Value::Integer(i64::from(raw & 0xff))),
            4 => Ok(Value::Float(f64::from(raw) / 100.0)),
            5 => {
                // The vendor's -4 example is 0x8004 (sign and magnitude).
                let magnitude = i32::from(raw & 0x7fff);
                let signed = if raw & 0x8000 == 0 {
                    magnitude
                } else {
                    -magnitude
                };
                if !(-50..=13_000).contains(&signed) {
                    return Err(DriverError::new(
                        "rrg12 flow outside documented range",
                        PortState::Synchronized,
                    ));
                }
                Ok(Value::Float(f64::from(signed) / 100.0))
            }
            _ => Ok(Value::Integer(i64::from(raw))),
        }
    }

    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        Err(DriverError::new(
            "rrg12 control writes are unavailable",
            PortState::Synchronized,
        ))
    }
}
