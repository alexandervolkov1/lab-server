//! Preliminary MTM9D driver for the Thyracont V1 pressure exchange.

use std::io;
use std::thread;
use std::time::Duration;

use serde::Deserialize;

use super::{DeviceIo, Driver, DriverError, PortState};
use crate::model::{ParameterDescription, Spec, Value};

const MAX_RESPONSE_LEN: usize = 12;
// Hold the port after CR long enough for METAKON receivers on a mixed line.
const INTERFRAME_PAUSE: Duration = Duration::from_millis(9);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    address: u16,
}

pub struct Erstevak {
    address: [u8; 3],
}

impl Erstevak {
    pub fn from_options(options: &toml::Table) -> Result<Self, String> {
        let options: Options = toml::Value::Table(options.clone())
            .try_into()
            .map_err(|error| format!("invalid erstevak options: {error}"))?;
        if !(1..=999).contains(&options.address) {
            return Err("erstevak address must be 1..=999".into());
        }
        let mut address = [0; 3];
        address.copy_from_slice(format!("{:03}", options.address).as_bytes());
        Ok(Self { address })
    }

    fn request(&self) -> [u8; 6] {
        let [a, b, c] = self.address;
        let mut request = [a, b, c, b'M', 0, b'\r'];
        request[4] = checksum(&request[..4]);
        request
    }

    fn pressure(&self, frame: &[u8]) -> Result<f64, DriverError> {
        if !(6..=MAX_RESPONSE_LEN).contains(&frame.len()) || !frame.ends_with(b"\r") {
            return Err(desync("invalid response length or terminator"));
        }
        let (body, trailer) = frame.split_at(frame.len() - 2);
        if checksum(body) != trailer[0] {
            return Err(desync("response checksum mismatch"));
        }
        if body[..3] != self.address {
            return Err(desync("response address mismatch"));
        }
        let status = match &body[3..] {
            b"M000000" => "pressure underrange",
            b"M999999" => "pressure overrange",
            b"M1" => "gauge or sensor fault",
            b"M_LOGIC" => "command unavailable in the current state",
            b"NO_DEF" => "unsupported command",
            b"M_RANGE" => "command value out of range",
            [b'M', mantissa @ .., e1, e2]
                if mantissa.len() == 4
                    && mantissa[0] != b'0'
                    && mantissa.iter().all(u8::is_ascii_digit)
                    && e1.is_ascii_digit()
                    && e2.is_ascii_digit() =>
            {
                let mantissa = mantissa.iter().fold(0, |n, &digit| n * 10 + decimal(digit));
                let exponent = 10 * decimal(*e1) + decimal(*e2) - 23;
                return Ok(pressure_mbar(mantissa, exponent));
            }
            _ => return Err(desync("unexpected response command or data")),
        };
        Err(local_error(status))
    }
}

fn decimal(digit: u8) -> i32 {
    i32::from(digit - b'0')
}

fn pressure_mbar(mantissa: i32, exponent: i32) -> f64 {
    f64::from(mantissa) * 10_f64.powi(exponent)
}

fn checksum(bytes: &[u8]) -> u8 {
    (bytes.iter().copied().fold(0, u8::wrapping_add) % 64) + 64
}

fn local_error(message: &str) -> DriverError {
    DriverError::new(format!("erstevak V1: {message}"), PortState::Synchronized)
}

fn desync(message: &str) -> DriverError {
    DriverError::new(format!("erstevak V1: {message}"), PortState::Desynchronized)
}

fn read_frame(io: &mut dyn DeviceIo) -> Result<Vec<u8>, DriverError> {
    let mut frame = Vec::with_capacity(MAX_RESPONSE_LEN);
    loop {
        let available = match io.fill_buf() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if available.is_empty() {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "erstevak V1 response EOF").into(),
            );
        }
        let end = available.iter().position(|&byte| byte == b'\r');
        if let Some(index) = end
            && index + 1 != available.len()
        {
            return Err(desync("unexpected buffered bytes after CR"));
        }
        let length = available.len();
        if length > MAX_RESPONSE_LEN - frame.len()
            || (length == MAX_RESPONSE_LEN - frame.len() && end.is_none())
        {
            return Err(desync("response exceeds frame length"));
        }
        frame.extend_from_slice(available);
        io.consume(length);
        if end.is_some() {
            return Ok(frame);
        }
    }
}

impl Driver for Erstevak {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "pressure".into(),
            // Limits of the V1 number encoding, independent of the sensor range.
            spec: Spec::Float {
                min: pressure_mbar(1000, -23),
                max: pressure_mbar(9998, 76),
            },
            unit: "mbar".into(),
            writable: false,
            default_poll_period: Some(Duration::from_secs(1)),
        }]
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        if parameter != "pressure" {
            return Err(local_error("unknown parameter"));
        }
        io.send(&self.request())?;
        let frame = read_frame(io)?;
        thread::sleep(INTERFRAME_PAUSE);
        self.pressure(&frame).map(Value::Float)
    }

    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        Err(local_error("pressure is read-only"))
    }
}

#[cfg(test)]
mod tests;
