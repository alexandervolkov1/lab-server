use std::time::Duration;

use crate::driver::{DeviceIo, Driver};
use crate::model::{ParameterDescription, Spec, Value};

pub struct Arduino {
    next_id: u16,
}

impl Arduino {
    pub fn new() -> Self {
        Self { next_id: 1 }
    }
}

impl Driver for Arduino {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![
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
        ]
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, String> {
        let device_key = match parameter {
            "sample_temperature" => "t.sample",
            "heater_temperature" => "t.heater",
            "room_temperature" => "t.room",
            "power" => "power",
            _ => return Err(format!("Unknown Arduino parameter: {parameter}")),
        };

        let description = self
            .parameters()
            .into_iter()
            .find(|item| item.id == parameter)
            .expect("Known Arduino parameter must have a description");

        let id = self.next_id;

        self.next_id = if id == u16::MAX { 1 } else { id + 1 };

        let command = format!("o {id} get {device_key}\n");

        io.send(command.as_bytes())
            .map_err(|error| error.to_string())?;

        let mut response = String::new();

        let bytes = io
            .read_line(&mut response)
            .map_err(|error| error.to_string())?;

        if bytes == 0 {
            return Err("Empty response".to_string());
        }

        parse_numeric_reply(&response, id, &description.spec).map_err(|error| error.to_string())
    }

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), String> {
        let description = self
            .parameters()
            .into_iter()
            .find(|item| item.id == parameter)
            .ok_or_else(|| format!("Unknown Arduino parameter: {parameter}"))?;

        if !description.writable {
            return Err("Parameter is read-only".to_string());
        }

        if !description.spec.accepts(value) {
            return Err("Value does not match parameter spec".to_string());
        }

        let id = self.next_id;

        let command = match (parameter, value) {
            ("power", Value::Integer(power)) => {
                format!("o {id} set power {power}\n")
            }

            _ => return Err("Unsupported Arduino write".to_string()),
        };

        self.next_id = if id == u16::MAX { 1 } else { id + 1 };

        io.send(command.as_bytes())
            .map_err(|error| error.to_string())?;

        let mut response = String::new();

        let bytes = io
            .read_line(&mut response)
            .map_err(|error| error.to_string())?;

        if bytes == 0 {
            return Err("Empty response".to_string());
        }

        let mut parts = response.split_whitespace();

        let expected_id = format!("@{id}");

        if parts.next() != Some(expected_id.as_str()) {
            return Err("Unexpected response".to_string());
        }

        match (parts.next(), parts.next(), parts.next()) {
            (Some("ok"), None, None) => Ok(()),

            (Some("err"), Some(reason), None) => Err(format!("Arduino error: {reason}")),

            _ => Err("Unexpected response".to_string()),
        }
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
    };

    if !spec.accepts(&value) {
        return Err("Value out of range");
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::io::{self, BufReader, Cursor, Read, Write};

    use super::*;

    struct FakePort {
        input: Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for FakePort {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.input.read(buf)
        }
    }

    impl Write for FakePort {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn reads_temperature() {
        let fake = FakePort {
            input: Cursor::new(b"@1 ok 23.75\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        let result = driver.read("sample_temperature", &mut port);

        assert_eq!(result, Ok(Value::Float(23.75)));
        assert_eq!(port.get_ref().output, b"o 1 get t.sample\n");
    }

    #[test]
    fn rejects_unexpected_response() {
        let fake = FakePort {
            input: Cursor::new(b"ERR\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        let result = driver.read("sample_temperature", &mut port);

        assert_eq!(result, Err("Unexpected response".to_string()));
    }

    #[test]
    fn rejects_unknown_parameter() {
        let fake = FakePort {
            input: Cursor::new(Vec::new()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        let result = driver.read("unknown", &mut port);

        assert!(result.is_err());
        assert!(port.get_ref().output.is_empty());
    }

    #[test]
    fn parses_numeric_reply() {
        let temperature = Spec::Float {
            min: -250.0,
            max: 1500.0,
        };

        let power = Spec::Integer { min: 0, max: 100 };

        assert_eq!(
            parse_numeric_reply("@7 ok 23.75", 7, &temperature),
            Ok(Value::Float(23.75))
        );

        assert_eq!(
            parse_numeric_reply("@8 ok 10", 8, &power),
            Ok(Value::Integer(10))
        );

        assert!(parse_numeric_reply("@8 ok 10.5", 8, &power).is_err());
        assert!(parse_numeric_reply("@8 ok 101", 8, &power).is_err());
        assert!(parse_numeric_reply("@8 ok -1", 8, &power).is_err());

        assert!(parse_numeric_reply("@8 ok 23.75", 7, &temperature).is_err());
        assert!(parse_numeric_reply("@7 ok abc", 7, &temperature).is_err());
        assert!(parse_numeric_reply("@7 err key", 7, &temperature).is_err());
        assert!(parse_numeric_reply("ERR", 7, &temperature).is_err());
        assert!(parse_numeric_reply("@7 ok 23.75 extra", 7, &temperature).is_err());
    }

    #[test]
    fn increments_request_id() {
        let fake = FakePort {
            input: Cursor::new(b"@1 ok 20.00\n@2 ok 21.00\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        assert_eq!(
            driver.read("sample_temperature", &mut port),
            Ok(Value::Float(20.0))
        );

        assert_eq!(
            driver.read("sample_temperature", &mut port),
            Ok(Value::Float(21.0))
        );

        assert_eq!(
            port.get_ref().output,
            b"o 1 get t.sample\no 2 get t.sample\n"
        );
    }

    #[test]
    fn writes_power() {
        let fake = FakePort {
            input: Cursor::new(b"@1 ok 20.00\n@2 ok\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        assert_eq!(
            driver.read("sample_temperature", &mut port),
            Ok(Value::Float(20.0))
        );

        assert_eq!(
            driver.write("power", &Value::Integer(10), &mut port),
            Ok(())
        );

        assert_eq!(
            port.get_ref().output,
            b"o 1 get t.sample\no 2 set power 10\n"
        );
    }

    #[test]
    fn rejects_invalid_writes() {
        let fake = FakePort {
            input: Cursor::new(b"@1 ok\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        assert!(
            driver
                .write("power", &Value::Integer(-1), &mut port)
                .is_err()
        );
        assert!(
            driver
                .write("power", &Value::Integer(101), &mut port)
                .is_err()
        );
        assert!(
            driver
                .write("power", &Value::Float(10.0), &mut port)
                .is_err()
        );

        assert!(
            driver
                .write("sample_temperature", &Value::Float(20.0), &mut port)
                .is_err()
        );

        assert!(port.get_ref().output.is_empty());

        assert_eq!(
            driver.write("power", &Value::Integer(10), &mut port),
            Ok(())
        );

        assert_eq!(port.get_ref().output, b"o 1 set power 10\n");
    }

    #[test]
    fn reports_arduino_write_error() {
        let fake = FakePort {
            input: Cursor::new(b"@1 err range\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        let result = driver.write("power", &Value::Integer(10), &mut port);

        assert_eq!(result, Err("Arduino error: range".to_string()));
    }

    #[test]
    fn reads_remaining_numeric_parameters() {
        let fake = FakePort {
            input: Cursor::new(b"@1 ok 31.5\n@2 ok 21.25\n@3 ok 10\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino::new();

        assert_eq!(
            driver.read("heater_temperature", &mut port),
            Ok(Value::Float(31.5))
        );

        assert_eq!(
            driver.read("room_temperature", &mut port),
            Ok(Value::Float(21.25))
        );

        assert_eq!(driver.read("power", &mut port), Ok(Value::Integer(10)));

        assert_eq!(
            port.get_ref().output,
            b"o 1 get t.heater\no 2 get t.room\no 3 get power\n"
        );
    }
}
