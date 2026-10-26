use crate::driver::{DeviceIo, Driver};
use crate::model::{SignalDescription, Spec, Value};

pub struct Arduino {
    next_id: u16,
}

impl Arduino {
    pub fn new() -> Self {
        Self { next_id: 1 }
    }
}

impl Driver for Arduino {
    fn signals(&self) -> Vec<SignalDescription> {
        vec![SignalDescription {
            id: "sample_temperature".to_string(),
            spec: Spec::Float {
                min: -250.0,
                max: 1500.0,
            },
            unit: "degC".to_string(),
        }]
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, String> {
        if parameter != "sample_temperature" {
            return Err(format!("Unknown Arduino parameter: {parameter}"));
        }

        let id = self.next_id;

        self.next_id = if id == u16::MAX { 1 } else { id + 1 };

        let command = format!("o {id} get t.sample\n");

        io.send(command.as_bytes())
            .map_err(|error| error.to_string())?;

        let mut response = String::new();

        let bytes = io
            .read_line(&mut response)
            .map_err(|error| error.to_string())?;

        if bytes == 0 {
            return Err("Empty response".to_string());
        }

        let temperature = parse_sample_temp(&response, id).map_err(|error| error.to_string())?;

        Ok(Value::Float(temperature))
    }
}

fn parse_sample_temp(line: &str, request_id: u16) -> Result<f64, &'static str> {
    let mut parts = line.split_whitespace();

    let expected_id = format!("@{request_id}");

    if parts.next() != Some(expected_id.as_str()) {
        return Err("Unexpected response");
    }

    if parts.next() != Some("ok") {
        return Err("Arduino request failed");
    }

    let raw_value = parts.next().ok_or("Missing temperature")?;

    if parts.next().is_some() {
        return Err("Too many fields");
    }

    let value = raw_value
        .parse::<f64>()
        .map_err(|_| "Invalid temperature")?;

    if !value.is_finite() {
        return Err("Non-finite temperature");
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
    fn parses_temperature() {
        assert_eq!(parse_sample_temp("@7 ok 23.75", 7), Ok(23.75));

        assert!(parse_sample_temp("@8 ok 23.75", 7).is_err());
        assert!(parse_sample_temp("@7 ok abc", 7).is_err());
        assert!(parse_sample_temp("@7 err key", 7).is_err());
        assert!(parse_sample_temp("ERR", 7).is_err());
        assert!(parse_sample_temp("@7 ok 23.75 extra", 7).is_err());
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
}
