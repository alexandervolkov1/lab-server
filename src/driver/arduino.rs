use crate::driver::{DeviceIo, Driver};
use crate::model::{SignalDescription, Spec, Value};

pub struct Arduino;

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

        io.send(b"ts\n").map_err(|error| error.to_string())?;

        let mut response = String::new();

        let bytes = io
            .read_line(&mut response)
            .map_err(|error| error.to_string())?;

        if bytes == 0 {
            return Err("Empty response".to_string());
        }

        let temperature = parse_sample_temp(&response).map_err(|error| error.to_string())?;

        Ok(Value::Float(temperature))
    }
}

fn parse_sample_temp(line: &str) -> Result<f64, &'static str> {
    let mut parts = line.split_whitespace();

    if parts.next() != Some("SAMPLE_TEMP") {
        return Err("Unexpected response");
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
            input: Cursor::new(b"SAMPLE_TEMP 23.75\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino;

        let result = driver.read("sample_temperature", &mut port);

        assert_eq!(result, Ok(Value::Float(23.75)));
        assert_eq!(port.get_ref().output, b"ts\n");
    }

    #[test]
    fn rejects_unexpected_response() {
        let fake = FakePort {
            input: Cursor::new(b"ERR\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut port = BufReader::new(fake);
        let mut driver = Arduino;

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
        let mut driver = Arduino;

        let result = driver.read("unknown", &mut port);

        assert!(result.is_err());
        assert!(port.get_ref().output.is_empty());
    }

    #[test]
    fn parses_temperature() {
        assert_eq!(parse_sample_temp("SAMPLE_TEMP 23.75"), Ok(23.75));
        assert!(parse_sample_temp("SAMPLE_TEMP abc").is_err());
        assert!(parse_sample_temp("ERR").is_err());
        assert!(parse_sample_temp("SAMPLE_TEMP 23.75 extra").is_err());
    }
}
