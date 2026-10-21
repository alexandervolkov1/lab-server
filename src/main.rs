use std::io::{BufRead, BufReader, Write};
use std::time::Duration;

use crate::model::{Spec, Value};

mod model;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut port = serialport::new("COM3", 57_600)
        .timeout(Duration::from_secs(3))
        .open()?;

    std::thread::sleep(Duration::from_secs(2));

    port.write_all(b"ts\n")?;
    port.flush()?;

    let mut reader = BufReader::new(port);
    let mut response = String::new();

    reader.read_line(&mut response)?;

    println!("Arduino: {}", response.trim());

    Ok(())
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
    use super::*;

    #[test]
    fn test_temperature() {
        assert_eq!(parse_sample_temp("SAMPLE_TEMP 23.75"), Ok(23.75));
        assert!(parse_sample_temp("SAMPLE_TEMP abc").is_err());
        assert!(parse_sample_temp("ERR").is_err());
        assert!(parse_sample_temp("SAMPLE_TEMP 23.75 extra").is_err());
    }
}
