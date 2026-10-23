use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};

use crate::model::{Observation, SignalDescription, Spec, Value};

mod model;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = serialport::new("COM3", 57_600)
        .timeout(Duration::from_secs(3))
        .open()?;

    std::thread::sleep(Duration::from_secs(2));

    let mut reader = BufReader::new(port);

    let description = SignalDescription {
        id: "arduino.sample_temperature".to_string(),
        spec: Spec::Float {
            min: -250.0,
            max: 1500.0,
        },
        unit: "degC".to_string(),
    };

    println!("Signal: {} [{}]", description.id, description.unit);

    let (tx, rx) = mpsc::channel::<Observation>();
    let latest = Arc::new(Mutex::new(None::<Observation>));

    let listener = TcpListener::bind("127.0.0.1:7240")?;

    println!("TCP server listening on {}", listener.local_addr()?);

    let tcp_latest = Arc::clone(&latest);

    let _tcp_worker = thread::spawn(move || {
        for connection in listener.incoming() {
            match connection {
                Ok(stream) => {
                    let client_latest = Arc::clone(&tcp_latest);

                    thread::spawn(move || {
                        if let Err(error) = handle_client(stream, &client_latest) {
                            eprintln!("TCP client error: {error}");
                        }
                    });
                }

                Err(error) => {
                    eprintln!("TCP accept error: {error}");
                }
            }
        }
    });

    let worker = thread::spawn(move || {
        loop {
            let reading = match read_temperature(&mut reader) {
                Ok(temperature) => {
                    let value = Value::Float(temperature);

                    if description.spec.accepts(&value) {
                        Ok(value)
                    } else {
                        Err("Temperature out of range".to_string())
                    }
                }

                Err(error) => Err(error),
            };

            let observation = Observation {
                source: description.id.clone(),
                completed_at: SystemTime::now(),
                reading,
            };

            if tx.send(observation).is_err() {
                break;
            }

            thread::sleep(Duration::from_secs(1));
        }
    });

    for observation in rx {
        let mut guard = latest.lock().expect("Observation lock poisoned");

        *guard = Some(observation);
    }

    worker.join().expect("Reader thread panicked");

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

fn read_temperature<P: Read + Write>(reader: &mut BufReader<P>) -> Result<f64, String> {
    reader
        .get_mut()
        .write_all(b"ts\n")
        .map_err(|error| error.to_string())?;

    reader
        .get_mut()
        .flush()
        .map_err(|error| error.to_string())?;

    let mut response = String::new();

    let bytes = reader
        .read_line(&mut response)
        .map_err(|error| error.to_string())?;

    if bytes == 0 {
        return Err("Empty response".to_string());
    }

    parse_sample_temp(&response).map_err(|error| error.to_string())
}

fn handle_client(
    mut stream: TcpStream,
    latest: &Mutex<Option<Observation>>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;

    let mut request = String::new();

    BufReader::new(&stream).take(64).read_line(&mut request)?;

    if request.trim() != "GET" {
        writeln!(stream, "ERR Unknown command")?;
        return Ok(());
    }

    let response = {
        let guard = latest
            .lock()
            .map_err(|_| std::io::Error::other("Observation lock poisoned"))?;

        match guard.as_ref() {
            Some(observation) => format!("{observation:?}"),
            None => "NO_DATA".to_string(),
        }
    };

    writeln!(stream, "{response}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

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
    fn reads_temperature_without_arduino() {
        let fake = FakePort {
            input: Cursor::new(b"SAMPLE_TEMP 23.75\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut reader = BufReader::new(fake);
        let result = read_temperature(&mut reader);

        assert_eq!(result, Ok(23.75));

        assert_eq!(reader.get_ref().output.as_slice(), b"ts\n");

        let error_fake = FakePort {
            input: Cursor::new(b"ERR\r\n".to_vec()),
            output: Vec::new(),
        };

        let mut reader = BufReader::new(error_fake);
        let error_result = read_temperature(&mut reader);

        assert_eq!(error_result, Err("Unexpected response".to_string()));

        assert_eq!(reader.get_ref().output.as_slice(), b"ts\n");
    }

    #[test]
    fn test_temperature() {
        assert_eq!(parse_sample_temp("SAMPLE_TEMP 23.75"), Ok(23.75));
        assert!(parse_sample_temp("SAMPLE_TEMP abc").is_err());
        assert!(parse_sample_temp("ERR").is_err());
        assert!(parse_sample_temp("SAMPLE_TEMP 23.75 extra").is_err());
    }
}
