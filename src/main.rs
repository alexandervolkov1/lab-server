use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;

use crate::driver::{Driver, arduino::Arduino};
use crate::model::{Observation, Value};

mod driver;
mod model;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = serialport::new("COM3", 57_600)
        .timeout(Duration::from_secs(3))
        .open()?;

    std::thread::sleep(Duration::from_secs(2));

    let mut reader = BufReader::new(port);

    let mut driver = Arduino;

    let description = driver
        .signals()
        .into_iter()
        .find(|signal| signal.id == "sample_temperature")
        .expect("Arduino signal not found");

    let signal_id = format!("arduino.{}", description.id);

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
            let reading = driver.read(&description.id, &mut reader).and_then(|value| {
                if description.spec.accepts(&value) {
                    Ok(value)
                } else {
                    Err("Temperature out of range".to_string())
                }
            });

            let observation = Observation {
                source: signal_id.clone(),
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

fn observation_json(observation: &Observation) -> io::Result<String> {
    let duration = observation
        .completed_at
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;

    let timestamp_ms = u64::try_from(duration.as_millis()).map_err(io::Error::other)?;

    let reading = match &observation.reading {
        Ok(Value::Float(value)) => json!({
            "status": "ok",
            "value": {
                "type": "float",
                "value": value
            }
        }),

        Err(error) => json!({
            "status": "error",
            "message": error
        }),
    };

    let message = json!({
        "source": &observation.source,
        "completed_at_unix_ms": timestamp_ms,
        "reading": reading
    });

    Ok(message.to_string())
}

fn handle_client(
    mut stream: TcpStream,
    latest: &Mutex<Option<Observation>>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;

    let mut reader = BufReader::new(stream.try_clone()?);

    loop {
        let mut request = String::new();

        let bytes = reader.by_ref().take(64).read_line(&mut request)?;

        if bytes == 0 {
            return Ok(());
        }

        if !request.ends_with('\n') {
            writeln!(
                stream,
                "{}",
                json!({
                    "status": "error",
                    "message": "invalid_command_framing"
                })
            )?;

            return Ok(());
        }

        if request.trim() != "GET" {
            writeln!(
                stream,
                "{}",
                json!({
                    "status": "error",
                    "message": "unknown_command"
                })
            )?;

            continue;
        }

        let response = {
            let guard = latest
                .lock()
                .map_err(|_| std::io::Error::other("Observation lock poisoned"))?;

            match guard.as_ref() {
                Some(observation) => observation_json(observation)?,
                None => json!({"status": "no_data"}).to_string(),
            }
        };

        writeln!(stream, "{response}")?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_observation_as_json() {
        let mut observation = Observation {
            source: "arduino.sample_temperature".to_string(),
            completed_at: UNIX_EPOCH + Duration::from_millis(1234),
            reading: Ok(Value::Float(20.0)),
        };

        let text = observation_json(&observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["source"], "arduino.sample_temperature");
        assert_eq!(json["completed_at_unix_ms"], 1234);
        assert_eq!(json["reading"]["status"], "ok");
        assert_eq!(json["reading"]["value"]["value"], 20.0);

        observation.reading = Err("Read timeout".to_string());

        let text = observation_json(&observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["reading"]["status"], "error");
        assert_eq!(json["reading"]["message"], "Read timeout")
    }
}
