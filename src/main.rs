use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

use serde_json::json;

use crate::driver::{Driver, arduino::Arduino};
use crate::model::{Observation, Value};

mod driver;
mod model;
mod runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = serialport::new("COM3", 57_600)
        .timeout(Duration::from_secs(3))
        .open()?;

    std::thread::sleep(Duration::from_secs(2));

    let reader = BufReader::new(port);

    let driver = Arduino;

    let description = driver
        .signals()
        .into_iter()
        .find(|signal| signal.id == "sample_temperature")
        .expect("Arduino signal not found");

    let signal_id = format!("arduino.{}", description.id);

    println!("Signal: {} [{}]", signal_id, description.unit);

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

    let tasks = vec![runtime::PollTask {
        description,
        source: signal_id,
        period: Duration::from_secs(1),
    }];

    let worker = thread::spawn(move || {
        runtime::run_polling(driver, reader, tasks, tx);
    });

    for observation in rx {
        let mut guard = latest.lock().expect("Observation lock poisoned");

        *guard = Some(observation);
    }

    worker.join().expect("Reader thread panicked");

    Ok(())
}

fn observation_json(id: u64, observation: &Observation) -> io::Result<String> {
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
        "id": id,
        "source": &observation.source,
        "completed_at_unix_ms": timestamp_ms,
        "reading": reading
    });

    Ok(message.to_string())
}

fn parse_tcp_get(line: &str) -> Result<(u64, String), &'static str> {
    let request: serde_json::Value = serde_json::from_str(line).map_err(|_| "invalid_json")?;

    let id = request["id"]
        .as_u64()
        .filter(|id| *id > 0)
        .ok_or("invalid_id")?;

    if request["op"].as_str() != Some("get") {
        return Err("unsupported_op");
    }

    let key = request["key"]
        .as_str()
        .filter(|key| !key.is_empty())
        .ok_or("invalid_key")?;

    Ok((id, key.to_owned()))
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

        let (id, key) = match parse_tcp_get(&request) {
            Ok(parsed) => parsed,

            Err(error) => {
                writeln!(
                    stream,
                    "{}",
                    json!({
                        "status": "error",
                        "error": error
                    })
                )?;

                continue;
            }
        };

        if key != "sample_temperature" {
            writeln!(
                stream,
                "{}",
                json!({
                    "id": id,
                    "status": "error",
                    "error": "unknown_key"
                })
            )?;

            continue;
        }

        let response = {
            let guard = latest
                .lock()
                .map_err(|_| std::io::Error::other("Observation lock poisoned"))?;

            match guard.as_ref() {
                Some(observation) => observation_json(id, observation)?,
                None => json!({
                    "id": id,
                    "status": "no_data"})
                .to_string(),
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

        let text = observation_json(7, &observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["source"], "arduino.sample_temperature");
        assert_eq!(json["completed_at_unix_ms"], 1234);
        assert_eq!(json["reading"]["status"], "ok");
        assert_eq!(json["reading"]["value"]["value"], 20.0);
        assert_eq!(json["id"], 7);

        observation.reading = Err("Read timeout".to_string());

        let text = observation_json(7, &observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["reading"]["status"], "error");
        assert_eq!(json["reading"]["message"], "Read timeout")
    }

    #[test]
    fn parses_tcp_get() {
        let request = r#"{"id":7,"op":"get","key":"sample_temperature"}"#;

        let result = parse_tcp_get(request).unwrap();

        assert_eq!(result.0, 7);
        assert_eq!(result.1, "sample_temperature");

        assert_eq!(parse_tcp_get("GET"), Err("invalid_json"));

        assert_eq!(
            parse_tcp_get(r#"{"id":0,"op":"get","key":"power"}"#),
            Err("invalid_id")
        );

        assert_eq!(
            parse_tcp_get(r#"{"id":1,"op":"set","key":"power"}"#),
            Err("unsupported_op")
        );
    }
}
