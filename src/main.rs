use std::collections::{HashMap, HashSet};
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

    let driver = Arduino::new();

    let parameters = driver.parameters();

    let known_keys: Arc<HashSet<String>> = Arc::new(
        parameters
            .iter()
            .map(|parameter| parameter.id.clone())
            .collect(),
    );

    let tasks: Vec<runtime::PollTask> = parameters
        .into_iter()
        .map(|description| {
            let source = description.id.clone();

            println!("Signal: {} [{}]", source, description.unit);

            runtime::PollTask {
                description,
                source,
                period: Duration::from_secs(1),
            }
        })
        .collect();

    let (tx, rx) = mpsc::channel::<Observation>();
    let (command_tx, command_rx) = mpsc::channel::<runtime::SetCommand>();

    let latest = Arc::new(Mutex::new(HashMap::<String, Observation>::new()));

    let subscribers = Arc::new(Mutex::new(Vec::<mpsc::Sender<Observation>>::new()));

    let listener = TcpListener::bind("127.0.0.1:7240")?;

    println!("TCP server listening on {}", listener.local_addr()?);

    let tcp_latest = Arc::clone(&latest);
    let tcp_subscribers = Arc::clone(&subscribers);
    let tcp_commands = command_tx.clone();
    let tcp_known_keys = Arc::clone(&known_keys);

    let _tcp_worker = thread::spawn(move || {
        for connection in listener.incoming() {
            match connection {
                Ok(stream) => {
                    let client_latest = Arc::clone(&tcp_latest);
                    let client_subscribers = Arc::clone(&tcp_subscribers);
                    let client_commands = tcp_commands.clone();
                    let client_known_keys = Arc::clone(&tcp_known_keys);

                    thread::spawn(move || {
                        if let Err(error) = handle_client(
                            stream,
                            &client_latest,
                            &client_subscribers,
                            &client_commands,
                            &client_known_keys,
                        ) {
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
        runtime::run_polling(driver, reader, tasks, tx, command_rx);
    });

    for observation in rx {
        {
            let mut guard = latest.lock().expect("Observation lock poisoned");

            guard.insert(observation.source.clone(), observation.clone());
        }

        let mut clients = subscribers.lock().expect("Subscribers lock poisoned");

        clients.retain(|sender| sender.send(observation.clone()).is_ok());
    }

    worker.join().expect("Reader thread panicked");

    Ok(())
}

fn observation_json(id: Option<u64>, observation: &Observation) -> io::Result<String> {
    let duration = observation
        .completed_at
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;

    let timestamp_ms = u64::try_from(duration.as_millis()).map_err(io::Error::other)?;

    let mut message = match &observation.reading {
        Ok(value) => {
            let json_value = match value {
                Value::Float(value) => json!(value),
                Value::Integer(value) => json!(value),
            };
            json!({
                "status": "ok",
                "key": &observation.source,
                "value": json_value,
                "unit": &observation.unit,
                "completed_at_unix_ms": timestamp_ms
            })
        }

        Err(error) => json!({
            "status": "error",
            "key": &observation.source,
            "error": error,
            "completed_at_unix_ms": timestamp_ms
        }),
    };

    match id {
        Some(id) => message["id"] = json!(id),
        None => message["event"] = json!("measurement"),
    }

    Ok(message.to_string())
}

#[derive(Debug, PartialEq)]
enum TcpRequest {
    Get { id: u64, key: String },
    Subscribe { id: u64, key: String },
    Set { id: u64, key: String, value: Value },
}

fn parse_tcp_request(line: &str) -> Result<TcpRequest, &'static str> {
    let request: serde_json::Value = serde_json::from_str(line).map_err(|_| "invalid_json")?;

    let id = request["id"]
        .as_u64()
        .filter(|id| *id > 0)
        .ok_or("invalid_id")?;

    let key = request["key"]
        .as_str()
        .filter(|key| !key.is_empty())
        .ok_or("invalid_key")?
        .to_owned();

    match request["op"].as_str() {
        Some("get") => Ok(TcpRequest::Get { id, key }),
        Some("subscribe") => Ok(TcpRequest::Subscribe { id, key }),

        Some("set") => {
            let value = request["value"]
                .as_i64()
                .map(Value::Integer)
                .or_else(|| request["value"].as_f64().map(Value::Float))
                .ok_or("invalid_value")?;

            Ok(TcpRequest::Set { id, key, value })
        }

        _ => Err("unsupported_op"),
    }
}

fn handle_client(
    mut stream: TcpStream,
    latest: &Mutex<HashMap<String, Observation>>,
    subscribers: &Mutex<Vec<mpsc::Sender<Observation>>>,
    commands: &mpsc::Sender<runtime::SetCommand>,
    known_keys: &HashSet<String>,
) -> std::io::Result<()> {
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

        let command = match parse_tcp_request(&request) {
            Ok(command) => command,

            Err(error) => {
                let request_id = serde_json::from_str::<serde_json::Value>(&request)
                    .ok()
                    .and_then(|value| value["id"].as_u64())
                    .filter(|id| *id > 0);

                let mut response = json!({
                    "status": "error",
                    "error": error
                });

                if let Some(id) = request_id {
                    response["id"] = json!(id);
                }

                writeln!(stream, "{response}")?;

                continue;
            }
        };

        let (id, key) = match &command {
            TcpRequest::Get { id, key }
            | TcpRequest::Subscribe { id, key }
            | TcpRequest::Set { id, key, .. } => (*id, key.as_str()),
        };

        if !known_keys.contains(key) {
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

        match command {
            TcpRequest::Get { id, key } => {
                let response = {
                    let guard = latest
                        .lock()
                        .map_err(|_| io::Error::other("observation lock poisoned"))?;

                    match guard.get(&key) {
                        Some(observation) => observation_json(Some(id), observation)?,

                        None => json!({
                            "id": id,
                            "status": "no_data",
                            "key": key
                        })
                        .to_string(),
                    }
                };

                writeln!(stream, "{response}")?;
            }

            TcpRequest::Set { id, key, value } => {
                let (reply_tx, reply_rx) = mpsc::channel::<Result<(), String>>();

                let command = runtime::SetCommand {
                    parameter: key,
                    value,
                    reply: reply_tx,
                };

                if commands.send(command).is_err() {
                    writeln!(
                        stream,
                        "{}",
                        json!({
                            "id": id,
                            "status": "error",
                            "error": "runtime_unavailable"
                        })
                    )?;

                    continue;
                }

                let response = match reply_rx.recv_timeout(Duration::from_secs(10)) {
                    Ok(Ok(())) => json!({
                        "id": id,
                        "status": "ok"
                    }),

                    Ok(Err(error)) => json!({
                        "id": id,
                        "status": "error",
                        "error": error,
                    }),

                    Err(mpsc::RecvTimeoutError::Timeout) => json!({
                        "id": id,
                        "status": "error",
                        "error": "runtime_timeout"
                    }),

                    Err(mpsc::RecvTimeoutError::Disconnected) => json!({
                        "id": id,
                        "status": "error",
                        "error": "runtime_unavailable"
                    }),
                };

                writeln!(stream, "{response}")?;
            }

            TcpRequest::Subscribe { id, key } => {
                let (tx, rx) = mpsc::channel::<Observation>();

                subscribers
                    .lock()
                    .map_err(|_| io::Error::other("Subscribers lock poisoned"))?
                    .push(tx);

                writeln!(
                    stream,
                    "{}",
                    json!({
                        "id": id,
                        "status": "ok"
                    })
                )?;

                for observation in rx {
                    if observation.source != key {
                        continue;
                    }

                    let event = observation_json(None, &observation)?;

                    writeln!(stream, "{event}")?;
                }

                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_observation_as_json() {
        let mut observation = Observation {
            source: "sample_temperature".to_string(),
            unit: "degC".to_string(),
            completed_at: UNIX_EPOCH + Duration::from_millis(1234),
            reading: Ok(Value::Float(20.0)),
        };

        let text = observation_json(Some(7), &observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["id"], 7);
        assert_eq!(json["key"], "sample_temperature");
        assert_eq!(json["status"], "ok");
        assert_eq!(json["value"], 20.0);
        assert_eq!(json["unit"], "degC");
        assert_eq!(json["completed_at_unix_ms"], 1234);

        observation.reading = Err("Read timeout".to_string());

        let text = observation_json(Some(7), &observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["status"], "error");
        assert_eq!(json["error"], "Read timeout");

        let text = observation_json(None, &observation).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();

        assert_eq!(json["event"], "measurement");
        assert!(json.get("id").is_none());
    }

    #[test]
    fn parses_tcp_requests() {
        let request = r#"{"id":7,"op":"get","key":"sample_temperature"}"#;

        assert_eq!(
            parse_tcp_request(request),
            Ok(TcpRequest::Get {
                id: 7,
                key: "sample_temperature".to_string()
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":8,"op":"subscribe","key":"sample_temperature"}"#),
            Ok(TcpRequest::Subscribe {
                id: 8,
                key: "sample_temperature".to_string()
            })
        );

        assert_eq!(parse_tcp_request("GET"), Err("invalid_json"));

        assert_eq!(
            parse_tcp_request(r#"{"id":0,"op":"get","key":"power"}"#),
            Err("invalid_id")
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":9,"op":"set","key":"power","value":10}"#),
            Ok(TcpRequest::Set {
                id: 9,
                key: "power".to_string(),
                value: Value::Integer(10),
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":10,"op":"set","key":"power","value":10.5}"#),
            Ok(TcpRequest::Set {
                id: 10,
                key: "power".to_string(),
                value: Value::Float(10.5),
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":1,"op":"set","key":"power"}"#),
            Err("invalid_value")
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":1,"op":"unknown","key":"power"}"#),
            Err("unsupported_op")
        );
    }
}
