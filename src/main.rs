use std::collections::HashMap;
use std::io::{self, BufReader};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

#[cfg(test)]
use lab_server::pending;
use lab_server::{client_limit, connection, driver, fanout, model, runtime};

use crate::connection::TcpConnection;
use crate::driver::{Driver, arduino::Arduino};
use crate::fanout::ObservationFanout;
use crate::model::{Observation, ParameterDescription};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = serialport::new("COM3", 57_600)
        .timeout(Duration::from_secs(3))
        .open()?;

    std::thread::sleep(Duration::from_secs(2));

    let reader = BufReader::new(port);

    let driver = Arduino::new();

    let parameters = driver.parameters();

    let catalog: Arc<HashMap<String, ParameterDescription>> = Arc::new(
        parameters
            .iter()
            .map(|parameter| (parameter.id.clone(), parameter.clone()))
            .collect(),
    );

    let tasks: Vec<runtime::PollTask> = parameters
        .into_iter()
        .filter_map(|description| {
            let period = description.default_poll_period?;
            let source = description.id.clone();

            println!("Signal: {} [{}]", source, description.unit);

            Some(runtime::PollTask {
                description,
                source,
                period,
            })
        })
        .collect();

    let (tx, rx) = mpsc::sync_channel::<Observation>(runtime::OBSERVATION_QUEUE_CAPACITY);
    let (command_tx, command_rx) =
        mpsc::sync_channel::<runtime::DeviceCommand>(runtime::COMMAND_QUEUE_CAPACITY);

    let latest = Arc::new(Mutex::new(HashMap::<String, Observation>::new()));

    let fanout = Arc::new(ObservationFanout::new(128));
    let tx = runtime::ObservationOutput::new(tx, fanout.loss_epoch());

    let listener = TcpListener::bind("127.0.0.1:7240")?;

    println!("TCP server listening on {}", listener.local_addr()?);

    let tcp_latest = Arc::clone(&latest);
    let tcp_fanout = Arc::clone(&fanout);
    let tcp_commands = command_tx.clone();
    let tcp_catalog = Arc::clone(&catalog);

    let slots = client_limit::ClientSlots::new();
    let _tcp_worker = thread::spawn(move || {
        for connection in listener.incoming() {
            match connection {
                Ok(stream) => {
                    let Some(slot) = slots.try_acquire() else {
                        drop(stream);
                        continue;
                    };
                    let client_latest = Arc::clone(&tcp_latest);
                    let client_fanout = Arc::clone(&tcp_fanout);
                    let client_commands = tcp_commands.clone();
                    let client_catalog = Arc::clone(&tcp_catalog);

                    if let Err(error) = thread::Builder::new().spawn(move || {
                        let _slot = slot;
                        let result = (|| -> io::Result<()> {
                            let mut connection = TcpConnection::new(stream, 64, 64 * 1024)?;

                            let observations = client_fanout.register()?;

                            connection.run(
                                &client_commands,
                                &client_catalog,
                                &client_latest,
                                &observations,
                            )
                        })();

                        if let Err(error) = result {
                            eprintln!("TCP client error: {error}");
                        }
                    }) {
                        eprintln!("TCP worker spawn error: {error}");
                    }
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

        fanout.publish(&observation)?;
    }

    worker.join().expect("Reader thread panicked");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lab_server::{model::Value, observation_json, queue_read, queue_set};
    use std::time::UNIX_EPOCH;

    #[test]
    fn device_queue_rejects_overflow_without_blocking() {
        let (tx, _rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        for _ in 0..128 {
            assert!(
                queue_set(
                    &tx,
                    "power".into(),
                    Value::Integer(1),
                    crate::pending::SetExecution::new(
                        std::time::Instant::now() + Duration::from_secs(10)
                    )
                )
                .is_ok()
            );
        }
        assert!(
            queue_set(
                &tx,
                "power".into(),
                Value::Integer(1),
                crate::pending::SetExecution::new(
                    std::time::Instant::now() + Duration::from_secs(10)
                )
            )
            .is_err()
        );
    }

    #[test]
    fn queues_read_without_waiting_for_device() {
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let description = ParameterDescription {
            id: "configuration".to_owned(),
            spec: crate::model::Spec::Integer { min: 0, max: 100 },
            unit: "".to_owned(),
            writable: false,
            default_poll_period: None,
        };

        let reply_rx = queue_read(&command_tx, description.clone()).unwrap();

        assert!(matches!(
            reply_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let command = command_rx.try_recv().unwrap();

        match command {
            runtime::DeviceCommand::Read {
                description: queued,
                reply,
            } => {
                assert_eq!(queued.id, description.id);
                assert_eq!(queued.default_poll_period, description.default_poll_period);

                reply
                    .send(Observation {
                        source: queued.id,
                        unit: queued.unit,
                        completed_at: UNIX_EPOCH + Duration::from_millis(10),
                        reading: Ok(Value::Integer(30)),
                    })
                    .unwrap();
            }

            _ => panic!("expected Read command"),
        }

        let observation = reply_rx.try_recv().unwrap();

        assert_eq!(observation.source, "configuration");
        assert_eq!(observation.reading, Ok(Value::Integer(30)));

        let (closed_tx, closed_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        drop(closed_rx);

        assert!(queue_read(&closed_tx, description).is_err());
    }

    #[test]
    fn queues_set_without_waiting_for_device() {
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let reply_rx = queue_set(
            &command_tx,
            "power".to_owned(),
            Value::Integer(25),
            crate::pending::SetExecution::new(std::time::Instant::now() + Duration::from_secs(10)),
        )
        .unwrap();

        assert!(matches!(
            reply_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let command = command_rx.try_recv().unwrap();

        match command {
            runtime::DeviceCommand::Set(command) => {
                assert_eq!(command.parameter, "power");
                assert_eq!(command.value, Value::Integer(25));

                command.reply.send(Ok(())).unwrap();
            }

            _ => panic!("expected Set command"),
        }

        assert_eq!(reply_rx.try_recv(), Ok(Ok(())));

        let (closed_tx, closed_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        drop(closed_rx);

        assert!(
            queue_set(
                &closed_tx,
                "power".to_owned(),
                Value::Integer(25),
                crate::pending::SetExecution::new(
                    std::time::Instant::now() + Duration::from_secs(10)
                )
            )
            .is_err()
        );
    }

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
}
