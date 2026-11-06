//! Adapter for the existing single-device runtime.
use crate::model::{Observation, ParameterDescription, Value};
use crate::{connection, runtime};
use serde_json::json;
use std::io;
use std::sync::{Arc, mpsc};
use std::time::UNIX_EPOCH;

pub fn observation_json(id: Option<u64>, observation: &Observation) -> io::Result<String> {
    let duration = observation
        .completed_at
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;

    let timestamp_ms = u64::try_from(duration.as_millis()).map_err(io::Error::other)?;

    let mut message = match &observation.reading {
        Ok(value) => {
            let (json_value, unit) = match value {
                Value::Float(value) => (json!(value), observation.unit.as_str()),
                Value::Integer(value) => (json!(value), observation.unit.as_str()),
                Value::Boolean(value) => (json!(value), ""),
            };
            json!({
                "status": "ok",
                "key": &observation.source,
                "value": json_value,
                "unit": unit,
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

pub fn queue_read(
    commands: &mpsc::SyncSender<runtime::DeviceCommand>,
    description: ParameterDescription,
) -> Result<mpsc::Receiver<Observation>, connection::EnqueueError> {
    let (reply_tx, reply_rx) = mpsc::channel();

    let command = runtime::DeviceCommand::Read {
        description,
        reply: reply_tx,
    };

    commands.try_send(command).map_err(|error| match error {
        mpsc::TrySendError::Full(_) => connection::EnqueueError::RuntimeBusy,
        mpsc::TrySendError::Disconnected(_) => connection::EnqueueError::RuntimeUnavailable,
    })?;

    Ok(reply_rx)
}

pub fn queue_set(
    commands: &mpsc::SyncSender<runtime::DeviceCommand>,
    key: String,
    value: Value,
    execution: Arc<crate::pending::SetExecution>,
) -> Result<mpsc::Receiver<Result<(), String>>, connection::EnqueueError> {
    let (reply_tx, reply_rx) = mpsc::channel();

    let command = runtime::DeviceCommand::Set(runtime::SetCommand {
        execution,
        parameter: key,
        value,
        reply: reply_tx,
    });

    commands.try_send(command).map_err(|error| match error {
        mpsc::TrySendError::Full(_) => connection::EnqueueError::RuntimeBusy,
        mpsc::TrySendError::Disconnected(_) => connection::EnqueueError::RuntimeUnavailable,
    })?;

    Ok(reply_rx)
}
