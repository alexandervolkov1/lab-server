//! Adapter for the existing single-device runtime.
use crate::model::{Observation, ParameterDescription, Value};
use crate::{connection, runtime};
use std::sync::{Arc, mpsc};

pub use crate::protocol::observation_json;

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

impl connection::CommandBackend for mpsc::SyncSender<crate::runtime::DeviceCommand> {
    fn enqueue_read(
        &self,
        pending: &mut connection::PendingRequests,
        id: u64,
        deadline: std::time::Instant,
        description: ParameterDescription,
    ) -> Result<(), connection::EnqueueError> {
        pending.add_read(id, deadline, || crate::queue_read(self, description))
    }
    fn enqueue_set(
        &self,
        pending: &mut connection::PendingRequests,
        id: u64,
        deadline: std::time::Instant,
        key: String,
        value: Value,
    ) -> Result<(), connection::EnqueueError> {
        pending.add_set(id, deadline, |execution| {
            crate::queue_set(self, key, value, execution)
        })
    }
}
