//! Scripted command admission for session tests, compiled only with the test module.
//! Uses the production pending/execution boundary; no worker, driver or physical I/O.

use std::sync::{Arc, mpsc};
use std::time::Instant;

use super::Cell;
use crate::model::{ParameterDescription, Value};
use crate::pending::{Pending, SetExecution};
use crate::port_worker::{CommandError, DeviceObservation};
use crate::session::{CommandBackend, EnqueueError, PendingRequests};

type MutationResult = Result<(), CommandError>;

pub(super) struct TestMutation {
    pub parameter: String,
    pub value: Value,
    pub execution: Arc<SetExecution>,
    reply: mpsc::SyncSender<MutationResult>,
}

impl TestMutation {
    /// Starting is explicit so tests can distinguish queued and started requests.
    pub fn complete(
        self,
        result: MutationResult,
    ) -> Result<(), mpsc::TrySendError<MutationResult>> {
        let mut sent = Ok(());
        self.execution
            .complete(|| sent = self.reply.try_send(result));
        sent
    }
}

pub(super) enum TestCommand {
    Read {
        description: ParameterDescription,
        reply: mpsc::SyncSender<Result<DeviceObservation, CommandError>>,
    },
    Set(TestMutation),
}

pub(super) struct TestBackend {
    commands: mpsc::SyncSender<TestCommand>,
    /// Attempts to reach the command queue, including full/closed failures.
    pub submit_attempts: Cell<usize>,
}

impl TestBackend {
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<TestCommand>) {
        let (commands, receiver) = mpsc::sync_channel(capacity);
        (
            Self {
                commands,
                submit_attempts: Cell::new(0),
            },
            receiver,
        )
    }

    fn submit(&self, command: TestCommand) -> Result<(), EnqueueError> {
        self.submit_attempts.set(self.submit_attempts.get() + 1);
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => EnqueueError::RuntimeBusy,
                mpsc::TrySendError::Disconnected(_) => EnqueueError::RuntimeUnavailable,
            })
    }
}

impl CommandBackend for TestBackend {
    fn enqueue_read(
        &self,
        requests: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        description: ParameterDescription,
    ) -> Result<(), EnqueueError> {
        requests.ensure_available(id)?;
        let key = description.id.clone();
        let (reply, receiver) = mpsc::sync_channel(1);
        self.submit(TestCommand::Read { description, reply })?;
        requests.add_routed_read(id, key, Pending::new(id, deadline, receiver));
        Ok(())
    }

    fn enqueue_set(
        &self,
        requests: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        parameter: String,
        value: Value,
    ) -> Result<(), EnqueueError> {
        requests.ensure_available(id)?;
        let (reply, receiver) = mpsc::sync_channel(1);
        let execution = SetExecution::new(deadline);
        self.submit(TestCommand::Set(TestMutation {
            parameter,
            value,
            execution: execution.clone(),
            reply,
        }))?;
        requests.add_routed_mutation(
            id,
            Pending::new(id, deadline, receiver).with_execution(execution),
        );
        Ok(())
    }
}
