//! Bounded request tracking and encoding of routed completions.
use super::EnqueueError;
use crate::pending::{Completion, Pending, Poll};
use serde_json::json;
use std::collections::VecDeque;
use std::io;
use std::time::Instant;

fn command_error_code(error: &crate::port_worker::CommandError) -> &'static str {
    use crate::port_worker::CommandError::*;
    match error {
        UnknownDevice | UnknownParameter | UnknownAction => "unknown_key",
        ReadOnly => "Parameter is read-only",
        InvalidValue => "Value does not match parameter spec",
        RuntimeTimeout => "runtime_timeout",
        OutcomeUnknown => "outcome_unknown",
        PortStopped(_) => "runtime_unavailable",
    }
}

fn routed_completion<T>(
    completion: Completion<Result<T, crate::port_worker::CommandError>>,
    success: impl FnOnce(u64, T) -> io::Result<String>,
) -> io::Result<String> {
    let (id, error) = match completion {
        Completion::Received {
            id,
            value: Ok(value),
        } => return success(id, value),
        Completion::Received {
            id,
            value: Err(error),
        } => (id, command_error_code(&error)),
        Completion::Timeout { id } => (id, "runtime_timeout"),
        Completion::Disconnected { id } => (id, "runtime_unavailable"),
        Completion::OutcomeUnknown { id } => (id, "outcome_unknown"),
    };
    Ok(json!({ "id": id, "status": "error", "error": error }).to_string())
}

pub enum FinishedRequest {
    RoutedRead {
        key: String,
        completion: Completion<
            Result<crate::port_worker::DeviceObservation, crate::port_worker::CommandError>,
        >,
    },
    RoutedMutation(Completion<Result<(), crate::port_worker::CommandError>>),
}

pub(super) fn completion_json(completion: FinishedRequest) -> io::Result<String> {
    match completion {
        FinishedRequest::RoutedRead { key, completion } => {
            routed_completion(completion, |id, mut value| {
                value.observation.source = key;
                crate::observation_json(Some(id), &value.observation)
            })
        }
        FinishedRequest::RoutedMutation(completion) => routed_completion(completion, |id, ()| {
            Ok(json!({ "id": id, "status": "ok" }).to_string())
        }),
    }
}

enum Entry {
    RoutedRead {
        id: u64,
        key: String,
        pending: Pending<
            Result<crate::port_worker::DeviceObservation, crate::port_worker::CommandError>,
        >,
    },
    RoutedMutation {
        id: u64,
        pending: Pending<Result<(), crate::port_worker::CommandError>>,
    },
}

impl Entry {
    fn id(&self) -> u64 {
        match self {
            Self::RoutedRead { id, .. } | Self::RoutedMutation { id, .. } => *id,
        }
    }
}

pub struct PendingRequests {
    limit: usize,
    entries: VecDeque<Entry>,
}

impl PendingRequests {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn contains_id(&self, id: u64) -> bool {
        self.entries.iter().any(|entry| entry.id() == id)
    }

    pub(crate) fn ensure_available(&self, id: u64) -> Result<(), EnqueueError> {
        if self.contains_id(id) {
            return Err(EnqueueError::DuplicateId);
        }

        if self.entries.len() >= self.limit {
            return Err(EnqueueError::Full);
        }

        Ok(())
    }

    pub(crate) fn add_routed_read(
        &mut self,
        id: u64,
        key: String,
        pending: Pending<
            Result<crate::port_worker::DeviceObservation, crate::port_worker::CommandError>,
        >,
    ) {
        self.entries
            .push_back(Entry::RoutedRead { id, key, pending });
    }
    /// Shared completion storage for set and invoke_action; admission must first
    /// pass ensure_available so both consume the same per-session capacity.
    pub(crate) fn add_routed_mutation(
        &mut self,
        id: u64,
        pending: Pending<Result<(), crate::port_worker::CommandError>>,
    ) {
        self.entries
            .push_back(Entry::RoutedMutation { id, pending });
    }

    pub fn poll(&mut self, now: Instant) -> Vec<FinishedRequest> {
        let mut finished = Vec::new();
        let count = self.entries.len();

        for _ in 0..count {
            let entry = self.entries.pop_front().expect("entry count is known");

            match entry {
                Entry::RoutedRead { id, key, pending } => match pending.poll(now) {
                    Poll::Waiting(pending) => {
                        self.entries
                            .push_back(Entry::RoutedRead { id, key, pending })
                    }
                    Poll::Finished(completion) => {
                        finished.push(FinishedRequest::RoutedRead { key, completion })
                    }
                },
                Entry::RoutedMutation { id, pending } => match pending.poll(now) {
                    Poll::Waiting(pending) => self
                        .entries
                        .push_back(Entry::RoutedMutation { id, pending }),
                    Poll::Finished(completion) => {
                        finished.push(FinishedRequest::RoutedMutation(completion))
                    }
                },
            }
        }

        finished
    }
}
