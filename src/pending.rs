use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Clone, Copy)]
enum ExecutionState {
    Queued,
    Started,
    Cancelled,
}

/// Shared execution boundary for state-changing operations (set and actions).
pub struct SetExecution {
    deadline: Instant,
    state: Mutex<ExecutionState>,
}

impl SetExecution {
    pub fn new(deadline: Instant) -> Arc<Self> {
        Arc::new(Self {
            deadline,
            state: Mutex::new(ExecutionState::Queued),
        })
    }

    pub fn try_start(&self) -> bool {
        let mut state = self.state.lock().expect("execution lock poisoned");
        // Read the clock inside the lock: a waiter must not use a stale timestamp.
        if matches!(*state, ExecutionState::Queued) && Instant::now() < self.deadline {
            *state = ExecutionState::Started;
            true
        } else {
            false
        }
    }

    pub fn complete(&self, send: impl FnOnce()) {
        let _state = self.state.lock().expect("execution lock poisoned");
        send();
    }
}

pub struct Pending<T> {
    id: u64,
    deadline: Instant,
    reply: Receiver<T>,
    execution: Option<Arc<SetExecution>>,
}

pub enum Completion<T> {
    Received { id: u64, value: T },
    Timeout { id: u64 },
    Disconnected { id: u64 },
    OutcomeUnknown { id: u64 },
}

pub enum Poll<T> {
    Waiting(Pending<T>),
    Finished(Completion<T>),
}

impl<T> Pending<T> {
    pub fn new(id: u64, deadline: Instant, reply: Receiver<T>) -> Self {
        Self {
            id,
            deadline,
            reply,
            execution: None,
        }
    }

    pub fn with_execution(mut self, execution: Arc<SetExecution>) -> Self {
        self.execution = Some(execution);
        self
    }

    pub fn poll(self, now: Instant) -> Poll<T> {
        let execution = self.execution.clone();
        let mut state = execution
            .as_ref()
            .map(|execution| execution.state.lock().expect("execution lock poisoned"));
        match self.reply.try_recv() {
            Ok(value) => Poll::Finished(Completion::Received { id: self.id, value }),

            Err(TryRecvError::Disconnected) => {
                if state
                    .as_deref()
                    .is_some_and(|s| matches!(s, ExecutionState::Started))
                {
                    Poll::Finished(Completion::OutcomeUnknown { id: self.id })
                } else {
                    if let Some(state) = state.as_deref_mut() {
                        *state = ExecutionState::Cancelled;
                    }
                    Poll::Finished(Completion::Disconnected { id: self.id })
                }
            }

            Err(TryRecvError::Empty) if now >= self.deadline => {
                if state
                    .as_deref()
                    .is_some_and(|s| matches!(s, ExecutionState::Started))
                {
                    Poll::Finished(Completion::OutcomeUnknown { id: self.id })
                } else {
                    if let Some(state) = state.as_deref_mut() {
                        *state = ExecutionState::Cancelled;
                    }
                    Poll::Finished(Completion::Timeout { id: self.id })
                }
            }

            Err(TryRecvError::Empty) => Poll::Waiting(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn timeout_and_start_have_one_winner() {
        for _ in 0..100 {
            let deadline = Instant::now() + Duration::from_secs(3600);
            let execution = SetExecution::new(deadline);
            let (tx, rx) = mpsc::channel();
            let pending = Pending::new(1, deadline, rx).with_execution(execution.clone());
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let worker_barrier = barrier.clone();
            let worker = std::thread::spawn(move || {
                worker_barrier.wait();
                let started = execution.try_start();
                if started {
                    execution.complete(|| {
                        let _ = tx.send(());
                    });
                }
                started
            });
            barrier.wait();
            let completion = pending.poll(deadline);
            let started = worker.join().unwrap();
            match completion {
                Poll::Finished(Completion::Timeout { .. } | Completion::Disconnected { .. }) => {
                    assert!(!started)
                }
                Poll::Finished(Completion::OutcomeUnknown { .. } | Completion::Received { .. }) => {
                    assert!(started)
                }
                Poll::Waiting(_) => panic!("deadline must finish the request"),
            }
        }
    }

    #[test]
    fn started_set_with_disconnected_reply_has_unknown_outcome() {
        let deadline = Instant::now() + Duration::from_secs(3600);
        let execution = SetExecution::new(deadline);
        let (tx, rx) = mpsc::channel::<()>();
        assert!(execution.try_start());
        drop(tx);
        assert!(matches!(
            Pending::new(1, deadline, rx)
                .with_execution(execution)
                .poll(Instant::now()),
            Poll::Finished(Completion::OutcomeUnknown { id: 1 })
        ));
    }

    #[test]
    fn waits_then_receives_result() {
        let (sender, receiver) = mpsc::channel();
        let now = Instant::now();

        let pending = Pending::new(7, now + Duration::from_secs(5), receiver);

        let pending = match pending.poll(now) {
            Poll::Waiting(pending) => pending,
            Poll::Finished(_) => panic!("completed too early"),
        };

        sender.send(42).unwrap();

        assert!(matches!(
            pending.poll(now),
            Poll::Finished(Completion::Received { id: 7, value: 42 })
        ));
    }

    #[test]
    fn times_out_without_sleeping() {
        let (_sender, receiver) = mpsc::channel::<i32>();
        let now = Instant::now();

        let pending = Pending::new(8, now + Duration::from_secs(5), receiver);

        assert!(matches!(
            pending.poll(now + Duration::from_secs(5)),
            Poll::Finished(Completion::Timeout { id: 8 })
        ));
    }

    #[test]
    fn detects_disconnected_reply_channel() {
        let (sender, receiver) = mpsc::channel::<i32>();
        drop(sender);

        let pending = Pending::new(9, Instant::now() + Duration::from_secs(5), receiver);

        assert!(matches!(
            pending.poll(Instant::now()),
            Poll::Finished(Completion::Disconnected { id: 9 })
        ));
    }

    #[test]
    fn available_result_takes_priority_over_timeout() {
        let (sender, receiver) = mpsc::channel();
        sender.send(99).unwrap();

        let now = Instant::now();
        let pending = Pending::new(10, now, receiver);

        assert!(matches!(
            pending.poll(now),
            Poll::Finished(Completion::Received { id: 10, value: 99 })
        ));
    }
}
