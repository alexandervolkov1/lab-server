use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Instant;

pub struct Pending<T> {
    id: u64,
    deadline: Instant,
    reply: Receiver<T>,
}

pub enum Completion<T> {
    Received { id: u64, value: T },
    Timeout { id: u64 },
    Disconnected { id: u64 },
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
        }
    }

    pub fn poll(self, now: Instant) -> Poll<T> {
        match self.reply.try_recv() {
            Ok(value) => Poll::Finished(Completion::Received { id: self.id, value }),

            Err(TryRecvError::Disconnected) => {
                Poll::Finished(Completion::Disconnected { id: self.id })
            }

            Err(TryRecvError::Empty) if now >= self.deadline => {
                Poll::Finished(Completion::Timeout { id: self.id })
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
