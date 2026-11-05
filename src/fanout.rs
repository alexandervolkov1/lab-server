use std::io;
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, SyncSender};

use crate::model::Observation;

pub struct ObservationFanout {
    queue_capacity: usize,
    subscribers: Mutex<Vec<SyncSender<Observation>>>,
}

impl ObservationFanout {
    pub fn new(queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0);

        Self {
            queue_capacity,
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self) -> io::Result<Receiver<Observation>> {
        let (sender, receiver) = mpsc::sync_channel(self.queue_capacity);

        self.subscribers
            .lock()
            .map_err(|_| io::Error::other("subscriber lock poisoned"))?
            .push(sender);

        Ok(receiver)
    }

    pub fn publish(&self, observation: &Observation) -> io::Result<()> {
        let mut subscribers = self
            .subscribers
            .lock()
            .map_err(|_| io::Error::other("subscriber lock poisoned"))?;

        subscribers.retain(|sender| sender.try_send(observation.clone()).is_ok());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    use crate::model::Value;

    fn observation(value: f64) -> Observation {
        Observation {
            source: "sample_temperature".to_owned(),
            unit: "degC".to_owned(),
            completed_at: UNIX_EPOCH,
            reading: Ok(Value::Float(value)),
        }
    }

    #[test]
    fn publishes_to_multiple_clients() {
        let fanout = ObservationFanout::new(2);

        let first = fanout.register().unwrap();
        let second = fanout.register().unwrap();

        fanout.publish(&observation(20.0)).unwrap();

        assert_eq!(first.try_recv().unwrap().reading, Ok(Value::Float(20.0)));

        assert_eq!(second.try_recv().unwrap().reading, Ok(Value::Float(20.0)));
    }

    #[test]
    fn slow_client_does_not_block_other_clients() {
        let fanout = ObservationFanout::new(1);

        let slow = fanout.register().unwrap();
        let fast = fanout.register().unwrap();

        fanout.publish(&observation(20.0)).unwrap();

        assert_eq!(fast.try_recv().unwrap().reading, Ok(Value::Float(20.0)));

        fanout.publish(&observation(21.0)).unwrap();

        assert_eq!(fast.try_recv().unwrap().reading, Ok(Value::Float(21.0)));

        assert_eq!(slow.try_recv().unwrap().reading, Ok(Value::Float(20.0)));

        assert!(matches!(
            slow.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }
}
