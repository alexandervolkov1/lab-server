use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, Weak};

use crate::model::Observation;

pub struct PublishedObservation {
    pub sequence: u64,
    pub observation: Observation,
}

pub struct ObservationReceiver {
    receiver: Receiver<PublishedObservation>,
    pub clock: Arc<AtomicU64>,
    _lifetime: Arc<()>,
}

impl ObservationReceiver {
    pub fn try_recv(&self) -> Result<PublishedObservation, mpsc::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub struct ObservationFanout {
    queue_capacity: usize,
    subscribers: Mutex<Vec<(Weak<()>, SyncSender<PublishedObservation>)>>,
    clock: Arc<AtomicU64>,
}

impl ObservationFanout {
    pub fn new(queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0);

        Self {
            queue_capacity,
            clock: Arc::new(AtomicU64::new(0)),
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn register(&self) -> io::Result<ObservationReceiver> {
        let (sender, receiver) = mpsc::sync_channel(self.queue_capacity);
        let lifetime = Arc::new(());
        let mut subscribers = self
            .subscribers
            .lock()
            .map_err(|_| io::Error::other("subscriber lock poisoned"))?;
        subscribers.retain(|(lifetime, _)| lifetime.strong_count() > 0);
        subscribers.push((Arc::downgrade(&lifetime), sender));
        Ok(ObservationReceiver {
            receiver,
            clock: self.clock.clone(),
            _lifetime: lifetime,
        })
    }

    pub fn publish(&self, observation: &Observation) -> io::Result<()> {
        let mut subscribers = self
            .subscribers
            .lock()
            .map_err(|_| io::Error::other("subscriber lock poisoned"))?;

        let sequence = self
            .clock
            .load(Ordering::SeqCst)
            .checked_add(1)
            .ok_or_else(|| io::Error::other("observation sequence exhausted"))?;
        // This increment is the publication point, serialized with other publishers.
        self.clock.store(sequence, Ordering::SeqCst);
        subscribers.retain(|(lifetime, sender)| {
            lifetime.strong_count() > 0
                && sender
                    .try_send(PublishedObservation {
                        sequence,
                        observation: observation.clone(),
                    })
                    .is_ok()
        });

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

        assert_eq!(
            first.try_recv().unwrap().observation.reading,
            Ok(Value::Float(20.0))
        );

        assert_eq!(
            second.try_recv().unwrap().observation.reading,
            Ok(Value::Float(20.0))
        );
    }

    #[test]
    fn slow_client_does_not_block_other_clients() {
        let fanout = ObservationFanout::new(1);

        let slow = fanout.register().unwrap();
        let fast = fanout.register().unwrap();

        fanout.publish(&observation(20.0)).unwrap();

        assert_eq!(
            fast.try_recv().unwrap().observation.reading,
            Ok(Value::Float(20.0))
        );

        fanout.publish(&observation(21.0)).unwrap();

        assert_eq!(
            fast.try_recv().unwrap().observation.reading,
            Ok(Value::Float(21.0))
        );

        assert_eq!(
            slow.try_recv().unwrap().observation.reading,
            Ok(Value::Float(20.0))
        );

        assert!(matches!(
            slow.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }
}
