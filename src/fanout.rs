use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, Weak};

use crate::model::Observation;

pub struct PublishedObservation {
    pub sequence: u64,
    pub observation: Observation,
}

pub struct ObservationReceiver {
    receiver: Receiver<PublishedObservation>,
    pub clock: Arc<AtomicU64>,
    pub interest: Arc<ObservationInterest>,
}

struct InterestState {
    active: bool,
    registered_epoch: u64,
    upstream_lost: bool,
    overflowed: bool,
}

pub struct ObservationInterest {
    state: Mutex<InterestState>,
    loss_epoch: Arc<AtomicU64>,
}

impl ObservationInterest {
    pub fn set_active(&self, active: bool) {
        let mut state = self
            .state
            .lock()
            .expect("observation interest lock poisoned");
        // This load is the transition boundary relative to upstream loss.
        let epoch = self.loss_epoch.load(Ordering::SeqCst);
        // Capture loss before leaving the active interval. Subscription changes
        // are not an acknowledgement: only notification and stream teardown finish it.
        if state.active && epoch != state.registered_epoch {
            state.upstream_lost = true;
        }
        if active && !state.active {
            // Ignore only losses from the inactive interval; retain earlier loss.
            state.registered_epoch = epoch;
        }
        state.active = active;
    }
}

impl ObservationReceiver {
    pub fn closed_error(&self) -> &'static str {
        let state = self
            .interest
            .state
            .lock()
            .expect("observation interest lock poisoned");
        if state.overflowed
            || state.upstream_lost
            || (state.active
                && self.interest.loss_epoch.load(Ordering::SeqCst) != state.registered_epoch)
        {
            "observation_data_lost"
        } else {
            "observation_stream_closed"
        }
    }

    pub fn try_recv(&self) -> Result<PublishedObservation, mpsc::TryRecvError> {
        let state = self
            .interest
            .state
            .lock()
            .expect("observation interest lock poisoned");
        if state.active
            && (state.upstream_lost
                || self.interest.loss_epoch.load(Ordering::SeqCst) != state.registered_epoch)
        {
            return Err(TryRecvError::Disconnected);
        }
        match self.receiver.try_recv() {
            // Preserve the queued prefix, then explicitly report personal overflow.
            Err(TryRecvError::Empty) if state.active && state.overflowed => {
                Err(TryRecvError::Disconnected)
            }
            result => result,
        }
    }
}

pub struct ObservationFanout {
    queue_capacity: usize,
    subscribers: Mutex<Vec<(Weak<ObservationInterest>, SyncSender<PublishedObservation>)>>,
    clock: Arc<AtomicU64>,
    loss_epoch: Arc<AtomicU64>,
}

impl ObservationFanout {
    pub fn new(queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0);

        Self {
            queue_capacity,
            clock: Arc::new(AtomicU64::new(0)),
            loss_epoch: Arc::new(AtomicU64::new(0)),
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn loss_epoch(&self) -> Arc<AtomicU64> {
        self.loss_epoch.clone()
    }

    pub fn register(&self) -> io::Result<ObservationReceiver> {
        let (sender, receiver) = mpsc::sync_channel(self.queue_capacity);
        let interest = Arc::new(ObservationInterest {
            state: Mutex::new(InterestState {
                active: false,
                registered_epoch: self.loss_epoch.load(Ordering::SeqCst),
                upstream_lost: false,
                overflowed: false,
            }),
            loss_epoch: self.loss_epoch.clone(),
        });
        let mut subscribers = self
            .subscribers
            .lock()
            .map_err(|_| io::Error::other("subscriber lock poisoned"))?;
        subscribers.retain(|(lifetime, _)| lifetime.strong_count() > 0);
        subscribers.push((Arc::downgrade(&interest), sender));
        Ok(ObservationReceiver {
            receiver,
            clock: self.clock.clone(),
            interest,
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
        subscribers.retain(|(interest, sender)| {
            let Some(interest) = interest.upgrade() else {
                return false;
            };
            let mut state = interest
                .state
                .lock()
                .expect("observation interest lock poisoned");
            if !state.active || state.overflowed {
                return true;
            }
            match sender.try_send(PublishedObservation {
                sequence,
                observation: observation.clone(),
            }) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    state.overflowed = true;
                    // Keep the loss sticky and pause until this stream closes.
                    true
                }
                Err(TrySendError::Disconnected(_)) => false,
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    use crate::model::Value;

    fn register_active(fanout: &ObservationFanout) -> ObservationReceiver {
        let receiver = fanout.register().unwrap();
        receiver.interest.set_active(true);
        receiver
    }

    #[test]
    fn loss_before_first_subscription_does_not_poison_subscription() {
        for upstream in [true, false] {
            let fanout = ObservationFanout::new(1);
            let receiver = fanout.register().unwrap();
            if upstream {
                fanout.loss_epoch().fetch_add(1, Ordering::SeqCst);
            } else {
                fanout.publish(&observation(20.0)).unwrap();
                fanout.publish(&observation(21.0)).unwrap();
            }
            receiver.interest.set_active(true);
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            fanout.publish(&observation(22.0)).unwrap();
            assert_eq!(
                receiver.try_recv().unwrap().observation.reading,
                Ok(Value::Float(22.0))
            );
        }
    }

    #[test]
    fn upstream_loss_closes_existing_streams_and_allows_reconnect() {
        let fanout = ObservationFanout::new(2);
        let first = register_active(&fanout);
        let second = register_active(&fanout);
        fanout.publish(&observation(20.0)).unwrap();
        fanout.loss_epoch().fetch_add(1, Ordering::SeqCst);
        assert!(matches!(
            first.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert!(matches!(
            second.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert_eq!(first.closed_error(), "observation_data_lost");
        let reconnected = register_active(&fanout);
        fanout.publish(&observation(21.0)).unwrap();
        assert_eq!(
            reconnected.try_recv().unwrap().observation.reading,
            Ok(Value::Float(21.0))
        );
    }

    #[test]
    fn disconnected_registrations_do_not_accumulate_without_publications() {
        let fanout = ObservationFanout::new(1);
        for _ in 0..1000 {
            drop(register_active(&fanout));
        }
        assert_eq!(fanout.subscribers.lock().unwrap().len(), 1);
    }

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

        let first = register_active(&fanout);
        let second = register_active(&fanout);

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

        let slow = register_active(&fanout);
        let fast = register_active(&fanout);

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
        assert_eq!(slow.closed_error(), "observation_data_lost");
    }

    #[test]
    fn personal_overflow_persists_until_receiver_is_replaced() {
        let fanout = ObservationFanout::new(1);
        let receiver = register_active(&fanout);
        fanout.publish(&observation(20.0)).unwrap();
        fanout.publish(&observation(21.0)).unwrap();
        receiver.interest.set_active(true);
        assert_eq!(
            receiver.try_recv().unwrap().observation.reading,
            Ok(Value::Float(20.0))
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert_eq!(receiver.closed_error(), "observation_data_lost");

        receiver.interest.set_active(false);
        fanout.publish(&observation(22.0)).unwrap();
        receiver.interest.set_active(true);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert_eq!(receiver.closed_error(), "observation_data_lost");
        // Reading an error does not acknowledge its delivery to a TCP client.
        let receiver = register_active(&fanout);
        fanout.publish(&observation(23.0)).unwrap();
        assert_eq!(
            receiver.try_recv().unwrap().observation.reading,
            Ok(Value::Float(23.0))
        );

        drop(fanout);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert_eq!(receiver.closed_error(), "observation_stream_closed");
    }
}
