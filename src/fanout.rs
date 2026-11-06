use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, Weak};

use crate::device_registry::DeviceRegistry;
use crate::model::Observation;
use crate::port_worker::PortHandle;

pub struct PublishedObservation {
    pub sequence: u64,
    pub observation: Observation,
}

pub struct ObservationReceiver {
    receiver: Receiver<PublishedObservation>,
    pub clock: Arc<AtomicU64>,
    pub interest: Arc<ObservationInterest>,
}

struct PortStream {
    handle: Option<PortHandle>,
    reported_epoch: AtomicU64,
    closed: AtomicBool,
    corrupted: AtomicBool,
}
impl PortStream {
    fn epoch(&self) -> u64 {
        self.reported_epoch
            .load(Ordering::SeqCst)
            .max(self.handle.as_ref().map_or(0, PortHandle::loss_epoch))
    }
}
struct Domains {
    keys: HashMap<String, String>,
    ports: BTreeMap<String, PortStream>,
}

struct InterestState {
    active: bool,
    registered_epoch: u64,
    upstream_lost: bool,
    overflowed: bool,
    stream_closed: bool,
    keys: HashSet<String>,
    ports: BTreeMap<String, u64>,
}

pub struct ObservationInterest {
    state: Mutex<InterestState>,
    loss_epoch: Arc<AtomicU64>,
    domains: Option<Arc<Domains>>,
}

impl ObservationInterest {
    /// Compatibility entry point for the single-device runtime.
    pub fn set_active(&self, active: bool) {
        assert!(
            self.domains.is_none(),
            "routed subscriptions must specify keys"
        );
        let mut state = self
            .state
            .lock()
            .expect("observation interest lock poisoned");
        let epoch = self.loss_epoch.load(Ordering::SeqCst);
        if state.active && epoch != state.registered_epoch {
            state.upstream_lost = true;
        }
        if active && !state.active {
            state.registered_epoch = epoch;
        }
        state.active = active;
    }

    /// Linearize subscription boundaries against each affected worker's
    /// publication + loss commit. All affected gates are held together in port
    /// id order. The worker never takes interest/subscriber locks, and its gate
    /// never covers driver I/O. No network calls occur under these locks.
    pub fn set_keys(&self, keys: &[String]) {
        let Some(domains) = &self.domains else {
            self.set_active(!keys.is_empty());
            return;
        };
        let mut state = self
            .state
            .lock()
            .expect("observation interest lock poisoned");
        let next_ports: BTreeSet<_> = keys
            .iter()
            .filter_map(|key| domains.keys.get(key).cloned())
            .collect();
        let affected: BTreeSet<_> = state
            .ports
            .keys()
            .cloned()
            .chain(next_ports.iter().cloned())
            .collect();
        let _boundaries: Vec<_> = affected
            .iter()
            .filter_map(|id| domains.ports[id].handle.as_ref())
            .map(PortHandle::observation_boundary)
            .collect();
        self.refresh(&mut state);
        let mut next = BTreeMap::new();
        for id in next_ports {
            let port = &domains.ports[&id];
            let epoch = port.epoch();
            if epoch == u64::MAX || port.corrupted.load(Ordering::SeqCst) {
                state.upstream_lost = true;
            }
            if port.closed.load(Ordering::SeqCst) {
                state.stream_closed = true;
            }
            next.insert(id.clone(), state.ports.get(&id).copied().unwrap_or(epoch));
        }
        state.ports = next;
        state.keys = keys.iter().cloned().collect();
        state.active = !keys.is_empty();
    }

    fn refresh(&self, state: &mut InterestState) {
        if let Some(domains) = &self.domains {
            for (id, baseline) in &state.ports {
                let port = &domains.ports[id];
                let epoch = port.epoch();
                if epoch != *baseline || epoch == u64::MAX || port.corrupted.load(Ordering::SeqCst)
                {
                    state.upstream_lost = true;
                }
                if port.closed.load(Ordering::SeqCst) {
                    state.stream_closed = true;
                }
            }
        } else if state.active && self.loss_epoch.load(Ordering::SeqCst) != state.registered_epoch {
            state.upstream_lost = true;
        }
    }
}

impl ObservationReceiver {
    pub fn closed_error(&self) -> &'static str {
        let mut state = self
            .interest
            .state
            .lock()
            .expect("observation interest lock poisoned");
        self.interest.refresh(&mut state);
        if state.overflowed || state.upstream_lost {
            "observation_data_lost"
        } else {
            "observation_stream_closed"
        }
    }

    /// In routed mode an unsubscribe cannot acknowledge an already detected
    /// stream failure, even if no subscriptions remain at the next TCP turn.
    pub fn terminal_when_inactive(&self) -> bool {
        let state = self
            .interest
            .state
            .lock()
            .expect("observation interest lock poisoned");
        self.interest.domains.is_some()
            && (state.upstream_lost || state.overflowed || state.stream_closed)
    }

    pub fn try_recv(&self) -> Result<PublishedObservation, TryRecvError> {
        let mut state = self
            .interest
            .state
            .lock()
            .expect("observation interest lock poisoned");
        self.interest.refresh(&mut state);
        let relevant = state.active || self.interest.domains.is_some();
        if relevant && state.upstream_lost {
            return Err(TryRecvError::Disconnected);
        }
        match self.receiver.try_recv() {
            // Preserve the queued prefix for personal overflow and orderly EOF.
            Err(TryRecvError::Empty) if relevant && (state.overflowed || state.stream_closed) => {
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
    domains: Option<Arc<Domains>>,
}

impl ObservationFanout {
    pub fn new(queue_capacity: usize) -> Self {
        assert!(queue_capacity > 0);
        Self {
            queue_capacity,
            clock: Arc::new(AtomicU64::new(0)),
            loss_epoch: Arc::new(AtomicU64::new(0)),
            subscribers: Mutex::new(Vec::new()),
            domains: None,
        }
    }

    pub fn routed(queue_capacity: usize, registry: &DeviceRegistry) -> Self {
        let mut fanout = Self::new(queue_capacity);
        fanout.domains = Some(Arc::new(Domains {
            keys: registry
                .parameters()
                .map(|p| (p.global_key.clone(), p.port_id.clone()))
                .collect(),
            ports: registry
                .ports()
                .map(|p| {
                    (
                        p.id().into(),
                        PortStream {
                            handle: p.handle().cloned(),
                            reported_epoch: AtomicU64::new(0),
                            closed: AtomicBool::new(p.handle().is_none()),
                            corrupted: AtomicBool::new(false),
                        },
                    )
                })
                .collect(),
        }));
        fanout
    }

    /// Legacy single-runtime counter. Routed mode reads each PortHandle counter.
    pub fn loss_epoch(&self) -> Arc<AtomicU64> {
        self.loss_epoch.clone()
    }

    fn port(&self, id: &str) -> io::Result<&PortStream> {
        self.domains
            .as_ref()
            .and_then(|d| d.ports.get(id))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown stream port {id:?}"),
                )
            })
    }
    pub(crate) fn report_loss(&self, id: &str, epoch: u64) -> io::Result<()> {
        self.port(id)?
            .reported_epoch
            .fetch_max(epoch, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) fn close_port(&self, id: &str) -> io::Result<()> {
        self.port(id)?.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) fn corrupt_port(&self, id: &str) -> io::Result<()> {
        self.port(id)?.corrupted.store(true, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) fn port_usable(&self, id: &str) -> bool {
        self.port(id)
            .is_ok_and(|p| !p.closed.load(Ordering::SeqCst) && !p.corrupted.load(Ordering::SeqCst))
    }

    pub fn register(&self) -> io::Result<ObservationReceiver> {
        let (sender, receiver) = mpsc::sync_channel(self.queue_capacity);
        let interest = Arc::new(ObservationInterest {
            state: Mutex::new(InterestState {
                active: false,
                registered_epoch: self.loss_epoch.load(Ordering::SeqCst),
                upstream_lost: false,
                overflowed: false,
                stream_closed: false,
                keys: HashSet::new(),
                ports: BTreeMap::new(),
            }),
            loss_epoch: self.loss_epoch.clone(),
            domains: self.domains.clone(),
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
        self.clock.store(sequence, Ordering::SeqCst);
        subscribers.retain(|(interest, sender)| {
            let Some(interest) = interest.upgrade() else {
                return false;
            };
            let mut state = interest
                .state
                .lock()
                .expect("observation interest lock poisoned");
            interest.refresh(&mut state);
            if !state.active
                || state.overflowed
                || state.stream_closed
                || state.upstream_lost
                || (interest.domains.is_some() && !state.keys.contains(&observation.source))
            {
                return true;
            }
            match sender.try_send(PublishedObservation {
                sequence,
                observation: observation.clone(),
            }) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    state.overflowed = true;
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
