//! Bounded, nonblocking collection from independent physical ports.
//!
//! DataLost is a notification about a port's stream, NOT a gap boundary. The
//! worker's failed send and counter increment are separate operations: a loss
//! may be reported after the affected measurement batch was already returned.
//! Equal counter snapshots never certify continuity. Consumers must invalidate
//! continuity for that port even across earlier batches when DataLost arrives.
//! At u64::MAX continuity stays unknown for the rest of the source's lifetime.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::mpsc::{Receiver, TryRecvError};

use crate::device_registry::DeviceRegistry;
use crate::model::Observation;
use crate::port_worker::{DeviceObservation, PortHandle, StopReason};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
    UnknownPort(String),
    UnavailablePort(String),
    DuplicatePort(String),
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPort(id) => write!(f, "observation source {id:?}: unknown port"),
            Self::UnavailablePort(id) => write!(f, "observation source {id:?}: port has no worker"),
            Self::DuplicatePort(id) => write!(f, "observation source {id:?}: duplicate receiver"),
        }
    }
}
impl std::error::Error for BuildError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingError {
    UnknownDevice,
    UnknownParameter,
    WrongPort { registered_port_id: String },
}

#[derive(Debug, Clone)]
pub enum AggregatorEvent {
    Measurement {
        port_id: String,
        global_key: String,
        /// Unmodified local source, time, unit, value type or reading error.
        observation: Observation,
    },
    /// Coalesced notification, never a marker locating the gap between events.
    /// MAX is a sticky loss of continuity, even if no more notifications arrive.
    DataLost { port_id: String, loss_epoch: u64 },
    /// Emitted once, after all queued observations and the final loss check.
    PortStopped { port_id: String, reason: StopReason },
    InvalidObservation {
        port_id: String,
        observation: DeviceObservation,
        error: MappingError,
    },
    /// EOF without a registered worker stop reason indicates a mismatched
    /// receiver or violated lifecycle contract. Do not invent a stop reason.
    SourceClosedWithoutStop { port_id: String },
}

#[derive(Default)]
struct LossTracker {
    reported: u64,
}

impl LossTracker {
    fn observe(&mut self, before: u64, after: u64) -> Option<u64> {
        // Never infer continuity from equality; MAX cannot advance further.
        let latest = self.reported.max(before).max(after);
        if latest == self.reported {
            None
        } else {
            self.reported = latest;
            Some(latest)
        }
    }
}

struct Source {
    port_id: String,
    receiver: Receiver<DeviceObservation>,
    handle: PortHandle,
    loss: LossTracker,
}

pub struct ObservationAggregator<'registry> {
    registry: &'registry DeviceRegistry,
    // One entry per source, no internal observation/event backlog.
    sources: VecDeque<Source>,
}

impl<'registry> ObservationAggregator<'registry> {
    /// Take a receiver for each selected registered port (a subset is allowed).
    /// Omit unavailable ports. A stopped worker's receiver may still be drained.
    /// The caller must pair each receiver with the actual worker's port id;
    /// std::mpsc does not expose sender identity. Payload ownership is checked
    /// against the registry on every observation.
    ///
    /// On construction failure the supplied receivers are dropped, as on Drop.
    pub fn new(
        registry: &'registry DeviceRegistry,
        receivers: impl IntoIterator<Item = (String, Receiver<DeviceObservation>)>,
    ) -> Result<Self, BuildError> {
        let mut sources = VecDeque::new();
        let mut seen = HashSet::new();
        for (port_id, receiver) in receivers {
            if !seen.insert(port_id.clone()) {
                return Err(BuildError::DuplicatePort(port_id));
            }
            let port = registry
                .port(&port_id)
                .ok_or_else(|| BuildError::UnknownPort(port_id.clone()))?;
            let handle = port
                .handle()
                .ok_or_else(|| BuildError::UnavailablePort(port_id.clone()))?
                .clone();
            sources.push_back(Source {
                port_id,
                receiver,
                handle,
                loss: LossTracker::default(),
            });
        }
        Ok(Self { registry, sources })
    }

    /// Visit at most min(max_sources, active sources) ports, round-robin, taking
    /// at most one observation from each. Returns at most 2 * visited events.
    /// Zero budget does no work. Empty sources consume a turn too, so neither
    /// busy nor idle ports cause an unbounded scan or starve another source.
    ///
    /// The counter is checked before AND after try_recv, including Empty and
    /// Disconnected. A change during this window is reported in this batch,
    /// before its measurement; the notification does not assert a gap position.
    /// A later concurrent increment will be detected on a later visit. After
    /// EOF the genuine worker can no longer increment its counter, so the final
    /// check precedes the terminal event. FIFO is preserved within each port;
    /// there is no cross-port time ordering or completeness guarantee.
    pub fn poll(&mut self, max_sources: usize) -> Vec<AggregatorEvent> {
        let visits = max_sources.min(self.sources.len());
        let mut events = Vec::new();
        for _ in 0..visits {
            let mut source = self.sources.pop_front().expect("bounded by source count");
            let (observation, before, after) =
                read_window(&source.receiver, || source.handle.loss_epoch());
            if let Some(loss_epoch) = source.loss.observe(before, after) {
                events.push(AggregatorEvent::DataLost {
                    port_id: source.port_id.clone(),
                    loss_epoch,
                });
            }
            match observation {
                Ok(observation) => events.push(self.map_observation(&source.port_id, observation)),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    events.push(match source.handle.stop_reason() {
                        Some(reason) => AggregatorEvent::PortStopped {
                            port_id: source.port_id,
                            reason,
                        },
                        None => AggregatorEvent::SourceClosedWithoutStop {
                            port_id: source.port_id,
                        },
                    });
                    continue;
                }
            }
            self.sources.push_back(source);
        }
        events
    }

    /// True after every selected receiver reached EOF and its terminal event
    /// was returned. An empty poll result alone does not mean completion.
    pub fn is_finished(&self) -> bool {
        self.sources.is_empty()
    }

    fn map_observation(&self, port_id: &str, observation: DeviceObservation) -> AggregatorEvent {
        let error = match self.registry.device(&observation.device_id) {
            None => Some(MappingError::UnknownDevice),
            Some(device) if device.port_id != port_id => Some(MappingError::WrongPort {
                registered_port_id: device.port_id.clone(),
            }),
            Some(_) => None,
        };
        if error.is_none()
            && let Some(parameter) = self.registry.observation_parameter(&observation)
        {
            return AggregatorEvent::Measurement {
                port_id: port_id.into(),
                global_key: parameter.global_key.clone(),
                observation: observation.observation,
            };
        }
        AggregatorEvent::InvalidObservation {
            port_id: port_id.into(),
            observation,
            error: error.unwrap_or(MappingError::UnknownParameter),
        }
    }
}

// Keep the actual receive between the two counter reads. The closure also lets
// tests deterministically interleave a producer at either boundary.
fn read_window(
    receiver: &Receiver<DeviceObservation>,
    mut loss_epoch: impl FnMut() -> u64,
) -> (Result<DeviceObservation, TryRecvError>, u64, u64) {
    let before = loss_epoch();
    let observation = receiver.try_recv();
    let after = loss_epoch();
    (observation, before, after)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
        mpsc,
    };
    use std::thread;

    #[test]
    fn loss_tracker_coalesces_and_never_resets_saturation() {
        let mut loss = LossTracker::default();
        assert_eq!(loss.observe(0, 0), None);
        assert_eq!(loss.observe(2, 5), Some(5));
        assert_eq!(loss.observe(5, 5), None);
        assert_eq!(loss.observe(5, 7), Some(7));
        assert_eq!(loss.observe(7, u64::MAX), Some(u64::MAX));
        assert_eq!(loss.observe(u64::MAX, u64::MAX), None);
        assert_eq!(loss.reported, u64::MAX);
    }

    #[test]
    fn concurrent_increment_after_receive_is_reported_in_the_same_window() {
        let epoch = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let (sender, receiver) = mpsc::channel();
        sender
            .send(DeviceObservation {
                device_id: "device".into(),
                observation: Observation {
                    source: "value".into(),
                    unit: "unit".into(),
                    completed_at: std::time::SystemTime::UNIX_EPOCH,
                    reading: Ok(crate::model::Value::Integer(1)),
                },
            })
            .unwrap();
        let producer_epoch = epoch.clone();
        let producer_barrier = barrier.clone();
        let producer = thread::spawn(move || {
            producer_barrier.wait();
            producer_epoch.store(3, Ordering::SeqCst);
            producer_barrier.wait();
            drop(sender);
        });
        let mut checks = 0;
        let (received, before, after) = read_window(&receiver, || {
            checks += 1;
            if checks == 2 {
                barrier.wait();
                barrier.wait();
            }
            epoch.load(Ordering::SeqCst)
        });
        assert!(received.is_ok());
        assert_eq!((before, after), (0, 3));
        assert_eq!(LossTracker::default().observe(before, after), Some(3));
        producer.join().unwrap();
    }

    #[test]
    fn concurrent_increment_after_window_is_still_reported_on_next_empty_visit() {
        let epoch = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let (_sender, receiver) = mpsc::channel();
        let producer_epoch = epoch.clone();
        let producer_barrier = barrier.clone();
        let producer = thread::spawn(move || {
            producer_barrier.wait();
            producer_epoch.store(1, Ordering::SeqCst);
            producer_barrier.wait();
        });
        let mut loss = LossTracker::default();
        let (_, before, after) = read_window(&receiver, || epoch.load(Ordering::SeqCst));
        assert_eq!(loss.observe(before, after), None);
        barrier.wait();
        barrier.wait();
        let (received, before, after) = read_window(&receiver, || epoch.load(Ordering::SeqCst));
        assert!(matches!(received, Err(TryRecvError::Empty)));
        assert_eq!(loss.observe(before, after), Some(1));
        producer.join().unwrap();
    }
}
