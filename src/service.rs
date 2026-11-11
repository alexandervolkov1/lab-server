//! Shared device backend, cache and fanout. Transport adapters share one instance;
//! callers own workers and the aggregator loop. This module opens no ports.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::command_router::{CommandRouter, RouteError};
use crate::device_registry::{Availability, DeviceRegistry};
use crate::fanout::{ObservationFanout, ObservationReceiver};
use crate::model::{ActionDescription, Observation, ParameterDescription, Value};
use crate::observation_aggregator::{AggregatorEvent, ObservationAggregator};
use crate::session::{CommandBackend, EnqueueError, PendingRequests};

pub struct DeviceService {
    router: CommandRouter,
    pub(crate) catalog: HashMap<String, ParameterDescription>,
    pub(crate) latest: Mutex<HashMap<String, Observation>>,
    fanout: Arc<ObservationFanout>,
}

impl DeviceService {
    pub fn new(router: CommandRouter, observation_capacity: usize) -> Self {
        let catalog = router
            .registry()
            .parameters()
            .map(|p| {
                let mut description = p.description.clone();
                description.id = p.global_key.clone();
                (p.global_key.clone(), description)
            })
            .collect();
        let fanout = Arc::new(ObservationFanout::routed(
            observation_capacity,
            router.registry(),
        ));
        Self {
            router,
            catalog,
            latest: Mutex::new(HashMap::new()),
            fanout,
        }
    }

    pub fn registry(&self) -> &DeviceRegistry {
        self.router.registry()
    }

    pub fn register_client(&self) -> io::Result<ObservationReceiver> {
        self.fanout.register()
    }

    /// Dispatch one complete JSON text against the shared device context.
    pub fn receive_message<Input>(
        &self,
        session: &mut crate::session::SessionState<Input>,
        text: &str,
        now: Instant,
    ) -> io::Result<()> {
        session.receive_message(text, self, &self.catalog, &self.latest, now)
    }

    /// Bounded pump for the external server loop; no extra observation queue.
    pub fn collect(
        &self,
        aggregator: &mut ObservationAggregator<'_>,
        max_sources: usize,
    ) -> io::Result<usize> {
        let events = aggregator.poll(max_sources);
        let count = events.len();
        for event in events {
            self.apply_event(event)?;
        }
        Ok(count)
    }

    fn clear_port_cache(&self, port_id: &str) {
        self.latest
            .lock()
            .expect("observation lock poisoned")
            .retain(|key, _| {
                self.registry()
                    .parameter(key)
                    .is_some_and(|p| p.port_id != port_id)
            });
    }

    /// Preserve FIFO by applying the aggregator's returned events in order.
    pub fn apply_event(&self, event: AggregatorEvent) -> io::Result<()> {
        match event {
            AggregatorEvent::Measurement {
                port_id,
                global_key,
                mut observation,
            } => {
                // Defend the public entry point too: never publish a misbound key.
                if !self
                    .registry()
                    .parameter(&global_key)
                    .is_some_and(|p| p.port_id == port_id && p.description.id == observation.source)
                {
                    eprintln!(
                        "Invalid aggregated observation: port={port_id:?}, key={global_key:?}"
                    );
                    self.fanout.corrupt_port(&port_id)?;
                    self.clear_port_cache(&port_id);
                    return Ok(());
                }
                observation.source = global_key.clone();
                if self.available(&global_key) {
                    self.latest
                        .lock()
                        .map_err(|_| io::Error::other("observation lock poisoned"))?
                        .insert(global_key, observation.clone());
                }
                // Queued observations may precede EOF even after stop_reason was
                // recorded. Preserve that prefix for subscribers, not the cache.
                self.fanout.publish(&observation)?;
            }
            AggregatorEvent::DataLost {
                port_id,
                loss_epoch,
            } => self.fanout.report_loss(&port_id, loss_epoch)?,
            AggregatorEvent::PortStopped { port_id, reason } => {
                eprintln!("Observation port stopped: {port_id:?}: {reason:?}");
                self.fanout.close_port(&port_id)?;
                self.clear_port_cache(&port_id);
            }
            AggregatorEvent::SourceClosedWithoutStop { port_id } => {
                eprintln!("Observation source closed without worker stop: {port_id:?}");
                self.fanout.close_port(&port_id)?;
                self.clear_port_cache(&port_id);
            }
            AggregatorEvent::InvalidObservation {
                port_id,
                observation,
                error,
            } => {
                eprintln!(
                    "Invalid observation on {port_id:?}, device {:?}, source {:?}: {error:?}",
                    observation.device_id, observation.observation.source
                );
                self.fanout.corrupt_port(&port_id)?;
                self.clear_port_cache(&port_id);
            }
        }
        Ok(())
    }
}

fn route_error(error: RouteError) -> EnqueueError {
    match error {
        RouteError::UnknownSignal(_)
        | RouteError::UnknownDevice(_)
        | RouteError::UnknownAction(_) => EnqueueError::UnknownKey,
        RouteError::Unavailable { .. } | RouteError::Stopped { .. } => {
            EnqueueError::RuntimeUnavailable
        }
        RouteError::QueueFull { .. } => EnqueueError::RuntimeBusy,
        RouteError::ReadOnly { .. } => EnqueueError::ReadOnly,
        RouteError::InvalidValue { .. } => EnqueueError::InvalidValue,
    }
}

impl CommandBackend for DeviceService {
    fn actions(&self) -> Vec<ActionDescription> {
        self.registry()
            .actions()
            .map(|action| ActionDescription {
                name: action.global_key.clone(),
                description: action.description.description.clone(),
            })
            .collect()
    }

    fn enqueue_action(
        &self,
        requests: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        key: String,
    ) -> Result<(), EnqueueError> {
        requests.ensure_available(id)?;
        let pending = self
            .router
            .invoke_action(id, &key, deadline)
            .map_err(route_error)?;
        requests.add_routed_mutation(id, pending);
        Ok(())
    }
    fn enqueue_read(
        &self,
        requests: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        description: ParameterDescription,
    ) -> Result<(), EnqueueError> {
        requests.ensure_available(id)?;
        let pending = self
            .router
            .read(id, &description.id, deadline)
            .map_err(route_error)?;
        requests.add_routed_read(id, description.id, pending);
        Ok(())
    }
    fn enqueue_set(
        &self,
        requests: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        key: String,
        value: Value,
    ) -> Result<(), EnqueueError> {
        requests.ensure_available(id)?;
        let pending = self
            .router
            .set(id, &key, value, deadline)
            .map_err(route_error)?;
        requests.add_routed_mutation(id, pending);
        Ok(())
    }
    fn available(&self, key: &str) -> bool {
        self.registry().parameter(key).is_some_and(|p| {
            self.fanout.port_usable(&p.port_id)
                && self.router.availability(&p.device_id) == Ok(Availability::Available)
        })
    }
}
