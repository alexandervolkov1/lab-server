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
    gate: Mutex<()>,
    processing: Mutex<crate::processing::Engine>,
    continuity: Mutex<HashMap<String, crate::continuity::Continuity>>,
    router: CommandRouter,
    pub(crate) catalog: HashMap<String, ParameterDescription>,
    pub(crate) latest: Mutex<HashMap<String, Observation>>,
    fanout: Arc<ObservationFanout>,
}

impl DeviceService {
    pub(crate) fn lock_service(&self) -> std::sync::MutexGuard<'_, ()> {
        self.gate.lock().expect("service lock poisoned")
    }
    pub fn manage(
        &self,
        command: crate::processing::Management,
    ) -> Result<serde_json::Value, String> {
        let _gate = self.lock_service();
        self.manage_inner(command)
    }
    pub(crate) fn manage_inner(
        &self,
        command: crate::processing::Management,
    ) -> Result<serde_json::Value, String> {
        use crate::processing::Management::*;
        let mut engine = self.processing.lock().expect("processing lock poisoned");
        match command {
            Types => return Ok(engine.types()),
            List => return Ok(engine.list()),
            Create {
                configuration,
                expected_revision,
            } => engine.create(configuration, expected_revision)?,
            Configure {
                configuration,
                expected_revision,
            } => engine.configure(configuration, expected_revision)?,
            Delete {
                name,
                cascade,
                expected_revision,
            } => {
                engine.remove(&name, cascade, expected_revision)?;
            }
            State {
                name,
                state,
                expected_revision,
            } => {
                if state == crate::processing::State::Auto {
                    return Err("output_not_bound".into());
                }
                engine.set_state(&name, state, expected_revision)?;
            }
        }
        let catalog = engine.catalog();
        let physical = self
            .registry()
            .parameters()
            .map(|p| {
                (
                    p.global_key.clone(),
                    std::collections::BTreeSet::from([p.port_id.clone()]),
                )
            })
            .collect();
        self.fanout.update_keys(engine.lineage(&physical));
        self.latest
            .lock()
            .expect("cache lock poisoned")
            .retain(|k, _| catalog.contains_key(k));
        Ok(engine.list())
    }
    pub fn apply_event(&self, event: AggregatorEvent) -> io::Result<()> {
        self.apply_event_recorded(event, |_| {})
    }
    /// One serialized commit boundary for original + derived events and recording.
    pub fn apply_event_recorded(
        &self,
        event: AggregatorEvent,
        mut record: impl FnMut(&AggregatorEvent),
    ) -> io::Result<()> {
        let _gate = self.lock_service();
        record(&event);
        let mut produced = Vec::new();
        match &event {
            AggregatorEvent::Measurement {
                port_id,
                global_key,
                observation,
            } => {
                let bound = self.registry().parameter(global_key).is_some_and(|p| {
                    p.port_id == *port_id && p.description.id == observation.source
                });
                let accepted = bound
                    && observation.metadata.as_ref().is_some_and(|m| {
                        self.continuity
                            .lock()
                            .unwrap()
                            .get_mut(port_id)
                            .is_some_and(|c| c.accept(m))
                    });
                if accepted {
                    produced = self.processing.lock().unwrap().process(
                        global_key.clone(),
                        observation.clone(),
                        Instant::now(),
                    );
                }
            }
            _ => {
                let (port, reason) = match &event {
                    AggregatorEvent::DataLost {
                        port_id,
                        loss_epoch,
                    } => {
                        if let Some(c) = self.continuity.lock().unwrap().get_mut(port_id) {
                            c.lost(*loss_epoch);
                        }
                        (port_id, "source_data_lost")
                    }
                    AggregatorEvent::PortStopped { port_id, .. }
                    | AggregatorEvent::SourceClosedWithoutStop { port_id }
                    | AggregatorEvent::InvalidObservation { port_id, .. } => {
                        if let Some(c) = self.continuity.lock().unwrap().get_mut(port_id) {
                            c.stop();
                        }
                        (port_id, "source_unavailable")
                    }
                    _ => unreachable!(),
                };
                let keys = self
                    .registry()
                    .parameters()
                    .filter(|p| p.port_id == *port)
                    .map(|p| p.global_key.clone())
                    .collect::<Vec<_>>();
                self.processing.lock().unwrap().invalidate(&keys, reason);
                self.invalidate_cached(port, reason, &mut record)?;
            }
        }
        self.apply_physical_event(event)?;
        for item in produced {
            let event = AggregatorEvent::Measurement {
                port_id: "processing".into(),
                global_key: item.key.clone(),
                observation: item.observation.clone(),
            };
            record(&event);
            let mut observation = item.observation;
            observation.source = item.key.clone();
            self.latest
                .lock()
                .unwrap()
                .insert(item.key, observation.clone());
            self.fanout.publish(&observation)?;
        }
        Ok(())
    }
    fn invalidate_cached(
        &self,
        port: &str,
        reason: &str,
        record: &mut impl FnMut(&AggregatorEvent),
    ) -> io::Result<()> {
        let physical = self
            .registry()
            .parameters()
            .map(|p| {
                (
                    p.global_key.clone(),
                    std::collections::BTreeSet::from([p.port_id.clone()]),
                )
            })
            .collect();
        let engine = self.processing.lock().unwrap();
        let domains = engine.lineage(&physical);
        let invalid = engine.invalid_outputs();
        drop(engine);
        let mut latest = self.latest.lock().unwrap();
        for (key, observation) in latest.iter_mut() {
            if !self.catalog.contains_key(key)
                && ((port == "*" && invalid.contains(key))
                    || domains.get(key).is_some_and(|d| d.contains(port)))
                && observation.reading.is_ok()
            {
                observation.reading = Err(reason.into());
                observation.completed_at = std::time::SystemTime::now();
                observation.metadata = None;
                record(&AggregatorEvent::Measurement {
                    port_id: "processing".into(),
                    global_key: key.clone(),
                    observation: observation.clone(),
                });
                self.fanout.publish(observation)?;
            }
        }
        Ok(())
    }
    pub fn tick_recorded(&self, mut record: impl FnMut(&AggregatorEvent)) -> io::Result<()> {
        let _gate = self.lock_service();
        let stale = self.processing.lock().unwrap().tick(Instant::now());
        if !stale.is_empty() {
            self.invalidate_cached("*", "stale_input", &mut record)?;
        }
        Ok(())
    }
    pub fn new(router: CommandRouter, observation_capacity: usize) -> Self {
        let catalog: HashMap<String, ParameterDescription> = router
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
        let continuity = router
            .registry()
            .ports()
            .filter_map(|p| {
                p.handle().map(|h| {
                    (
                        p.id().to_owned(),
                        crate::continuity::Continuity::new(h.source_instance()),
                    )
                })
            })
            .collect();
        Self {
            gate: Mutex::new(()),
            processing: Mutex::new(crate::processing::Engine::new(
                crate::processing::builtins(),
                catalog.clone(),
            )),
            continuity: Mutex::new(continuity),
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
        let _gate = self.gate.lock().expect("service lock poisoned");
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
                    .is_none_or(|p| p.port_id != port_id)
            });
    }

    /// Preserve FIFO by applying the aggregator's returned events in order.
    fn apply_physical_event(&self, event: AggregatorEvent) -> io::Result<()> {
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
    fn catalog_snapshot(&self) -> Option<HashMap<String, ParameterDescription>> {
        Some(
            self.processing
                .lock()
                .expect("processing lock poisoned")
                .catalog(),
        )
    }
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
        if !self.catalog.contains_key(key) {
            return self
                .processing
                .lock()
                .expect("processing lock poisoned")
                .catalog()
                .contains_key(key);
        }
        self.registry().parameter(key).is_some_and(|p| {
            self.fanout.port_usable(&p.port_id)
                && self.router.availability(&p.device_id) == Ok(Availability::Available)
        })
    }
}
