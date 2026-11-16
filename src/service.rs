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

struct Control {
    owner: crate::output::Owner,
    target: String,
    timeout: std::time::Duration,
    pending: Option<crate::pending::Pending<Result<(), crate::port_worker::CommandError>>>,
}

pub struct DeviceService {
    controls: Mutex<HashMap<String, Control>>,
    gate: Mutex<()>,
    processing: Mutex<crate::processing::Engine>,
    continuity: Mutex<HashMap<String, crate::continuity::Continuity>>,
    router: CommandRouter,
    pub(crate) catalog: HashMap<String, ParameterDescription>,
    pub(crate) latest: Mutex<HashMap<String, Observation>>,
    fanout: Arc<ObservationFanout>,
}

impl DeviceService {
    pub fn stop_controls(&self) {
        let _gate = self.lock_service();
        self.router.authority().release_all();
        let mut engine = self.processing.lock().unwrap();
        for name in self.controls.lock().unwrap().keys() {
            engine.fault(name, "shutdown");
        }
    }
    fn reconcile_controls(&self, engine: &mut crate::processing::Engine) {
        use crate::pending::{Completion, Poll};
        let mut controls = self.controls.lock().unwrap();
        for (name, c) in controls.iter_mut() {
            if let Some(pending) = c.pending.take() {
                match pending.poll(Instant::now()) {
                    Poll::Waiting(p) => c.pending = Some(p),
                    Poll::Finished(Completion::Received { value: Ok(()), .. }) => {}
                    Poll::Finished(_) => {
                        if engine.state(name).is_some_and(|(_, inc)| inc == c.owner.1) {
                            engine.fault(name, "output_failed");
                        }
                        self.router.authority().release(&c.owner);
                    }
                }
            }
            if engine.state(name) != Some((crate::processing::State::Auto, c.owner.1)) {
                self.router.authority().release(&c.owner);
            }
        }
        controls.retain(|name, c| {
            c.pending.is_some() || engine.state(name).is_some_and(|(_, inc)| inc == c.owner.1)
        });
    }
    fn submit_proposal(&self, name: &str, incarnation: u64, value: f64) {
        let mut engine = self.processing.lock().unwrap();
        if engine.state(name) != Some((crate::processing::State::Auto, incarnation)) {
            return;
        }
        let mut controls = self.controls.lock().unwrap();
        let Some(c) = controls.get_mut(name) else {
            return;
        };
        if c.owner.1 != incarnation || c.pending.is_some() {
            return;
        }
        let result = (|| {
            let deadline = engine
                .proposal_deadline(name)
                .ok_or("input_not_ready".to_owned())?
                .min(Instant::now() + c.timeout);
            let description = self
                .router
                .description(&c.target)
                .map_err(|e| e.to_string())?;
            let value = crate::output::convert(&description.description.spec, value)
                .map_err(str::to_owned)?;
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
            let lineage = engine.lineage(&physical);
            let prefix = format!("processing/{name}/");
            let ports = lineage
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .flat_map(|(_, p)| p.iter().cloned())
                .collect::<std::collections::BTreeSet<_>>();
            let continuity = self.continuity.lock().unwrap();
            let sources = ports
                .iter()
                .filter_map(|p| {
                    self.registry()
                        .port(p)?
                        .handle()
                        .map(|h| (h.clone(), continuity[p].generation()))
                })
                .collect();
            self.router
                .set_control(
                    incarnation,
                    &c.target,
                    value,
                    deadline,
                    Some(&c.owner),
                    sources,
                )
                .map_err(|e| e.to_string())
        })();
        match result {
            Ok(p) => c.pending = Some(p),
            Err(reason) => {
                engine.fault(name, &reason);
                self.router.authority().release(&c.owner);
            }
        }
    }
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
        let mut discontinued = std::collections::BTreeSet::new();
        match command {
            Outputs => return Ok(self.router.authority().snapshot()),
            Bind {
                name,
                target,
                command_timeout_ms,
                expected_revision,
            } => {
                if command_timeout_ms == 0 || command_timeout_ms > 10000 {
                    return Err("invalid_timeout".into());
                }
                let (state, incarnation) = engine.state(&name).ok_or("unknown_instance")?;
                if state != crate::processing::State::Shadow {
                    return Err("stop_in_shadow_before_bind".into());
                }
                let p = self
                    .router
                    .description(&target)
                    .map_err(|e| e.to_string())?;
                if !p.description.writable
                    || matches!(p.description.spec, crate::model::Spec::Boolean)
                {
                    return Err("invalid_output_target".into());
                }
                let mut controls = self.controls.lock().unwrap();
                if controls.get(&name).is_some_and(|c| c.pending.is_some()) {
                    return Err("output_busy".into());
                }
                engine.touch(expected_revision)?;
                controls.insert(
                    name.clone(),
                    Control {
                        owner: (name, incarnation),
                        target,
                        timeout: std::time::Duration::from_millis(command_timeout_ms),
                        pending: None,
                    },
                );
            }
            Types => return Ok(engine.types()),
            List { offset, limit } => return engine.page(offset, limit),
            Create {
                configuration,
                expected_revision,
            } => engine.create(configuration, expected_revision)?,
            Configure {
                configuration,
                expected_revision,
            } => discontinued = engine.configure_with_changes(configuration, expected_revision)?,
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
                    let incarnation = engine.ready_for_auto(&name, expected_revision)?;
                    let controls = self.controls.lock().unwrap();
                    let c = controls
                        .get(&name)
                        .filter(|c| c.owner.1 == incarnation)
                        .ok_or("output_not_bound")?;
                    if c.pending.is_some() {
                        return Err("output_busy".into());
                    }
                    self.router
                        .authority()
                        .acquire(&c.target, c.owner.clone())?;
                }
                engine.set_state(&name, state, expected_revision)?;
            }
        }
        self.reconcile_controls(&mut engine);
        let invalid = engine.invalid_outputs();
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
        self.fanout
            .update_keys(engine.lineage(&physical), &discontinued);
        self.latest
            .lock()
            .expect("cache lock poisoned")
            .retain(|k, _| catalog.contains_key(k) && !invalid.contains(k));
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
        self.reconcile_controls(&mut self.processing.lock().unwrap());
        self.apply_physical_event(event)?;
        for item in produced {
            if let Some((name, incarnation, value)) = &item.proposal {
                self.submit_proposal(name, *incarnation, *value);
            }
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
        let mut engine = self.processing.lock().unwrap();
        let stale = engine.tick(Instant::now());
        self.reconcile_controls(&mut engine);
        let invalid = !engine.invalid_outputs().is_empty();
        drop(engine);
        if invalid {
            // A failed write may be the only event: do not wait for another
            // measurement to invalidate the controller's published diagnostics.
            let reason = if stale.is_empty() {
                "component_unavailable"
            } else {
                "stale_input"
            };
            self.invalidate_cached("*", reason, &mut record)?;
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
        let actions_bytes = router
            .registry()
            .actions()
            .map(|a| {
                serde_json::json!({"key":a.global_key,"description":a.description.description})
                    .to_string()
                    .len()
                    + 1
            })
            .sum::<usize>();
        let catalog_budget = (64 * 1024usize).saturating_sub(actions_bytes.saturating_add(1024));
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
            controls: Mutex::new(HashMap::new()),
            gate: Mutex::new(()),
            processing: Mutex::new(
                crate::processing::Engine::new(crate::processing::builtins(), catalog.clone())
                    .with_catalog_budget(catalog_budget),
            ),
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
        RouteError::Authority(reason) => EnqueueError::Authority(reason),
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
    fn components(&self, command: serde_json::Value) -> Result<serde_json::Value, String> {
        self.manage_inner(
            serde_json::from_value(command)
                .map_err(|e| format!("invalid_component_request:{e}"))?,
        )
    }
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
