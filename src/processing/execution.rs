//! Measurement execution, freshness and propagation of invalid outputs.

use super::{Engine, Inputs, Instance, Kind, State, signal};
use crate::model::{Observation, Value};
use serde_json::json;
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

pub struct Produced {
    pub key: String,
    pub observation: Observation,
    pub proposal: Option<(String, u64, f64)>,
}

#[derive(Clone, Copy)]
pub(super) enum FaultUpdate {
    PreserveInactive,
    Replace,
}

impl Engine {
    pub(crate) fn enable_audit(&mut self, recorder: crate::recorder::RecorderHandle) {
        self.audit = Some(recorder);
    }

    pub fn fault(&mut self, name: &str, reason: &str) {
        if self.nodes.contains_key(name) {
            self.invalidate_nodes(&[name.into()], reason, FaultUpdate::Replace);
        }
    }
    pub fn proposal_deadline(&self, name: &str) -> Option<Instant> {
        let n = self.nodes.get(name)?;
        n.config
            .inputs
            .values()
            .map(|k| {
                let o = self.latest.get(k)?;
                o.reading.as_ref().ok()?;
                self.oldest
                    .get(k)?
                    .checked_add(Duration::from_millis(n.config.max_age_ms))
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .min()
    }

    pub fn invalid_outputs(&self) -> BTreeSet<String> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.fault.is_some())
            .flat_map(|(name, n)| {
                n.description
                    .outputs
                    .iter()
                    .map(move |o| signal(name, &o.name))
            })
            .collect()
    }

    pub(super) fn invalidate_nodes(&mut self, names: &[String], reason: &str, update: FaultUpdate) {
        let names: BTreeSet<_> = names.iter().flat_map(|n| self.dependents(n)).collect();
        for name in names {
            self.invalidate_node(&name, reason, update);
        }
    }
    pub(super) fn invalidate_node(&mut self, name: &str, reason: &str, update: FaultUpdate) {
        let node = self.nodes.get_mut(name).unwrap();
        if matches!(update, FaultUpdate::Replace) || !node.state.is_inactive() {
            node.component.reset();
            if node.description.kind == Kind::Controller {
                audit_fault(self.audit.as_ref(), name, node, self.revision, reason);
                node.state = State::Fault;
            }
            node.fault = Some(reason.into());
        }
        self.clear_node_outputs(name);
    }
    pub(super) fn clear_node_outputs(&mut self, name: &str) {
        for output in &self.nodes[name].description.outputs {
            let key = signal(name, &output.name);
            self.latest.remove(&key);
            self.oldest.remove(&key);
        }
    }
    pub fn invalidate(&mut self, keys: &[String], reason: &str) {
        for key in keys {
            self.latest.remove(key);
            self.oldest.remove(key);
        }
        let names = self
            .nodes
            .iter()
            .filter(|(_, n)| n.config.inputs.values().any(|k| keys.contains(k)))
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();
        self.invalidate_nodes(&names, reason, FaultUpdate::PreserveInactive);
    }
    pub fn tick(&mut self, now: Instant) -> Vec<String> {
        let stale = self
            .nodes
            .iter()
            .filter(|(_, n)| {
                n.config.inputs.values().any(|k| {
                    self.oldest.get(k).is_some_and(|at| {
                        now.saturating_duration_since(*at)
                            > Duration::from_millis(n.config.max_age_ms)
                    })
                })
            })
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();
        self.invalidate_nodes(&stale, "stale_input", FaultUpdate::PreserveInactive);
        stale
    }
    pub fn process(
        &mut self,
        key: String,
        observation: Observation,
        now: Instant,
    ) -> Vec<Produced> {
        if let Some(m) = &observation.metadata {
            self.oldest.insert(key.clone(), m.acquired_at);
        } else {
            self.oldest.remove(&key);
        }
        self.latest.insert(key.clone(), observation.clone());
        let mut changed = BTreeSet::from([key]);
        let catalog = self.catalog();
        let mut produced = Vec::new();
        for name in &self.order {
            let node = self.nodes.get_mut(name).unwrap();
            if !node.config.inputs.values().any(|k| changed.contains(k)) {
                continue;
            }
            let inputs: Inputs = node
                .config
                .inputs
                .iter()
                .filter_map(|(n, k)| self.latest.get(k).map(|o| (n.clone(), o.clone())))
                .collect();
            if inputs.len() != node.config.inputs.len() {
                continue;
            }
            let oldest = node
                .config
                .inputs
                .values()
                .filter_map(|k| self.oldest.get(k))
                .min()
                .copied();
            let valid = inputs.values().all(|o| o.reading.is_ok())
                && node
                    .config
                    .inputs
                    .values()
                    .all(|k| self.oldest.contains_key(k))
                && oldest.is_some_and(|at| {
                    now.saturating_duration_since(at)
                        <= Duration::from_millis(node.config.max_age_ms)
                });
            let inactive = node.state.is_inactive();
            let result = if !valid {
                Err("invalid_input".into())
            } else if inactive {
                Err("controller_stopped".into())
            } else {
                node.component.process(&inputs).and_then(|v| {
                    if v.len() != node.description.outputs.len()
                        || node
                            .description
                            .outputs
                            .iter()
                            .any(|o| !v.get(&o.name).is_some_and(|v| v.is_finite()))
                    {
                        Err("invalid_component_output".into())
                    } else {
                        Ok(v)
                    }
                })
            };
            // An observation can carry an error without changing the controller's cause.
            if !inactive {
                if let Err(error) = &result {
                    node.component.reset();
                    node.fault = Some(error.clone());
                    if node.description.kind == Kind::Controller {
                        audit_fault(self.audit.as_ref(), name, node, self.revision, error);
                        node.state = State::Fault;
                    }
                } else {
                    node.fault = None;
                }
            }
            for output in &node.description.outputs {
                let key = signal(name, &output.name);
                let reading = result
                    .as_ref()
                    .map(|v| Value::Float(v[&output.name]))
                    .map_err(Clone::clone);
                let mut sample = observation.clone();
                sample.source = output.name.clone();
                sample.unit = catalog[&key].unit.clone();
                sample.reading = reading;
                let proposal = if node.state == State::Auto
                    && node.description.proposal.as_ref() == Some(&output.name)
                {
                    result
                        .as_ref()
                        .ok()
                        .map(|v| (name.clone(), node.incarnation, v[&output.name]))
                } else {
                    None
                };
                if let Some(at) = oldest {
                    self.oldest.insert(key.clone(), at);
                }
                self.latest.insert(key.clone(), sample.clone());
                changed.insert(key.clone());
                produced.push(Produced {
                    key,
                    observation: sample,
                    proposal,
                });
            }
        }
        produced
    }
}

pub fn number(inputs: &Inputs, name: &str) -> Result<f64, String> {
    match inputs
        .get(name)
        .ok_or("missing_input")?
        .reading
        .as_ref()
        .map_err(Clone::clone)?
    {
        Value::Float(v) if v.is_finite() => Ok(*v),
        Value::Integer(v) => Ok(*v as f64),
        _ => Err("non_numeric_input".into()),
    }
}
fn audit_fault(
    recorder: Option<&crate::recorder::RecorderHandle>,
    name: &str,
    node: &Instance,
    revision: u64,
    reason: &str,
) {
    if node.state != State::Fault
        && let Some(r) = recorder
    {
        let a = crate::audit::Operation::new(
            r,
            json!({"source":"internal","component":name,"incarnation":node.incarnation.to_string(),"request":{"op":"state","state":"fault","reason":reason}}),
        );
        a.accepted();
        a.event("applied",json!({"state_before":node.state,"state_after":"fault","revision_before":revision,"revision_after":revision}));
    }
}
