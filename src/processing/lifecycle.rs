//! Component configuration, state transitions and management snapshots.

use super::execution::FaultUpdate;
use super::schema::{identifier, validate};
use super::{Configuration, Engine, Instance, Kind, MAX_COMPONENTS, State, signal};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::collections::BTreeSet;
use std::time::Instant;

impl Engine {
    pub fn touch(&mut self, expected: u64) -> Result<(), String> {
        self.check_revision(expected)?;
        self.revision += 1;
        Ok(())
    }

    pub fn ready_for_auto(&self, name: &str, expected: u64) -> Result<u64, String> {
        self.check_revision(expected)?;
        let n = self.nodes.get(name).ok_or("unknown_instance")?;
        if n.description.kind != Kind::Controller
            || n.state != State::Shadow
            || n.description.proposal.is_none()
        {
            return Err("restart_in_shadow_first".into());
        }
        self.proposal_deadline(name)
            .filter(|d| *d > Instant::now())
            .ok_or("input_not_ready")?;
        Ok(n.incarnation)
    }

    fn check_revision(&self, expected: u64) -> Result<(), String> {
        if self.revision == u64::MAX || self.next_incarnation == u64::MAX {
            return Err("revision_exhausted".into());
        }
        if expected != self.revision {
            return Err("revision_conflict".into());
        }
        Ok(())
    }

    pub fn types(&self) -> Json {
        json!(self.registry.types())
    }
    pub fn list(&self) -> Json {
        self.page(0, 16).expect("default page")
    }
    /// Compact live state; no configuration, bindings or algorithm history.
    pub(crate) fn diagnostics(&self) -> Vec<Json> {
        self.nodes
            .iter()
            .map(|(name, n)| {
                json!({
                    "name": name, "type_id": n.config.type_id,
                    "state": n.state, "error": n.fault
                })
            })
            .collect()
    }
    pub fn page(&self, offset: usize, limit: usize) -> Result<Json, String> {
        self.page_with(offset, limit, |_, _, _| {})
    }
    /// The service can add management metadata before the shared byte budget is checked.
    pub(crate) fn page_with(
        &self,
        offset: usize,
        limit: usize,
        mut decorate: impl FnMut(&str, u64, &mut Json),
    ) -> Result<Json, String> {
        if limit == 0 || limit > 64 || offset > self.nodes.len() {
            return Err("invalid_page".into());
        }
        let mut instances = Vec::new();
        let mut bytes = 256; // response fields, separators and maximal u64 revision
        for n in self.nodes.values().skip(offset).take(limit) {
            let mut value = json!({"configuration":n.config,"incarnation":n.incarnation,"state":n.state,"fault":n.fault});
            decorate(&n.config.name, n.incarnation, &mut value);
            let size = value.to_string().len() + 1;
            if bytes + size > 48 * 1024 {
                if instances.is_empty() {
                    return Err("instance_too_large".into());
                }
                break;
            }
            bytes += size;
            instances.push(value);
        }
        let next = offset + instances.len();
        Ok(
            json!({"revision":self.revision,"instances":instances,"total":self.nodes.len(),"next_offset":if next<self.nodes.len(){Some(next)}else{None}}),
        )
    }
    pub fn state(&self, name: &str) -> Option<(State, u64)> {
        self.nodes.get(name).map(|n| (n.state, n.incarnation))
    }
    pub fn create(&mut self, mut config: Configuration, expected: u64) -> Result<(), String> {
        self.check_revision(expected)?;
        if !identifier(&config.name)
            || self.nodes.contains_key(&config.name)
            || self.nodes.len() >= MAX_COMPONENTS
        {
            return Err("invalid_instance".into());
        }
        let factory = self.registry.factory(&config.type_id)?;
        let description = factory.describe();
        config.parameters = validate(&description, &config.parameters)?;
        let component = factory.create(&config.parameters)?;
        let plan = self.validate_graph(&config, &description)?;
        let state = if description.kind == Kind::Filter {
            State::Running
        } else {
            State::Shadow
        };
        self.nodes.insert(
            config.name.clone(),
            Instance {
                config,
                description,
                inherited_unit: String::new(),
                component,
                incarnation: self.next_incarnation,
                state,
                fault: None,
            },
        );
        self.next_incarnation += 1;
        self.revision += 1;
        self.install_graph(plan);
        Ok(())
    }
    pub fn configure(&mut self, config: Configuration, expected: u64) -> Result<(), String> {
        self.configure_with_changes(config, expected).map(|_| ())
    }
    /// Replace one parameter of the current configuration under the caller's commit lock.
    pub fn parameter_set(
        &mut self,
        name: &str,
        parameter: String,
        value: Json,
        expected: Option<u64>,
    ) -> Result<(), String> {
        let expected = expected.unwrap_or(self.revision);
        self.check_revision(expected)?;
        let mut config = self
            .nodes
            .get(name)
            .ok_or("unknown_instance")?
            .config
            .clone();
        config.parameters.insert(parameter, value);
        self.configure(config, expected)
    }
    /// Output histories invalidated by this committed rewire, including descendants.
    pub(crate) fn configure_with_changes(
        &mut self,
        mut config: Configuration,
        expected: u64,
    ) -> Result<BTreeSet<String>, String> {
        self.check_revision(expected)?;
        let node = self.nodes.get(&config.name).ok_or("unknown_instance")?;
        if node.config.type_id != config.type_id || node.state == State::Auto {
            return Err("stop_before_configure".into());
        }
        let description = node.description.clone();
        config.parameters = validate(&description, &config.parameters)?;
        let plan = self.validate_graph(&config, &description)?;
        let mut replacement = node.component.reconfigure(&config.parameters)?;
        let rewire = config.inputs != node.config.inputs;
        if rewire {
            replacement.reset();
        }
        let name = config.name.clone();
        let changed = if rewire {
            let dependents = {
                #[cfg(test)]
                let _span = super::rewire_measurement::span(
                    super::rewire_measurement::Phase::FirstDependents,
                );
                self.dependents(&name)
            };
            #[cfg(test)]
            let _span = super::rewire_measurement::span(super::rewire_measurement::Phase::Changed);
            dependents
                .iter()
                .flat_map(|name| {
                    self.nodes[name]
                        .description
                        .outputs
                        .iter()
                        .map(move |o| signal(name, &o.name))
                })
                .collect()
        } else {
            BTreeSet::new()
        };
        let node = self.nodes.get_mut(&name).unwrap();
        node.config = config;
        node.component = replacement;
        if rewire {
            self.invalidate_nodes(&[name], "input_changed", FaultUpdate::PreserveInactive);
        }
        self.install_graph(plan);
        self.revision += 1;
        Ok(changed)
    }
    pub fn remove(
        &mut self,
        name: &str,
        cascade: bool,
        expected: u64,
    ) -> Result<Vec<String>, String> {
        self.check_revision(expected)?;
        if !self.nodes.contains_key(name) {
            return Err("unknown_instance".into());
        }
        let names = self.dependents(name);
        if !cascade && names.len() > 1 {
            return Err("has_dependents".into());
        }
        let mut keys = Vec::new();
        for name in &names {
            let node = self.nodes.remove(name).unwrap();
            for o in node.description.outputs {
                let key = signal(name, &o.name);
                self.latest.remove(&key);
                self.oldest.remove(&key);
                keys.push(key);
            }
        }
        // Every dependent was removed, so the remaining units are still valid.
        self.order.retain(|n| !names.contains(n));
        self.revision += 1;
        Ok(keys)
    }
    pub fn set_state(&mut self, name: &str, state: State, expected: u64) -> Result<(), String> {
        self.check_revision(expected)?;
        if !matches!(state, State::Shadow | State::Stopped | State::Auto) {
            return Err("invalid_state".into());
        }
        let node = self.nodes.get_mut(name).ok_or("unknown_instance")?;
        if node.description.kind != Kind::Controller {
            return Err("not_controller".into());
        }
        if state == State::Auto && node.state != State::Shadow {
            return Err("restart_in_shadow_first".into());
        }
        node.component.reset();
        node.state = state;
        node.fault = None;
        if state == State::Stopped {
            node.fault = Some("component_stopped".into());
            self.clear_node_outputs(name);
            for dependent in self.dependents(name) {
                if dependent != name {
                    self.invalidate_node(
                        &dependent,
                        "component_stopped",
                        FaultUpdate::PreserveInactive,
                    );
                }
            }
        }
        self.revision += 1;
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Management {
    Outputs,
    Bind {
        name: String,
        target: String,
        command_timeout_ms: u64,
        expected_revision: u64,
    },
    Types,
    List {
        #[serde(default)]
        offset: usize,
        #[serde(default = "page_limit")]
        limit: usize,
    },
    Create {
        configuration: Configuration,
        expected_revision: u64,
    },
    Configure {
        configuration: Configuration,
        expected_revision: u64,
    },
    ParameterSet {
        name: String,
        parameter: String,
        value: Json,
        expected_revision: Option<u64>,
    },
    Delete {
        name: String,
        #[serde(default)]
        cascade: bool,
        expected_revision: u64,
    },
    State {
        name: String,
        state: State,
        expected_revision: u64,
    },
}
fn page_limit() -> usize {
    16
}
