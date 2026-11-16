//! Server-owned, transport-independent component graph. No device I/O.
use crate::model::{Observation, ParameterDescription, Spec, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

pub type Parameters = BTreeMap<String, Json>;
pub type Inputs = BTreeMap<String, Observation>;
pub type Calculation = BTreeMap<String, f64>;

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParameterType {
    Float { min: f64, max: f64 },
    Integer { min: i64, max: i64 },
    Boolean,
    Choice { values: Vec<String> },
}
#[derive(Clone, Serialize)]
pub struct ParameterSchema {
    pub name: String,
    pub unit: String,
    pub value_type: ParameterType,
    pub default: Json,
}
#[derive(Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Filter,
    Controller,
}
#[derive(Clone, Serialize)]
pub struct Output {
    pub name: String,
    /// None inherits the first input's unit.
    pub unit: Option<String>,
}
#[derive(Clone, Serialize)]
pub struct TypeDescription {
    pub type_id: String,
    pub kind: Kind,
    pub inputs: Vec<String>,
    pub outputs: Vec<Output>,
    pub parameters: Vec<ParameterSchema>,
    /// A controller's proposal signal; the graph itself never writes it.
    pub proposal: Option<String>,
}
/// All inputs and outputs are finite numeric scalars in this first contract.
/// Errors invalidate the entire output set. No blocking calls or external effects.
pub trait Component: Send {
    fn process(&mut self, inputs: &Inputs) -> Result<Calculation, String>;
    fn reset(&mut self);
    /// Stage a replacement, preserving allowed state; failure must not mutate self.
    fn reconfigure(&self, parameters: &Parameters) -> Result<Box<dyn Component>, String>;
}
pub trait ComponentFactory: Send + Sync {
    fn describe(&self) -> TypeDescription;
    fn create(&self, parameters: &Parameters) -> Result<Box<dyn Component>, String>;
}
#[derive(Default)]
pub struct Registry {
    factories: BTreeMap<String, Box<dyn ComponentFactory>>,
}
impl Registry {
    pub fn register(&mut self, factory: impl ComponentFactory + 'static) -> Result<(), String> {
        let d = factory.describe();
        if !identifier(&d.type_id)
            || self.factories.contains_key(&d.type_id)
            || d.inputs.is_empty()
            || d.inputs.len() > 8
            || d.outputs.is_empty()
            || d.outputs.len() > 8
            || d.parameters.len() > 32
            || !unique(d.inputs.iter().map(String::as_str))
            || !unique(d.outputs.iter().map(|o| o.name.as_str()))
            || !unique(d.parameters.iter().map(|p| p.name.as_str()))
            || (d.kind == Kind::Filter && d.proposal.is_some())
            || d.proposal
                .as_ref()
                .is_some_and(|p| !d.outputs.iter().any(|o| &o.name == p))
        {
            return Err("invalid_type_descriptor".into());
        }
        validate(&d, &Parameters::new())?;
        self.factories.insert(d.type_id.clone(), Box::new(factory));
        Ok(())
    }
    pub fn types(&self) -> Vec<TypeDescription> {
        self.factories.values().map(|f| f.describe()).collect()
    }
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}
fn unique<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut set = BTreeSet::new();
    values.into_iter().all(|s| identifier(s) && set.insert(s))
}
fn validate(d: &TypeDescription, params: &Parameters) -> Result<Parameters, String> {
    if params
        .keys()
        .any(|k| !d.parameters.iter().any(|p| &p.name == k))
    {
        return Err("unknown_parameter".into());
    }
    d.parameters
        .iter()
        .map(|p| {
            let v = params.get(&p.name).unwrap_or(&p.default);
            let ok = match &p.value_type {
                ParameterType::Float { min, max } => {
                    min.is_finite()
                        && max.is_finite()
                        && min <= max
                        && v.as_f64()
                            .is_some_and(|n| n.is_finite() && n >= *min && n <= *max)
                }
                ParameterType::Integer { min, max } => {
                    min <= max && v.as_i64().is_some_and(|n| n >= *min && n <= *max)
                }
                ParameterType::Boolean => v.is_boolean(),
                ParameterType::Choice { values } => {
                    v.as_str().is_some_and(|s| values.iter().any(|v| v == s))
                }
            };
            if ok {
                Ok((p.name.clone(), v.clone()))
            } else {
                Err(format!("invalid_parameter:{}", p.name))
            }
        })
        .collect()
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    pub name: String,
    pub type_id: String,
    pub inputs: BTreeMap<String, String>,
    #[serde(default)]
    pub parameters: Parameters,
    /// Independent freshness policy, applies even when no samples arrive.
    pub max_age_ms: u64,
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Running,
    Shadow,
    Stopped,
    Fault,
    Auto,
}
struct Instance {
    config: Configuration,
    description: TypeDescription,
    incarnation: u64,
    component: Box<dyn Component>,
    state: State,
    fault: Option<String>,
}
pub struct Engine {
    registry: Registry,
    physical: HashMap<String, ParameterDescription>,
    nodes: BTreeMap<String, Instance>,
    order: Vec<String>,
    latest: HashMap<String, Observation>,
    pub revision: u64,
    next_incarnation: u64,
}
pub struct Produced {
    pub key: String,
    pub observation: Observation,
    pub proposal: Option<(String, u64, f64)>,
}
impl Engine {
    pub fn new(registry: Registry, physical: HashMap<String, ParameterDescription>) -> Self {
        Self {
            registry,
            physical,
            nodes: BTreeMap::new(),
            order: Vec::new(),
            latest: HashMap::new(),
            revision: 0,
            next_incarnation: 1,
        }
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
        json!({"revision":self.revision, "instances":self.nodes.values().map(|n| json!({"configuration":n.config,"incarnation":n.incarnation,"state":n.state,"fault":n.fault})).collect::<Vec<_>>()})
    }
    pub fn state(&self, name: &str) -> Option<(State, u64)> {
        self.nodes.get(name).map(|n| (n.state, n.incarnation))
    }
    pub fn create(&mut self, mut config: Configuration, expected: u64) -> Result<(), String> {
        self.check_revision(expected)?;
        if !identifier(&config.name)
            || self.nodes.contains_key(&config.name)
            || self.nodes.len() >= 128
        {
            return Err("invalid_instance".into());
        }
        let factory = self
            .registry
            .factories
            .get(&config.type_id)
            .ok_or("unknown_type")?;
        let description = factory.describe();
        config.parameters = validate(&description, &config.parameters)?;
        let component = factory.create(&config.parameters)?;
        let order = self.validate_graph(&config, &description)?;
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
                component,
                incarnation: self.next_incarnation,
                state,
                fault: None,
            },
        );
        self.next_incarnation += 1;
        self.revision += 1;
        self.order = order;
        Ok(())
    }
    pub fn configure(&mut self, mut config: Configuration, expected: u64) -> Result<(), String> {
        self.check_revision(expected)?;
        let node = self.nodes.get(&config.name).ok_or("unknown_instance")?;
        if node.config.type_id != config.type_id || node.state == State::Auto {
            return Err("stop_before_configure".into());
        }
        let description = node.description.clone();
        config.parameters = validate(&description, &config.parameters)?;
        let order = self.validate_graph(&config, &description)?;
        let mut replacement = node.component.reconfigure(&config.parameters)?;
        let rewire = config.inputs != node.config.inputs;
        if rewire {
            replacement.reset();
        }
        let name = config.name.clone();
        let node = self.nodes.get_mut(&name).unwrap();
        node.config = config;
        node.component = replacement;
        if rewire {
            self.invalidate_nodes(&[name], "input_changed");
        }
        self.order = order;
        self.revision += 1;
        Ok(())
    }
    fn validate_graph(
        &self,
        config: &Configuration,
        desc: &TypeDescription,
    ) -> Result<Vec<String>, String> {
        if config.max_age_ms == 0
            || config.max_age_ms > 86_400_000
            || config.inputs.keys().cloned().collect::<BTreeSet<_>>()
                != desc.inputs.iter().cloned().collect()
        {
            return Err("invalid_inputs".into());
        }
        let mut configs: BTreeMap<_, _> = self
            .nodes
            .iter()
            .map(|(k, n)| (k.clone(), (&n.config, &n.description)))
            .collect();
        configs.insert(config.name.clone(), (config, desc));
        let mut owners = HashMap::new();
        for (name, (_, d)) in &configs {
            for o in &d.outputs {
                let key = signal(name, &o.name);
                if self.physical.contains_key(&key) || owners.insert(key, name.clone()).is_some() {
                    return Err("signal_collision".into());
                }
            }
        }
        let mut dependencies = BTreeMap::<String, BTreeSet<String>>::new();
        for (name, (c, _)) in &configs {
            let mut edges = BTreeSet::new();
            for key in c.inputs.values() {
                if let Some(owner) = owners.get(key) {
                    edges.insert(owner.clone());
                } else if !self.physical.get(key).is_some_and(|p| {
                    !matches!(p.spec, Spec::Boolean) && p.default_poll_period.is_some()
                }) {
                    return Err("unknown_or_non_numeric_input".into());
                }
            }
            dependencies.insert(name.clone(), edges);
        }
        let mut order = Vec::new();
        while !dependencies.is_empty() {
            let name = dependencies
                .iter()
                .find(|(_, e)| e.is_empty())
                .map(|(n, _)| n.clone())
                .ok_or("dependency_cycle")?;
            dependencies.remove(&name);
            for edges in dependencies.values_mut() {
                edges.remove(&name);
            }
            order.push(name);
        }
        Ok(order)
    }
    pub fn dependents(&self, name: &str) -> Vec<String> {
        let mut names = BTreeSet::from([name.to_owned()]);
        loop {
            let before = names.len();
            for (id, n) in &self.nodes {
                if n.config.inputs.values().any(|key| {
                    names.iter().any(|name| {
                        self.nodes.get(name).is_some_and(|n| {
                            n.description
                                .outputs
                                .iter()
                                .any(|o| signal(name, &o.name) == *key)
                        })
                    })
                }) {
                    names.insert(id.clone());
                }
            }
            if names.len() == before {
                break;
            }
        }
        names.into_iter().collect()
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
                keys.push(key);
            }
        }
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
        self.revision += 1;
        Ok(())
    }
    pub fn catalog(&self) -> HashMap<String, ParameterDescription> {
        let mut catalog = self.physical.clone();
        for name in &self.order {
            let node = &self.nodes[name];
            let unit = node
                .config
                .inputs
                .values()
                .next()
                .and_then(|k| catalog.get(k))
                .map(|p| p.unit.clone())
                .unwrap_or_default();
            for output in &node.description.outputs {
                let id = signal(name, &output.name);
                catalog.insert(
                    id.clone(),
                    ParameterDescription {
                        id,
                        spec: Spec::Float {
                            min: -f64::MAX,
                            max: f64::MAX,
                        },
                        unit: output.unit.clone().unwrap_or_else(|| unit.clone()),
                        writable: false,
                        default_poll_period: Some(Duration::from_millis(1)),
                    },
                );
            }
        }
        catalog
    }
    pub fn lineage(
        &self,
        physical: &HashMap<String, BTreeSet<String>>,
    ) -> HashMap<String, BTreeSet<String>> {
        let mut domains = physical.clone();
        for name in &self.order {
            let n = &self.nodes[name];
            let ports = n
                .config
                .inputs
                .values()
                .filter_map(|k| domains.get(k))
                .flatten()
                .cloned()
                .collect::<BTreeSet<_>>();
            for o in &n.description.outputs {
                domains.insert(signal(name, &o.name), ports.clone());
            }
        }
        domains
    }
    fn invalidate_nodes(&mut self, names: &[String], reason: &str) {
        let names: BTreeSet<_> = names.iter().flat_map(|n| self.dependents(n)).collect();
        for name in names {
            let n = self.nodes.get_mut(&name).unwrap();
            n.component.reset();
            if n.description.kind == Kind::Controller {
                n.state = State::Fault;
            }
            n.fault = Some(reason.into());
            for o in &n.description.outputs {
                self.latest.remove(&signal(&name, &o.name));
            }
        }
    }
    pub fn invalidate(&mut self, keys: &[String], reason: &str) {
        for key in keys {
            self.latest.remove(key);
        }
        let names = self
            .nodes
            .iter()
            .filter(|(_, n)| n.config.inputs.values().any(|k| keys.contains(k)))
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();
        self.invalidate_nodes(&names, reason);
    }
    pub fn tick(&mut self, now: Instant) -> Vec<String> {
        let stale = self
            .nodes
            .iter()
            .filter(|(_, n)| {
                n.config.inputs.values().any(|k| {
                    self.latest
                        .get(k)
                        .and_then(|o| o.metadata.as_ref())
                        .is_some_and(|m| {
                            now.saturating_duration_since(m.acquired_at)
                                > Duration::from_millis(n.config.max_age_ms)
                        })
                })
            })
            .map(|(n, _)| n.clone())
            .collect::<Vec<_>>();
        self.invalidate_nodes(&stale, "stale_input");
        stale
    }
    pub fn process(
        &mut self,
        key: String,
        observation: Observation,
        now: Instant,
    ) -> Vec<Produced> {
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
            let valid = inputs.len() == node.config.inputs.len()
                && inputs.values().all(|o| {
                    o.reading.is_ok()
                        && o.metadata.as_ref().is_some_and(|m| {
                            now.saturating_duration_since(m.acquired_at)
                                <= Duration::from_millis(node.config.max_age_ms)
                        })
                });
            let result = if !valid {
                Err("invalid_input".into())
            } else if matches!(node.state, State::Stopped | State::Fault) {
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
            if let Err(error) = &result {
                node.component.reset();
                node.fault = Some(error.clone());
                if node.description.kind == Kind::Controller && node.state != State::Stopped {
                    node.state = State::Fault;
                }
            } else {
                node.fault = None;
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
pub fn signal(instance: &str, output: &str) -> String {
    format!("processing/{instance}/{output}")
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
pub fn float_parameter(
    name: &str,
    default: f64,
    min: f64,
    max: f64,
    unit: &str,
) -> ParameterSchema {
    ParameterSchema {
        name: name.into(),
        unit: unit.into(),
        value_type: ParameterType::Float { min, max },
        default: json!(default),
    }
}
