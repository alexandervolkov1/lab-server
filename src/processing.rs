//! Server-owned, transport-independent component graph. No device I/O.
use crate::model::{Observation, ParameterDescription};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

mod execution;
mod graph;
mod lifecycle;
mod schema;

pub use execution::{Produced, number};
pub use lifecycle::Management;
pub use schema::{
    Component, ComponentFactory, Kind, Output, ParameterSchema, ParameterType, Registry,
    TypeDescription, float_parameter,
};

pub type Parameters = BTreeMap<String, Json>;
pub type Inputs = BTreeMap<String, Observation>;
pub type Calculation = BTreeMap<String, f64>;
const MAX_COMPONENTS: usize = 128;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
impl State {
    fn is_inactive(self) -> bool {
        matches!(self, Self::Stopped | Self::Fault)
    }
}

struct Instance {
    config: Configuration,
    description: TypeDescription,
    inherited_unit: String,
    incarnation: u64,
    component: Box<dyn Component>,
    state: State,
    fault: Option<String>,
}
pub struct Engine {
    audit: Option<crate::recorder::RecorderHandle>,
    registry: Registry,
    physical: HashMap<String, ParameterDescription>,
    nodes: BTreeMap<String, Instance>,
    order: Vec<String>,
    latest: HashMap<String, Observation>,
    oldest: HashMap<String, Instant>,
    pub revision: u64,
    next_incarnation: u64,
    catalog_budget: usize,
}
impl Engine {
    /// Complete configuration in stable instance-name order, with normalized
    /// parameters. Incarnations are used only to match service-side bindings.
    pub(crate) fn configurations(&self) -> impl Iterator<Item = (&Configuration, u64)> {
        self.nodes
            .values()
            .map(|node| (&node.config, node.incarnation))
    }

    pub(crate) fn with_catalog_budget(mut self, bytes: usize) -> Self {
        self.catalog_budget = bytes.min(48 * 1024);
        self
    }
    pub fn new(registry: Registry, physical: HashMap<String, ParameterDescription>) -> Self {
        Self {
            audit: None,
            registry,
            physical,
            nodes: BTreeMap::new(),
            order: Vec::new(),
            latest: HashMap::new(),
            oldest: HashMap::new(),
            revision: 0,
            next_incarnation: 1,
            catalog_budget: 48 * 1024,
        }
    }
}
pub fn signal(instance: &str, output: &str) -> String {
    format!("processing/{instance}/{output}")
}

pub mod ema;
pub mod pid;
pub fn builtins() -> Registry {
    let mut r = Registry::default();
    r.register(ema::EmaFactory).expect("builtin descriptor");
    r.register(pid::PidFactory).expect("builtin descriptor");
    r
}
