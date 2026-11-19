//! Component descriptors, factories and parameter validation.

use super::{Calculation, Inputs, Parameters};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};

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
    pub(super) fn factory(&self, type_id: &str) -> Result<&dyn ComponentFactory, &'static str> {
        self.factories
            .get(type_id)
            .map(Box::as_ref)
            .ok_or("unknown_type")
    }

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
pub(super) fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}
fn unique<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let mut set = BTreeSet::new();
    values.into_iter().all(|s| identifier(s) && set.insert(s))
}
pub(super) fn validate(d: &TypeDescription, params: &Parameters) -> Result<Parameters, String> {
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
