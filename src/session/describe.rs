use std::collections::HashMap;

use serde::{Serialize, Serializer};

use super::request_phase;
use crate::model::{ActionDescription, ParameterDescription, Spec};

pub(crate) fn describe_response(
    id: u64,
    catalog: &HashMap<String, ParameterDescription>,
    mut actions: Vec<ActionDescription>,
) -> String {
    #[cfg(feature = "gate-metrics")]
    let _describe = super::metrics::span(super::metrics::Phase::Describe);
    let parameters = request_phase!(DescribeSort, {
        let mut parameters: Vec<_> = catalog.values().collect();
        parameters.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        actions.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        parameters
    });
    request_phase!(
        DescribeSerialize,
        serde_json::to_string(&Response {
            actions: &actions,
            id,
            parameters: &parameters,
            status: "ok",
        })
        .expect("describe response contains only JSON-representable values")
    )
}

// Field order matches the sorted serde_json::Map keys in the original v1 response.
#[derive(Serialize)]
struct Response<'a> {
    #[serde(serialize_with = "serialize_actions")]
    actions: &'a [ActionDescription],
    id: u64,
    #[serde(serialize_with = "serialize_parameters")]
    parameters: &'a [&'a ParameterDescription],
    status: &'static str,
}

#[derive(Serialize)]
struct Action<'a> {
    description: &'a str,
    key: &'a str,
}

fn serialize_actions<S: Serializer>(
    actions: &[ActionDescription],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(actions.iter().map(|action| Action {
        description: &action.description,
        key: &action.name,
    }))
}

#[derive(Serialize)]
struct Parameter<'a> {
    key: &'a str,
    max: Bound,
    min: Bound,
    poll_period_ns: Option<u128>,
    #[serde(rename = "type")]
    kind: &'static str,
    unit: &'a str,
    writable: bool,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Bound {
    Float(f64),
    Integer(i64),
    Null,
}

fn serialize_parameters<S: Serializer>(
    parameters: &[&ParameterDescription],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(parameters.iter().map(|description| {
        let (kind, min, max, unit) = match description.spec {
            Spec::Float { min, max } => (
                "float",
                Bound::Float(min),
                Bound::Float(max),
                description.unit.as_str(),
            ),
            Spec::Integer { min, max } => (
                "integer",
                Bound::Integer(min),
                Bound::Integer(max),
                description.unit.as_str(),
            ),
            Spec::Boolean => ("boolean", Bound::Null, Bound::Null, ""),
        };
        Parameter {
            key: &description.id,
            max,
            min,
            poll_period_ns: description
                .default_poll_period
                .map(|period| period.as_nanos()),
            kind,
            unit,
            writable: description.writable,
        }
    }))
}
