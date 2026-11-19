use super::describe_response;
use crate::model::{ActionDescription, ParameterDescription, Spec};
use crate::processing::{Configuration, Engine, builtins, signal};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

fn reference_response(
    id: u64,
    catalog: &HashMap<String, ParameterDescription>,
    mut actions: Vec<ActionDescription>,
) -> String {
    let mut descriptions: Vec<_> = catalog.values().collect();
    descriptions.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    let parameters: Vec<_> = descriptions
        .into_iter()
        .map(|description| {
            let (kind, min, max, unit) = match &description.spec {
                Spec::Float { min, max } => {
                    ("float", json!(min), json!(max), description.unit.as_str())
                }
                Spec::Integer { min, max } => {
                    ("integer", json!(min), json!(max), description.unit.as_str())
                }
                Spec::Boolean => ("boolean", json!(null), json!(null), ""),
            };
            json!({
                "key": &description.id,
                "type": kind,
                "min": min,
                "max": max,
                "unit": unit,
                "writable": description.writable,
                "poll_period_ns": description.default_poll_period.map(|period| period.as_nanos())
            })
        })
        .collect();
    actions.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    let actions: Vec<_> = actions
        .into_iter()
        .map(|action| json!({ "key": action.name, "description": action.description }))
        .collect();
    json!({"id": id, "status": "ok", "parameters": parameters, "actions": actions}).to_string()
}

fn parameter(
    id: &str,
    spec: Spec,
    unit: &str,
    writable: bool,
    default_poll_period: Option<Duration>,
) -> ParameterDescription {
    ParameterDescription {
        id: id.into(),
        spec,
        unit: unit.into(),
        writable,
        default_poll_period,
    }
}

fn assert_reference(
    id: u64,
    catalog: &HashMap<String, ParameterDescription>,
    actions: Vec<ActionDescription>,
) -> String {
    let actual = describe_response(id, catalog, actions.clone());
    assert_eq!(actual, reference_response(id, catalog, actions));
    actual
}

#[test]
fn empty_describe_matches_compact_v1_field_order() {
    let actual = assert_reference(u64::MAX, &HashMap::new(), vec![]);
    assert_eq!(
        actual,
        r#"{"actions":[],"id":18446744073709551615,"parameters":[],"status":"ok"}"#
    );
}

#[test]
fn describe_matches_reference_for_numeric_bounds_poll_periods_and_escaping() {
    let escaped = "кВт \"😀\"\\\n\r\t\u{0000}\u{001f}";
    let poll_over_u64 = Duration::from_nanos(u64::MAX) + Duration::from_nanos(1);
    let parameters = [
        parameter(
            "z/extreme_float",
            Spec::Float {
                min: -f64::MAX,
                max: f64::MAX,
            },
            escaped,
            false,
            Some(Duration::MAX),
        ),
        parameter(
            "c/signed_zero",
            Spec::Float {
                min: -0.0,
                max: 0.0,
            },
            "",
            true,
            Some(poll_over_u64),
        ),
        parameter(
            "b/integer",
            Spec::Integer {
                min: i64::MIN,
                max: i64::MAX,
            },
            "шт",
            true,
            Some(Duration::from_nanos(u64::MAX)),
        ),
        parameter("a/boolean", Spec::Boolean, "ignored", true, None),
        parameter(
            escaped,
            Spec::Float {
                min: -f64::from_bits(1),
                max: f64::MIN_POSITIVE,
            },
            "",
            false,
            Some(Duration::ZERO),
        ),
        // An external backend can provide descriptors that bypass driver validation.
        parameter(
            "nonfinite_nan",
            Spec::Float {
                min: f64::NAN,
                max: f64::INFINITY,
            },
            "",
            false,
            None,
        ),
        parameter(
            "nonfinite_neg_inf",
            Spec::Float {
                min: f64::NEG_INFINITY,
                max: 1e-7,
            },
            "",
            false,
            None,
        ),
    ];
    for description in &parameters {
        let single = HashMap::from([("catalog entry".into(), description.clone())]);
        assert_reference(9_007_199_254_740_993, &single, vec![]);
    }
    let catalog = parameters
        .into_iter()
        .enumerate()
        .map(|(index, description)| (index.to_string(), description))
        .collect();
    let actions = ["z_action", escaped, "a_action"]
        .into_iter()
        .map(|name| ActionDescription {
            name: name.into(),
            description: escaped.into(),
        })
        .collect();
    let actual = assert_reference(u64::MAX, &catalog, actions);
    assert!(actual.contains(r#""max":0.0,"min":-0.0,"poll_period_ns":18446744073709551616"#));
    assert!(actual.contains(r#""poll_period_ns":18446744073709551615999999999"#));
    assert!(actual.contains(r#""key":"a/boolean","max":null,"min":null,"poll_period_ns":null,"type":"boolean","unit":"","writable":true"#));
    assert!(!actual.contains('\n'));
}

#[test]
fn describe_matches_reference_for_128_real_processing_components() {
    let physical = parameter(
        "device/input",
        Spec::Float {
            min: -10.0,
            max: 250.0,
        },
        "°C",
        false,
        Some(Duration::from_millis(500)),
    );
    let alternate = parameter(
        "device/alternate",
        Spec::Float {
            min: 0.0,
            max: 500.0,
        },
        "°F",
        false,
        Some(Duration::from_millis(500)),
    );
    let mut input = physical.id.clone();
    let mut engine = Engine::new(
        builtins(),
        HashMap::from([(input.clone(), physical), (alternate.id.clone(), alternate)]),
    );
    for index in (0..128).rev() {
        let name = format!("node_{index:03}");
        let config = Configuration {
            name: name.clone(),
            type_id: "ema".into(),
            inputs: [("input".into(), input)].into(),
            parameters: Default::default(),
            max_age_ms: 1000,
        };
        engine.create(config, engine.revision).unwrap();
        input = signal(&name, "value");
    }
    let catalog = engine.catalog();
    assert_eq!(catalog.len(), 130);
    assert_eq!(catalog[&input].unit, "°C");
    let actual = assert_reference(42, &catalog, vec![]);
    let last = actual.find("processing/node_127/value").unwrap();
    let first = actual.find("processing/node_000/value").unwrap();
    assert!(first < last);

    engine
        .configure(
            Configuration {
                name: "node_127".into(),
                type_id: "ema".into(),
                inputs: [("input".into(), "device/alternate".into())].into(),
                parameters: Default::default(),
                max_age_ms: 1000,
            },
            engine.revision,
        )
        .unwrap();
    let catalog = engine.catalog();
    assert_eq!(catalog[&input].unit, "°F");
    assert_reference(43, &catalog, vec![]);

    engine.remove("node_000", false, engine.revision).unwrap();
    let catalog = engine.catalog();
    assert!(!catalog.contains_key(&input));
    assert_reference(44, &catalog, vec![]);
}
