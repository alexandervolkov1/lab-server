use lab_server::model::{Observation, ParameterDescription, SourceMetadata, Spec, Value};
use lab_server::processing::*;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant, SystemTime};
#[derive(Clone)]
struct Gain(f64);
struct Factory;
impl ComponentFactory for Factory {
    fn describe(&self) -> TypeDescription {
        TypeDescription {
            type_id: "test_gain".into(),
            kind: Kind::Filter,
            inputs: vec!["input".into()],
            outputs: vec![Output {
                name: "value".into(),
                unit: None,
            }],
            parameters: vec![float_parameter("gain", 1., 0., 10., "")],
            proposal: None,
        }
    }
    fn create(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
        Ok(Box::new(Gain(p["gain"].as_f64().unwrap())))
    }
}
impl Component for Gain {
    fn process(&mut self, inputs: &Inputs) -> Result<Calculation, String> {
        Ok(BTreeMap::from([(
            "value".into(),
            number(inputs, "input")? * self.0,
        )]))
    }
    fn reset(&mut self) {}
    fn reconfigure(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
        Factory.create(p)
    }
}
fn engine() -> Engine {
    let mut r = Registry::default();
    r.register(Factory).unwrap();
    Engine::new(
        r,
        HashMap::from([(
            "a/raw".into(),
            ParameterDescription {
                id: "a/raw".into(),
                spec: Spec::Float {
                    min: -100.,
                    max: 100.,
                },
                unit: "C".into(),
                writable: false,
                default_poll_period: Some(Duration::from_secs(1)),
            },
        )]),
    )
}
fn config(name: &str, input: &str) -> Configuration {
    Configuration {
        name: name.into(),
        type_id: "test_gain".into(),
        inputs: BTreeMap::from([("input".into(), input.into())]),
        parameters: BTreeMap::new(),
        max_age_ms: 1000,
    }
}
#[test]
fn custom_factory_chain_atomic_configuration_cycles_and_deletion() {
    let mut e = engine();
    e.create(config("one", "a/raw"), 0).unwrap();
    e.create(config("two", "processing/one/value"), 1).unwrap();
    let before = e.list();
    assert!(
        e.configure(config("one", "processing/two/value"), 2)
            .is_err()
    );
    assert_eq!(before, e.list());
    let mut c = config("one", "a/raw");
    c.parameters.insert("gain".into(), json!(2));
    assert!(e.configure(c.clone(), 1).is_err());
    e.configure(c, 2).unwrap();
    let at = Instant::now();
    let sample = Observation {
        source: "raw".into(),
        unit: "C".into(),
        completed_at: SystemTime::now(),
        metadata: Some(SourceMetadata {
            source_instance: 1,
            sequence: 1,
            generation: 0,
            acquired_at: at,
            elapsed: Duration::ZERO,
        }),
        reading: Ok(Value::Float(3.)),
    };
    let out = e.process("a/raw".into(), sample, at);
    assert_eq!(out.len(), 2);
    assert_eq!(out[1].observation.reading, Ok(Value::Float(6.)));
    assert!(e.remove("one", false, 3).is_err());
    assert_eq!(e.revision, 3);
    assert_eq!(e.remove("one", true, 3).unwrap().len(), 2);
    assert_eq!(e.catalog().len(), 1);
    e.create(config("one", "a/raw"), 4).unwrap();
    assert_eq!(e.state("one").unwrap().1, 3);
}
#[test]
fn schemas_reject_unknown_fields_and_wrong_types_without_revision_change() {
    let mut e = engine();
    for value in [json!(true), json!("2"), json!(-1), json!(11)] {
        let mut c = config("one", "a/raw");
        c.parameters.insert("gain".into(), value);
        assert!(e.create(c, 0).is_err());
        assert_eq!(e.revision, 0);
    }
    let mut c = config("one", "a/raw");
    c.parameters.insert("typo".into(), json!(1));
    assert!(e.create(c, 0).is_err());
}

#[test]
fn parameter_set_validates_all_schema_types_without_coercion() {
    struct TypedFactory;
    impl ComponentFactory for TypedFactory {
        fn describe(&self) -> TypeDescription {
            let mut d = Factory.describe();
            d.parameters.extend([
                ParameterSchema {
                    name: "count".into(),
                    unit: String::new(),
                    value_type: ParameterType::Integer {
                        min: i64::MIN,
                        max: i64::MAX,
                    },
                    default: json!(0),
                },
                ParameterSchema {
                    name: "enabled".into(),
                    unit: String::new(),
                    value_type: ParameterType::Boolean,
                    default: json!(false),
                },
                ParameterSchema {
                    name: "mode".into(),
                    unit: String::new(),
                    value_type: ParameterType::Choice {
                        values: vec!["a".into(), "b".into()],
                    },
                    default: json!("a"),
                },
            ]);
            d
        }
        fn create(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
            Factory.create(p)
        }
    }
    let mut registry = Registry::default();
    registry.register(TypedFactory).unwrap();
    let mut e = Engine::new(registry, engine().catalog());
    e.create(config("typed", "a/raw"), 0).unwrap();
    for (p, v) in [
        ("count", json!(i64::MAX)),
        ("enabled", json!(true)),
        ("mode", json!("b")),
        ("gain", json!(2.5)),
    ] {
        let revision = e.revision;
        e.parameter_set("typed", p.into(), v.clone(), None).unwrap();
        assert_eq!(e.revision, revision + 1);
        assert_eq!(
            e.list()["instances"][0]["configuration"]["parameters"][p],
            v
        );
    }
    let before = e.list();
    for (p, v) in [
        ("count", json!(2.0)),
        ("count", json!(u64::MAX)),
        ("enabled", json!(1)),
        ("mode", json!("c")),
        ("mode", json!(true)),
        ("gain", json!([])),
    ] {
        assert!(e.parameter_set("typed", p.into(), v, None).is_err());
        assert_eq!(e.list(), before);
    }
}

#[test]
fn pid_derivative_antiwindup_gap_and_loss_require_explicit_restart() {
    let mut e = Engine::new(builtins(), engine().catalog());
    let mut c = config("loop", "a/raw");
    c.type_id = "pid".into();
    c.max_age_ms = 20000;
    c.parameters=serde_json::from_value(json!({"setpoint":10.,"kp":2.,"ki":1.,"kd":3.,"output_min":0.,"output_max":20.,"max_gap":5.})).unwrap();
    e.create(c.clone(), 0).unwrap();
    let start = Instant::now();
    let sample = |t, x| Observation {
        source: "raw".into(),
        unit: "C".into(),
        completed_at: SystemTime::now(),
        metadata: Some(SourceMetadata {
            source_instance: 1,
            sequence: t + 1,
            generation: 0,
            acquired_at: start + Duration::from_secs(t),
            elapsed: Duration::from_secs(t),
        }),
        reading: Ok(Value::Float(x)),
    };
    let values = |out: Vec<Produced>| {
        out.into_iter()
            .map(|p| (p.key, p.observation.reading.unwrap()))
            .collect::<BTreeMap<_, _>>()
    };
    let first = values(e.process("a/raw".into(), sample(0, 0.), start));
    assert_eq!(first["processing/loop/i"], Value::Float(0.));
    assert_eq!(first["processing/loop/d"], Value::Float(0.));
    let second = values(e.process(
        "a/raw".into(),
        sample(1, 0.),
        start + Duration::from_secs(1),
    ));
    assert_eq!(second["processing/loop/i"], Value::Float(0.));
    c.parameters.insert("setpoint".into(), json!(20.));
    e.configure(c, 1).unwrap();
    let third = values(e.process(
        "a/raw".into(),
        sample(2, 0.),
        start + Duration::from_secs(2),
    ));
    assert_eq!(third["processing/loop/d"], Value::Float(0.));
    e.process(
        "a/raw".into(),
        sample(10, 0.),
        start + Duration::from_secs(10),
    );
    assert_eq!(e.state("loop").unwrap().0, State::Fault);
    e.process(
        "a/raw".into(),
        sample(11, 0.),
        start + Duration::from_secs(11),
    );
    assert_eq!(e.state("loop").unwrap().0, State::Fault);
    e.set_state("loop", State::Shadow, 2).unwrap();
    e.process(
        "a/raw".into(),
        sample(12, 2.),
        start + Duration::from_secs(12),
    );
    assert_eq!(e.state("loop").unwrap().0, State::Shadow);
    e.invalidate(&["a/raw".into()], "lost");
    e.process(
        "a/raw".into(),
        sample(13, 3.),
        start + Duration::from_secs(13),
    );
    assert_eq!(e.state("loop").unwrap().0, State::Fault);
}

#[test]
fn watchdog_and_input_invalidation_preserve_inactive_controllers() {
    let mut e = Engine::new(builtins(), engine().catalog());
    for (revision, name, type_id) in [
        (0, "loop", "pid"),
        (1, "paused", "pid"),
        (2, "smooth", "ema"),
        (3, "watchdog", "pid"),
    ] {
        let mut component = config(name, "a/raw");
        component.type_id = type_id.into();
        e.create(component, revision).unwrap();
    }
    let start = Instant::now();
    let sample = |seconds| Observation {
        source: "raw".into(),
        unit: "C".into(),
        completed_at: SystemTime::now(),
        metadata: Some(SourceMetadata {
            source_instance: 1,
            sequence: seconds + 1,
            generation: 0,
            acquired_at: start + Duration::from_secs(seconds),
            elapsed: Duration::from_secs(seconds),
        }),
        reading: Ok(Value::Float(3.)),
    };
    e.process("a/raw".into(), sample(0), start);
    e.set_state("loop", State::Auto, 4).unwrap();
    e.fault("loop", "output_failed");
    e.set_state("paused", State::Stopped, 5).unwrap();

    let causes = |e: &Engine| {
        e.list()["instances"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| {
                (
                    node["configuration"]["name"].as_str().unwrap().to_owned(),
                    node["fault"].clone(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let stale = e.tick(start + Duration::from_secs(2));
    assert!(stale.iter().any(|name| name == "watchdog"));
    let fault = causes(&e);
    assert_eq!(fault["loop"], "output_failed");
    assert_eq!(fault["paused"], "component_stopped");
    assert_eq!(fault["smooth"], "stale_input");
    assert_eq!(fault["watchdog"], "stale_input");
    assert_eq!(e.state("paused").unwrap().0, State::Stopped);
    assert_eq!(e.state("watchdog").unwrap().0, State::Fault);

    e.invalidate(&["a/raw".into()], "source_data_lost");
    let invalid = causes(&e);
    assert_eq!(invalid["loop"], "output_failed");
    assert_eq!(invalid["paused"], "component_stopped");
    assert_eq!(invalid["watchdog"], "stale_input");
    assert_eq!(invalid["smooth"], "source_data_lost");
    let produced = e.process("a/raw".into(), sample(3), start + Duration::from_secs(3));
    let output = |key: &str| produced.iter().find(|item| item.key == key).unwrap();
    assert_eq!(
        output("processing/smooth/value").observation.reading,
        Ok(Value::Float(3.))
    );
    for key in [
        "processing/loop/output",
        "processing/paused/output",
        "processing/watchdog/output",
    ] {
        assert!(output(key).observation.reading.is_err());
    }
    assert!(produced.iter().all(|item| item.proposal.is_none()));
    assert!(e.invalid_outputs().contains("processing/loop/output"));
    assert!(e.invalid_outputs().contains("processing/paused/output"));
    assert!(causes(&e)["smooth"].is_null());

    e.fault("loop", "shutdown");
    assert_eq!(causes(&e)["loop"], "shutdown");
    e.set_state("loop", State::Shadow, 6).unwrap();
    assert!(causes(&e)["loop"].is_null());
    let resumed = e.process("a/raw".into(), sample(4), start + Duration::from_secs(4));
    let resumed = resumed
        .iter()
        .find(|item| item.key == "processing/loop/output")
        .unwrap();
    assert!(resumed.observation.reading.is_ok());
    assert!(resumed.proposal.is_none());
}

#[test]
fn multi_input_fresh_trigger_cannot_hide_old_ancestor_from_controller_watchdog() {
    struct PairFactory;
    impl ComponentFactory for PairFactory {
        fn describe(&self) -> TypeDescription {
            let mut d = Factory.describe();
            d.type_id = "pair".into();
            d.inputs = vec!["input".into(), "other".into()];
            d
        }
        fn create(&self, p: &Parameters) -> Result<Box<dyn Component>, String> {
            Factory.create(p)
        }
    }
    let mut registry = builtins();
    registry.register(PairFactory).unwrap();
    let mut physical = engine().catalog();
    let mut b = physical["a/raw"].clone();
    b.id = "b/raw".into();
    physical.insert("b/raw".into(), b);
    let mut e = Engine::new(registry, physical);
    let mut pair = config("pair", "a/raw");
    pair.type_id = "pair".into();
    pair.inputs.insert("other".into(), "b/raw".into());
    pair.max_age_ms = 10000;
    e.create(pair, 0).unwrap();
    let mut controller = config("loop", "processing/pair/value");
    controller.type_id = "pid".into();
    controller.max_age_ms = 1000;
    e.create(controller, 1).unwrap();
    let now = Instant::now();
    let sample = |at| Observation {
        source: "raw".into(),
        unit: "C".into(),
        completed_at: SystemTime::now(),
        metadata: Some(SourceMetadata {
            source_instance: 1,
            sequence: 1,
            generation: 0,
            acquired_at: at,
            elapsed: Duration::ZERO,
        }),
        reading: Ok(Value::Float(1.)),
    };
    e.process("a/raw".into(), sample(now - Duration::from_secs(5)), now);
    let out = e.process("b/raw".into(), sample(now), now);
    assert!(out[0].observation.reading.is_ok());
    assert!(out.last().unwrap().observation.reading.is_err());
    assert_eq!(e.state("loop").unwrap().0, State::Fault);
    e.set_state("loop", State::Shadow, 2).unwrap();
    assert!(e.ready_for_auto("loop", 3).is_err());
    // A separate fresh branch remains operational.
    let mut fresh = config("fresh", "b/raw");
    fresh.type_id = "ema".into();
    e.create(fresh, 3).unwrap();
    assert!(
        e.process(
            "b/raw".into(),
            sample(now + Duration::from_millis(1)),
            now + Duration::from_millis(1)
        )
        .iter()
        .any(|p| p.key == "processing/fresh/value" && p.observation.reading.is_ok())
    );
}

#[test]
fn component_messages_reject_nested_duplicate_fields_and_paginate_instances() {
    use lab_server::protocol::{JsonError, ParseError, parse_request};
    assert!(matches!(
        parse_request(r#"{"id":1,"op":"components","command":{"action":"list","action":"types"}}"#),
        Err(ParseError::Json(JsonError::DuplicateKey))
    ));
    let mut e = engine();
    for n in 0..20 {
        e.create(config(&format!("n{n:02}"), "a/raw"), n).unwrap();
    }
    let first = e.page(0, 16).unwrap();
    assert_eq!(first["next_offset"], 16);
    assert_eq!(first["instances"].as_array().unwrap().len(), 16);
    let last = e.page(16, 16).unwrap();
    assert!(last["next_offset"].is_null());
    assert_eq!(last["instances"].as_array().unwrap().len(), 4);
    assert!(e.page(0, 65).is_err());
    let mut e = Engine::new(builtins(), engine().catalog());
    let mut rejected = false;
    for n in 0..128 {
        let mut c = config(&format!("loop{n}"), "a/raw");
        c.type_id = "pid".into();
        let before = e.list();
        if let Err(error) = e.create(c, n) {
            assert_eq!(error, "catalog_capacity");
            assert_eq!(before, e.list());
            rejected = true;
            break;
        }
    }
    assert!(
        rejected,
        "the graph must remain describable within the v1 response budget"
    );
}
