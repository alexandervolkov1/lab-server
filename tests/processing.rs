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
