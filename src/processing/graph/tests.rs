use super::*;
use crate::model::{Observation, SourceMetadata, Value};
use crate::processing::{
    Calculation, Component, ComponentFactory, Inputs, Kind, Output, Parameters, Registry, State,
    builtins, number,
};
use std::time::{Instant, UNIX_EPOCH};

// The pre-optimization fixed-point scan, kept independent of self.order.
fn reference_dependents(engine: &Engine, name: &str) -> Vec<String> {
    let mut names = BTreeSet::from([name.to_owned()]);
    loop {
        let before = names.len();
        for (id, n) in &engine.nodes {
            if n.config.inputs.values().any(|key| {
                names.iter().any(|name| {
                    engine.nodes.get(name).is_some_and(|n| {
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

const MAIN: &str = "device/main";
const OTHER: &str = "device/other";

#[derive(Clone)]
struct NodeFactory {
    inputs: usize,
    outputs: usize,
}

impl NodeFactory {
    fn type_id(&self) -> String {
        format!("graph_{}_{}", self.inputs, self.outputs)
    }
}

impl ComponentFactory for NodeFactory {
    fn describe(&self) -> TypeDescription {
        TypeDescription {
            type_id: self.type_id(),
            kind: Kind::Filter,
            inputs: (0..self.inputs).map(|i| format!("i{i}")).collect(),
            outputs: (0..self.outputs)
                .map(|i| Output {
                    name: format!("o{i}"),
                    unit: None,
                })
                .collect(),
            parameters: vec![],
            proposal: None,
        }
    }

    fn create(&self, _: &Parameters) -> Result<Box<dyn Component>, String> {
        Ok(Box::new(self.clone()))
    }
}

impl Component for NodeFactory {
    fn process(&mut self, inputs: &Inputs) -> Result<Calculation, String> {
        let value = number(inputs, "i0")?;
        Ok((0..self.outputs)
            .map(|i| (format!("o{i}"), value))
            .collect())
    }

    fn reset(&mut self) {}

    fn reconfigure(&self, parameters: &Parameters) -> Result<Box<dyn Component>, String> {
        self.create(parameters)
    }
}

fn physical() -> HashMap<String, ParameterDescription> {
    [MAIN, OTHER]
        .into_iter()
        .map(|key| {
            (
                key.to_owned(),
                ParameterDescription {
                    id: key.into(),
                    unit: "C".into(),
                    spec: Spec::Float { min: 0., max: 100. },
                    writable: false,
                    default_poll_period: Some(Duration::from_millis(1)),
                },
            )
        })
        .collect()
}

fn test_engine() -> Engine {
    let mut registry = Registry::default();
    for inputs in 1..=3 {
        for outputs in 1..=2 {
            registry.register(NodeFactory { inputs, outputs }).unwrap();
        }
    }
    Engine::new(registry, physical())
}

fn config(name: &str, inputs: &[&str], outputs: usize) -> Configuration {
    Configuration {
        name: name.into(),
        type_id: NodeFactory {
            inputs: inputs.len(),
            outputs,
        }
        .type_id(),
        inputs: inputs
            .iter()
            .enumerate()
            .map(|(i, key)| (format!("i{i}"), (*key).into()))
            .collect(),
        parameters: Parameters::new(),
        max_age_ms: 1000,
    }
}

fn add(engine: &mut Engine, name: &str, inputs: &[&str], outputs: usize) {
    engine
        .create(config(name, inputs, outputs), engine.revision)
        .unwrap();
}

fn compare_all(engine: &Engine) {
    for seed in engine.nodes.keys().map(String::as_str).chain(["missing"]) {
        assert_eq!(
            engine.dependents(seed),
            reference_dependents(engine, seed),
            "seed {seed}, {} nodes",
            engine.nodes.len()
        );
    }
}

fn diamond() -> Engine {
    let mut engine = test_engine();
    for (name, inputs, outputs) in [
        ("z_root", &[MAIN][..], 2),
        ("m_left", &["processing/z_root/o0"], 2),
        ("y_right", &["processing/z_root/o1"], 1),
        (
            "b_merge",
            &["processing/m_left/o1", "processing/y_right/o0"],
            2,
        ),
        (
            "c_other_merge",
            &["processing/m_left/o0", "processing/b_merge/o0"],
            1,
        ),
        (
            "a_final",
            &["processing/b_merge/o1", "processing/c_other_merge/o0"],
            1,
        ),
        ("side", &[OTHER], 2),
        ("spare", &["processing/side/o0"], 1),
    ] {
        add(&mut engine, name, inputs, outputs);
    }
    engine
}

#[test]
fn reference_equivalence_for_chain_wide_tree_and_overlapping_paths() {
    for wide in [false, true] {
        let mut engine = test_engine();
        for index in (0..32).rev() {
            let input = if index == 31 {
                MAIN.into()
            } else {
                let parent = if wide { 31 } else { index + 1 };
                signal(&format!("node{parent:02}"), "o0")
            };
            add(&mut engine, &format!("node{index:02}"), &[&input], 1);
        }
        assert_eq!(engine.order[0], "node31");
        let reachable = engine.dependents("node31");
        assert_eq!(reachable.len(), 32);
        assert_eq!(reachable[0], "node00");
        compare_all(&engine);
    }

    let engine = diamond();
    assert_eq!(
        engine.dependents("z_root"),
        [
            "a_final",
            "b_merge",
            "c_other_merge",
            "m_left",
            "y_right",
            "z_root"
        ]
    );
    assert_eq!(engine.dependents("missing"), ["missing"]);
    compare_all(&engine);
}

fn draw(state: &mut u64, upper: usize) -> usize {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*state >> 32) as usize) % upper
}

#[test]
fn reference_equivalence_for_seeded_dags_within_catalog_capacity() {
    for size in [1, 8, 32, 64, 128] {
        for mut state in [1, 703, 79_951, 0xbeef_cafe] {
            let mut engine = test_engine();
            let names: Vec<_> = (0..size)
                .map(|i| format!("r{:03}", (i * 73 + 31) % 128))
                .collect();
            let output_counts: Vec<_> = (0..size).map(|i| if i % 8 == 0 { 2 } else { 1 }).collect();
            for index in 0..size {
                let inputs: Vec<_> = (0..1 + draw(&mut state, 3))
                    .map(|_| {
                        if index == 0 || draw(&mut state, 4) == 0 {
                            [MAIN, OTHER][draw(&mut state, 2)].into()
                        } else {
                            let parent = draw(&mut state, index);
                            let output = draw(&mut state, output_counts[parent]);
                            signal(&names[parent], &format!("o{output}"))
                        }
                    })
                    .collect();
                let inputs: Vec<_> = inputs.iter().map(String::as_str).collect();
                add(&mut engine, &names[index], &inputs, output_counts[index]);
            }
            assert_eq!(engine.nodes.len(), size);
            compare_all(&engine);
        }
    }
}

#[test]
fn rewire_and_cascade_use_legacy_order_and_preserve_the_separate_branch() {
    let mut engine = diamond();
    let affected = reference_dependents(&engine, "z_root");
    let keys: Vec<_> = affected
        .iter()
        .flat_map(|name| {
            engine.nodes[name]
                .description
                .outputs
                .iter()
                .map(move |o| signal(name, &o.name))
        })
        .collect();
    let before = engine.list();
    assert_eq!(
        engine.remove("z_root", false, engine.revision),
        Err("has_dependents".into())
    );
    assert_eq!(engine.list(), before);

    let changed = engine
        .configure_with_changes(
            config("z_root", &["processing/side/o1"], 2),
            engine.revision,
        )
        .unwrap();
    assert_eq!(changed, keys.iter().cloned().collect());
    assert_eq!(engine.dependents("z_root"), affected);
    compare_all(&engine);
    assert_eq!(
        engine.remove("z_root", true, engine.revision).unwrap(),
        keys
    );
    assert_eq!(
        engine.nodes.keys().map(String::as_str).collect::<Vec<_>>(),
        ["side", "spare"]
    );
    compare_all(&engine);
}

#[test]
fn reachability_ignores_controller_state_and_watchdog_preserves_inactive_causes() {
    let mut engine = Engine::new(builtins(), physical());
    for (name, kind, input) in [
        ("root", "ema", MAIN),
        ("live", "pid", "processing/root/value"),
        ("shadow", "pid", "processing/root/value"),
        ("stopped", "pid", "processing/root/value"),
        ("faulted", "pid", "processing/root/value"),
        ("other", "ema", OTHER),
    ] {
        engine
            .create(
                Configuration {
                    name: name.into(),
                    type_id: kind.into(),
                    inputs: [("input".into(), input.into())].into(),
                    parameters: Parameters::new(),
                    max_age_ms: 1000,
                },
                engine.revision,
            )
            .unwrap();
    }
    let now = Instant::now();
    engine.process(
        MAIN.into(),
        Observation {
            source: "main".into(),
            unit: "C".into(),
            completed_at: UNIX_EPOCH,
            reading: Ok(Value::Float(10.)),
            metadata: Some(SourceMetadata {
                source_instance: 1,
                sequence: 1,
                generation: 0,
                acquired_at: now,
                elapsed: Duration::ZERO,
            }),
        },
        now,
    );
    engine
        .set_state("live", State::Auto, engine.revision)
        .unwrap();
    engine
        .set_state("stopped", State::Stopped, engine.revision)
        .unwrap();
    engine.fault("faulted", "initial_failure");
    compare_all(&engine);
    assert_eq!(
        engine.dependents("root"),
        ["faulted", "live", "root", "shadow", "stopped"]
    );

    assert!(
        engine
            .tick(now + Duration::from_secs(2))
            .contains(&"root".into())
    );
    engine.invalidate(&[MAIN.into()], "source_data_lost");
    for (name, state, reason) in [
        ("live", State::Fault, Some("stale_input")),
        ("shadow", State::Fault, Some("stale_input")),
        ("stopped", State::Stopped, Some("component_stopped")),
        ("faulted", State::Fault, Some("initial_failure")),
        ("root", State::Running, Some("source_data_lost")),
        ("other", State::Running, None),
    ] {
        assert_eq!(engine.nodes[name].state, state);
        assert_eq!(engine.nodes[name].fault.as_deref(), reason);
    }
    compare_all(&engine);
}
