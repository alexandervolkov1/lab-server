//! Release microbenchmark for an in-memory Engine. See docs/processing-architecture.md.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hint::black_box;
use std::time::{Duration, Instant, UNIX_EPOCH};

use lab_server::model::{Observation, ParameterDescription, SourceMetadata, Spec, Value};
use lab_server::processing::{
    Calculation, Component, ComponentFactory, Configuration, Engine, Inputs, Kind, Output,
    Parameters, Registry, TypeDescription, ema::EmaFactory, number, signal,
};

const MAIN: &str = "plant/main";
const ALTERNATE: &str = "backup/main";
const AUX: &str = "aux/input";
const REPETITIONS: usize = 9;

#[derive(Clone, Copy)]
enum Shape {
    Chain,
    Branching,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Self::Chain => "chain",
            Self::Branching => "branching",
        }
    }

    fn factory(self) -> &'static dyn ComponentFactory {
        match self {
            Self::Chain => &EmaFactory,
            Self::Branching => &MeanFactory,
        }
    }

    fn config(self, index: usize, root: &str) -> Configuration {
        let mut inputs = BTreeMap::from([("input".into(), root.into())]);
        if index > 0 {
            let parent = match self {
                Self::Chain => index - 1,
                Self::Branching => (index - 1) / 2,
            };
            inputs.insert("input".into(), signal(&node_name(parent), "value"));
        }
        if matches!(self, Self::Branching) {
            inputs.insert("other".into(), AUX.into());
        }
        Configuration {
            name: node_name(index),
            type_id: match self {
                Self::Chain => "ema",
                Self::Branching => "mean",
            }
            .into(),
            inputs,
            parameters: Parameters::new(),
            max_age_ms: 86_400_000,
        }
    }
}

struct MeanFactory;
struct Mean;

impl ComponentFactory for MeanFactory {
    fn describe(&self) -> TypeDescription {
        TypeDescription {
            type_id: "mean".into(),
            kind: Kind::Filter,
            inputs: vec!["input".into(), "other".into()],
            outputs: vec![Output {
                name: "value".into(),
                unit: None,
            }],
            parameters: vec![],
            proposal: None,
        }
    }

    fn create(&self, _: &Parameters) -> Result<Box<dyn Component>, String> {
        Ok(Box::new(Mean))
    }
}

impl Component for Mean {
    fn process(&mut self, inputs: &Inputs) -> Result<Calculation, String> {
        Ok(BTreeMap::from([(
            "value".into(),
            (number(inputs, "input")? + number(inputs, "other")?) / 2.0,
        )]))
    }

    fn reset(&mut self) {}

    fn reconfigure(&self, params: &Parameters) -> Result<Box<dyn Component>, String> {
        MeanFactory.create(params)
    }
}

fn node_name(index: usize) -> String {
    format!("node{index:03}")
}

fn make_engine(shape: Shape, nodes: usize) -> Engine {
    let mut registry = Registry::default();
    match shape {
        Shape::Chain => registry.register(EmaFactory).unwrap(),
        Shape::Branching => registry.register(MeanFactory).unwrap(),
    }
    let physical = [(MAIN, "C"), (ALTERNATE, "K"), (AUX, "C")].map(|(key, unit)| {
        (
            key.to_owned(),
            ParameterDescription {
                id: key.into(),
                spec: Spec::Float {
                    min: -100.0,
                    max: 100.0,
                },
                unit: unit.into(),
                writable: false,
                default_poll_period: Some(Duration::from_millis(1)),
            },
        )
    });
    let mut engine = Engine::new(registry, HashMap::from(physical));
    for index in 0..nodes {
        engine
            .create(shape.config(index, MAIN), engine.revision)
            .unwrap();
    }
    engine
}

fn sample(at: Instant, sequence: u64) -> Observation {
    Observation {
        source: "input".into(),
        unit: "C".into(),
        completed_at: UNIX_EPOCH,
        metadata: Some(SourceMetadata {
            source_instance: 1,
            sequence,
            generation: 0,
            acquired_at: at,
            elapsed: Duration::from_millis(sequence),
        }),
        reading: Ok(Value::Float((sequence % 10) as f64)),
    }
}

fn seed(engine: &mut Engine, shape: Shape, start: Instant) {
    if matches!(shape, Shape::Branching) {
        black_box(engine.process(AUX.into(), sample(start, 0), start));
    }
}

fn elapsed_us(start: Instant, calls: usize) -> f64 {
    start.elapsed().as_secs_f64() * 1e6 / calls as f64
}

fn median(mut run: impl FnMut() -> f64) -> f64 {
    black_box(run()); // one untimed result for warmup
    let mut samples: [f64; REPETITIONS] = std::array::from_fn(|_| run());
    samples.sort_by(f64::total_cmp);
    samples[REPETITIONS / 2]
}

fn steady(calls: usize, mut run: impl FnMut()) -> f64 {
    median(|| {
        let start = Instant::now();
        for _ in 0..calls {
            run();
        }
        elapsed_us(start, calls)
    })
}

fn measure(shape: Shape, nodes: usize) {
    let calls = (8192 / nodes).clamp(128, 2048);
    let slow_calls = (8192 / nodes / nodes).clamp(8, 128);
    let setup = median(|| {
        let start = Instant::now();
        let engine = black_box(make_engine(shape, nodes));
        let us = elapsed_us(start, 1);
        drop(engine);
        us
    });
    let cold_process = median(|| {
        let mut engine = make_engine(shape, nodes);
        let at = Instant::now();
        seed(&mut engine, shape, at);
        let input = sample(at, 1);
        let key = MAIN.to_owned();
        let start = Instant::now();
        let produced = black_box(engine.process(key, input, at));
        let us = elapsed_us(start, 1);
        assert_eq!(produced.len(), nodes);
        us
    });

    let mut engine = make_engine(shape, nodes);
    let start = Instant::now();
    seed(&mut engine, shape, start);
    let mut sequence = 0;
    let process = steady(calls, || {
        sequence += 1;
        let at = start + Duration::from_millis(sequence);
        black_box(engine.process(MAIN.into(), sample(at, sequence), at));
    });

    let factory = shape.factory();
    let parameters = factory
        .describe()
        .parameters
        .into_iter()
        .map(|parameter| (parameter.name, parameter.default))
        .collect();
    let mut components: Vec<_> = (0..nodes)
        .map(|_| factory.create(&parameters).unwrap())
        .collect();
    let mut inputs = BTreeMap::from([("input".into(), sample(start, 0))]);
    if matches!(shape, Shape::Branching) {
        inputs.insert("other".into(), sample(start, 0));
    }
    let component_calls = steady(calls, || {
        sequence += 1;
        inputs
            .get_mut("input")
            .unwrap()
            .metadata
            .as_mut()
            .unwrap()
            .acquired_at = start + Duration::from_millis(sequence);
        for component in &mut components {
            black_box(component.process(black_box(&inputs)).unwrap());
        }
    });
    let catalog = steady(calls, || {
        black_box(engine.catalog());
    });
    let physical = HashMap::from([
        (MAIN.into(), BTreeSet::from(["plant".into()])),
        (ALTERNATE.into(), BTreeSet::from(["backup".into()])),
        (AUX.into(), BTreeSet::from(["aux".into()])),
    ]);
    let lineage = steady(calls, || {
        black_box(engine.lineage(black_box(&physical)));
    });
    let dependents = steady(slow_calls, || {
        black_box(engine.dependents(black_box("node000")));
    });
    let mut root = MAIN;
    let configure = steady(slow_calls, || {
        root = if root == MAIN { ALTERNATE } else { MAIN };
        engine
            .configure(shape.config(0, root), engine.revision)
            .unwrap();
        black_box(&engine);
    });
    println!(
        "{},{nodes},{setup:.3},{cold_process:.3},{process:.3},{component_calls:.3},{catalog:.3},{lineage:.3},{dependents:.3},{configure:.3}",
        shape.label()
    );
}

fn compare_dependents() {
    println!("shape,nodes,position,reachable,calls_per_sample,median_us");
    for shape in [Shape::Chain, Shape::Branching] {
        for nodes in [8, 32, 128] {
            let engine = make_engine(shape, nodes);
            let middle = match shape {
                Shape::Chain => nodes / 2,
                Shape::Branching => nodes / 4,
            };
            let calls = (16_384 / (nodes * nodes)).clamp(24, 512);
            for (position, index) in [("root", 0), ("intermediate", middle), ("leaf", nodes - 1)] {
                let name = node_name(index);
                let reachable = engine.dependents(&name).len();
                match position {
                    "root" => assert_eq!(reachable, nodes),
                    "leaf" => assert_eq!(reachable, 1),
                    _ => assert!((2..nodes).contains(&reachable)),
                }
                let median = steady(calls, || {
                    black_box(engine.dependents(black_box(&name)));
                });
                println!(
                    "{},{nodes},{position},{reachable},{calls},{median:.3}",
                    shape.label()
                );
            }
        }
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("--dependents-comparison") => return compare_dependents(),
        Some(argument) => panic!("unknown argument {argument}"),
        None => {}
    }
    println!(
        "shape,nodes,setup_us,first_process_us,process_us,components_us,catalog_us,lineage_us,dependents_us,configure_us"
    );
    for shape in [Shape::Chain, Shape::Branching] {
        for nodes in [1, 8, 32, 128] {
            measure(shape, nodes);
        }
    }
}
