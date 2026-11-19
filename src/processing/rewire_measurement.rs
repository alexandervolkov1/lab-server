//! Opt-in phase timings for the ignored Release experiment only.

use super::{Configuration, Engine, State, builtins, signal};
use crate::model::{Observation, ParameterDescription, SourceMetadata, Spec, Value};
use crate::recorder::{Recorder, RecorderOptions};
use std::cell::Cell;
use std::hint::black_box;
use std::time::{Duration, Instant, UNIX_EPOCH};

const PHYSICAL: &str = "device/value";
const WARMUP: usize = 9;
const SAMPLES: usize = 65;

#[derive(Clone, Copy)]
pub(super) enum Phase {
    FirstDependents,
    Changed,
    SecondDependents,
    Invalidate,
}

#[derive(Clone, Copy, Default)]
struct Timing {
    calls: usize,
    elapsed: Duration,
}

type Sample = [Timing; 4];

thread_local! {
    static ACTIVE: Cell<Option<Sample>> = const { Cell::new(None) };
}

pub(super) struct Span(Phase, Instant);

pub(super) fn span(phase: Phase) -> Option<Span> {
    ACTIVE.with(|active| active.get().is_some().then(|| Span(phase, Instant::now())))
}

impl Drop for Span {
    fn drop(&mut self) {
        let elapsed = self.1.elapsed();
        ACTIVE.with(|active| {
            let mut result = active.take().unwrap();
            result[self.0 as usize].calls += 1;
            result[self.0 as usize].elapsed += elapsed;
            active.set(Some(result));
        });
    }
}

fn measure(configure: impl FnOnce()) -> Sample {
    ACTIVE.set(Some(Sample::default()));
    configure();
    ACTIVE.take().unwrap()
}

fn name(index: usize) -> String {
    format!("node{index:03}")
}

fn config(index: usize, input: &str, kind: &str) -> Configuration {
    Configuration {
        name: name(index),
        type_id: kind.into(),
        inputs: [("input".into(), input.into())].into(),
        parameters: Default::default(),
        max_age_ms: 60_000,
    }
}

fn series(nodes: usize) -> Vec<Sample> {
    let db = crate::recording_db::Database::new();
    let recorder = Recorder::with_options(
        &db.0,
        RecorderOptions {
            audit_enabled: true,
            measurements_enabled: false,
            ..RecorderOptions::default()
        },
    )
    .unwrap();
    let mut engine = Engine::new(
        builtins(),
        [(
            PHYSICAL.into(),
            ParameterDescription {
                id: PHYSICAL.into(),
                spec: Spec::Float { min: 0., max: 100. },
                unit: "C".into(),
                writable: false,
                default_poll_period: Some(Duration::from_millis(1)),
            },
        )]
        .into(),
    );
    engine.enable_audit(recorder.handle());
    for index in 0..nodes {
        let input = if index == 0 {
            PHYSICAL.to_owned()
        } else {
            signal(&name(index - 1), "value")
        };
        let kind = if index == nodes - 1 { "pid" } else { "ema" };
        engine
            .create(config(index, &input, kind), engine.revision)
            .unwrap();
    }
    let last = name(nodes - 1);
    let first_output = signal(&name(0), "value");
    let start = Instant::now();
    let mut samples = Vec::with_capacity(SAMPLES);
    for index in 0..WARMUP + SAMPLES {
        engine
            .set_state(&last, State::Shadow, engine.revision)
            .unwrap();
        let sequence = index as u64 + 1;
        let elapsed = Duration::from_millis(sequence);
        let at = start + elapsed;
        let produced = engine.process(
            PHYSICAL.into(),
            Observation {
                source: "value".into(),
                unit: "C".into(),
                completed_at: UNIX_EPOCH + elapsed,
                reading: Ok(Value::Float(10.)),
                metadata: Some(SourceMetadata {
                    source_instance: 1,
                    sequence,
                    generation: 0,
                    acquired_at: at,
                    elapsed,
                }),
            },
            at,
        );
        assert_eq!(produced.len(), nodes + 6);
        assert!(produced.iter().all(|p| p.observation.reading.is_ok()));
        let input = if index % 2 == 0 {
            PHYSICAL
        } else {
            &first_output
        };
        let configuration = config(1, input, "ema");
        let revision = engine.revision;
        let sample = measure(|| {
            black_box(engine.configure_with_changes(configuration, revision)).unwrap();
        });
        assert_eq!(sample.map(|t| t.calls), [1, 1, 1, 1]);
        assert_eq!(engine.state(&last).unwrap().0, State::Fault);
        if index >= WARMUP {
            samples.push(sample);
        }
    }
    let summary = recorder.shutdown().unwrap();
    assert_eq!(summary.audit_records, (2 * (WARMUP + SAMPLES)) as u64);
    assert_eq!(summary.losses.dropped_audit_events, 0);
    samples
}

#[test]
#[ignore = "Release phase benchmark with SQLite audit"]
fn measure_rewire_phases() {
    println!("nodes,phase,calls,median_us");
    for nodes in [8, 32, 128] {
        let samples = series(nodes);
        for (index, phase) in [
            "first_dependents",
            "changed",
            "second_dependents",
            "invalidate",
        ]
        .into_iter()
        .enumerate()
        {
            let mut times: Vec<_> = samples.iter().map(|s| s[index].elapsed).collect();
            times.sort_unstable();
            let median_us = times[times.len() / 2].as_secs_f64() * 1e6;
            println!("{nodes},{phase},{},{median_us:.3}", samples[0][index].calls);
        }
    }
}
