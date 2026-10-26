use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::driver::{DeviceIo, Driver};
use crate::model::{Observation, SignalDescription};

pub struct PollTask {
    pub description: SignalDescription,
    pub source: String,
    pub period: Duration,
}

struct ScheduledPoll {
    task: PollTask,
    next_at: Instant,
}

fn next_deadline(previous: Instant, period: Duration, now: Instant) -> Instant {
    let mut next = previous + period;

    while next <= now {
        next += period;
    }

    next
}

fn earliest_index(schedule: &[ScheduledPoll]) -> usize {
    schedule
        .iter()
        .enumerate()
        .min_by_key(|(_, item)| item.next_at)
        .map(|(index, _)| index)
        .expect("Polling schedule must not be empty")
}

pub fn run_polling<D: Driver, P: DeviceIo>(
    mut driver: D,
    mut port: P,
    tasks: Vec<PollTask>,
    tx: Sender<Observation>,
) {
    assert!(!tasks.is_empty(), "At least one polling task is required");

    assert!(
        tasks.iter().all(|task| !task.period.is_zero()),
        "Polling periods must be positive"
    );

    let start = Instant::now();

    let mut schedule: Vec<ScheduledPoll> = tasks
        .into_iter()
        .map(|task| ScheduledPoll {
            task,
            next_at: start,
        })
        .collect();

    loop {
        let index = earliest_index(&schedule);
        let deadline = schedule[index].next_at;

        let now = Instant::now();

        if now < deadline {
            thread::sleep(deadline - now);
        }

        let task = &schedule[index].task;

        let reading = driver
            .read(&task.description.id, &mut port)
            .and_then(|value| {
                if task.description.spec.accepts(&value) {
                    Ok(value)
                } else {
                    Err("Value out of range".to_string())
                }
            });

        let observation = Observation {
            source: task.source.clone(),
            unit: task.description.unit.clone(),
            completed_at: SystemTime::now(),
            reading,
        };
        let period = task.period;

        if tx.send(observation).is_err() {
            break;
        }

        schedule[index].next_at = next_deadline(deadline, period, Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Spec;

    fn test_task(id: &str, period_ms: u64) -> PollTask {
        PollTask {
            description: SignalDescription {
                id: id.to_string(),
                spec: Spec::Float {
                    min: 0.0,
                    max: 100.0,
                },
                unit: "test".to_string(),
            },

            source: id.to_string(),
            period: Duration::from_millis(period_ms),
        }
    }

    #[test]
    fn preserves_polling_phase() {
        let start = Instant::now();
        let period = Duration::from_secs(1);

        assert_eq!(
            next_deadline(start, period, start + Duration::from_millis(250)),
            start + Duration::from_secs(1)
        );

        assert_eq!(
            next_deadline(start, period, start + Duration::from_millis(1350)),
            start + Duration::from_secs(2)
        );

        assert_eq!(
            next_deadline(start, period, start + Duration::from_millis(4200)),
            start + Duration::from_secs(5)
        );
    }

    #[test]
    fn schedules_independed_signals() {
        let start = Instant::now();

        let mut schedule = vec![
            ScheduledPoll {
                task: test_task("fast", 200),
                next_at: start,
            },
            ScheduledPoll {
                task: test_task("slow", 1000),
                next_at: start,
            },
        ];

        let expected = [
            ("fast", 0),
            ("slow", 0),
            ("fast", 200),
            ("fast", 400),
            ("fast", 600),
            ("fast", 800),
            ("fast", 1000),
            ("slow", 1000),
            ("fast", 1200),
        ];

        for (name, time_ms) in expected {
            let index = earliest_index(&schedule);
            let item = &schedule[index];

            assert_eq!(item.task.description.id, name);
            assert_eq!(item.next_at, start + Duration::from_millis(time_ms));

            let previous = item.next_at;
            let period = item.task.period;

            schedule[index].next_at = next_deadline(previous, period, previous);
        }
    }
}
