use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::driver::{DeviceIo, Driver};
use crate::model::{Observation, ParameterDescription, Value};

pub struct PollTask {
    pub description: ParameterDescription,
    pub source: String,
    pub period: Duration,
}

pub struct SetCommand {
    pub parameter: String,
    pub value: Value,
    pub reply: Sender<Result<(), String>>,
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

fn execute_set<D: Driver, P: DeviceIo>(driver: &mut D, port: &mut P, command: SetCommand) {
    let result = driver.write(&command.parameter, &command.value, port);
    let _ = command.reply.send(result);
}

pub fn run_polling<D: Driver, P: DeviceIo>(
    mut driver: D,
    mut port: P,
    tasks: Vec<PollTask>,
    tx: Sender<Observation>,
    commands: Receiver<SetCommand>,
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
            match commands.recv_timeout(deadline - now) {
                Ok(command) => {
                    execute_set(&mut driver, &mut port, command);
                    continue;
                }

                Err(RecvTimeoutError::Timeout) => {}

                Err(RecvTimeoutError::Disconnected) => {
                    thread::sleep(deadline - now);
                }
            }
        }

        if let Ok(command) = commands.try_recv() {
            execute_set(&mut driver, &mut port, command);
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
    use std::io::{BufReader, Cursor};

    use super::*;
    use crate::model::Spec;

    fn test_task(id: &str, period_ms: u64) -> PollTask {
        PollTask {
            description: ParameterDescription {
                id: id.to_string(),
                spec: Spec::Float {
                    min: 0.0,
                    max: 100.0,
                },
                unit: "test".to_string(),
                writable: false,
            },

            source: id.to_string(),
            period: Duration::from_millis(period_ms),
        }
    }

    struct SlowDriver {
        read_delay: Duration,
        write_delay: Duration,
    }

    impl Driver for SlowDriver {
        fn parameters(&self) -> Vec<ParameterDescription> {
            Vec::new()
        }

        fn read(&mut self, _parameter: &str, _io: &mut dyn DeviceIo) -> Result<Value, String> {
            thread::sleep(self.read_delay);
            Ok(Value::Float(1.0))
        }

        fn write(
            &mut self,
            _parameter: &str,
            _value: &Value,
            _io: &mut dyn DeviceIo,
        ) -> Result<(), String> {
            thread::sleep(self.write_delay);
            Ok(())
        }
    }

    fn spawn_overloaded_worker(
        driver: SlowDriver,
        commands: Receiver<SetCommand>,
    ) -> (Receiver<Observation>, thread::JoinHandle<()>) {
        let (tx, rx) = std::sync::mpsc::channel();

        let worker = thread::spawn(move || {
            let port = BufReader::new(Cursor::new(Vec::<u8>::new()));

            let tasks = (0..4)
                .map(|index| test_task(&format!("signal_{index}"), 10))
                .collect();

            run_polling(driver, port, tasks, tx, commands);
        });

        (rx, worker)
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

    #[test]
    fn processes_set_during_overloaded_polling() {
        let (command_tx, command_rx) = std::sync::mpsc::channel();

        let (observations, worker) = spawn_overloaded_worker(
            SlowDriver {
                read_delay: Duration::from_millis(20),
                write_delay: Duration::ZERO,
            },
            command_rx,
        );

        observations.recv_timeout(Duration::from_secs(1)).unwrap();

        let (reply_tx, reply_rx) = std::sync::mpsc::channel();

        command_tx
            .send(SetCommand {
                parameter: "power".to_string(),
                value: Value::Integer(10),
                reply: reply_tx,
            })
            .unwrap();

        let result = reply_rx.recv_timeout(Duration::from_secs(1));

        drop(observations);
        worker.join().unwrap();

        assert_eq!(result.unwrap(), Ok(()));
    }

    #[test]
    fn polling_continues_with_queued_writes() {
        let (command_tx, command_rx) = std::sync::mpsc::channel();
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();

        for _ in 0..100 {
            command_tx
                .send(SetCommand {
                    parameter: "power".to_string(),
                    value: Value::Integer(10),
                    reply: reply_tx.clone(),
                })
                .unwrap();
        }

        let (observations, worker) = spawn_overloaded_worker(
            SlowDriver {
                read_delay: Duration::from_millis(20),
                write_delay: Duration::from_millis(20),
            },
            command_rx,
        );

        let reply = reply_rx.recv_timeout(Duration::from_secs(1));
        let observation = observations.recv_timeout(Duration::from_secs(1));

        drop(observations);
        worker.join().unwrap();

        assert_eq!(reply.unwrap(), Ok(()));
        assert!(observation.is_ok());
    }
}
