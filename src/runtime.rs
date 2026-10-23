use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::driver::{DeviceIo, Driver};
use crate::model::{Observation, SignalDescription};

fn next_deadline(previous: Instant, period: Duration, now: Instant) -> Instant {
    let mut next = previous + period;

    while next <= now {
        next += period;
    }

    next
}

pub fn run_polling<D: Driver, P: DeviceIo>(
    mut driver: D,
    mut port: P,
    description: SignalDescription,
    source: String,
    period: Duration,
    tx: Sender<Observation>,
) {
    assert!(!period.is_zero(), "Polling period must be positive");

    let mut next = Instant::now();

    loop {
        let now = Instant::now();

        if now < next {
            thread::sleep(next - now);
        }

        let reading = driver.read(&description.id, &mut port).and_then(|value| {
            if description.spec.accepts(&value) {
                Ok(value)
            } else {
                Err("Value out of range".to_string())
            }
        });

        let observation = Observation {
            source: source.clone(),
            completed_at: SystemTime::now(),
            reading,
        };

        if tx.send(observation).is_err() {
            break;
        }

        next = next_deadline(next, period, Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
