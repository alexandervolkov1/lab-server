use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::thread;

use super::*;
use crate::device_registry::{Availability, DeviceRegistry};
use crate::model::Spec;
use crate::port_worker::{DeviceInstance, PortWorker, StopReason};

fn timed(model: impl VirtualModel + 'static) -> (VirtualDeviceIo, VirtualDriver, Arc<AtomicU64>) {
    let millis = Arc::new(AtomicU64::new(0));
    let clock = millis.clone();
    let driver = VirtualDriver::new(&model);
    let io = VirtualDeviceIo::with_clock(Box::new(model), move || {
        Duration::from_millis(clock.load(Ordering::Relaxed))
    })
    .unwrap();
    (io, driver, millis)
}

fn thermal(options: &str) -> ThermalPlantModel {
    ThermalPlantModel::from_options(&options.parse().unwrap()).unwrap()
}

fn heater(driver: &mut VirtualDriver, io: &mut VirtualDeviceIo) -> f64 {
    let Value::Float(value) = driver.read("heater_temperature", io).unwrap() else {
        panic!("expected float temperature")
    };
    value
}

#[test]
fn plant_reads_initial_state_and_cools_after_heating() {
    let (mut io, mut driver, clock) = timed(thermal("[initial_state]\nsample_temp = 21.0"));
    for (parameter, initial) in [
        ("heater_temperature", Value::Float(20.0)),
        ("sample_temperature", Value::Float(21.0)),
        ("room_temperature", Value::Float(20.0)),
        ("power", Value::Integer(0)),
    ] {
        assert_eq!(driver.read(parameter, &mut io).unwrap(), initial);
    }
    driver
        .write("power", &Value::Integer(100), &mut io)
        .unwrap();
    clock.store(30_000, Ordering::Relaxed);
    // With no intervening reads, the write must catch up using the old power.
    driver.write("power", &Value::Integer(0), &mut io).unwrap();
    let hot = heater(&mut driver, &mut io);
    assert!(hot > 35.0, "heater reached {hot}");
    assert_eq!(heater(&mut driver, &mut io), hot);
    clock.store(60_000, Ordering::Relaxed);
    let cooled = heater(&mut driver, &mut io);
    assert!(cooled < hot, "heater cooled from {hot} to {cooled}");
}

fn trajectory(seed: u32, read_interval_ms: usize) -> Vec<Value> {
    let options = format!(
        "seed = {seed}\nsensor_noise = 1.0\nheater_power_fluctuation = 0.05\nparameter_spread = 0.05"
    );
    let (mut io, mut driver, clock) = timed(thermal(&options));
    let mut observations = Vec::new();
    let mut previous = 0;
    // Include commands between fixed steps to verify the epoch is preserved.
    for (at, power) in [
        (35, 63),
        (5_275, 18),
        (11_999, 100),
        (21_334, 0),
        (28_000, 0),
    ] {
        for tick in (previous..at).step_by(read_interval_ms) {
            clock.store(tick, Ordering::Relaxed);
            driver.read("room_temperature", &mut io).unwrap();
        }
        clock.store(at, Ordering::Relaxed);
        driver
            .write("power", &Value::Integer(power), &mut io)
            .unwrap();
        for name in [
            "heater_temperature",
            "sample_temperature",
            "room_temperature",
        ] {
            observations.push(driver.read(name, &mut io).unwrap());
        }
        previous = at;
    }
    observations
}

#[test]
fn seeded_trajectory_is_reproducible_and_independent_of_read_frequency() {
    let sparse = trajectory(312, 30_000);
    assert_eq!(sparse, trajectory(312, 30_000));
    assert_eq!(sparse, trajectory(312, 37));
    assert_ne!(sparse, trajectory(313, 37));
}

#[test]
fn plant_rejects_invalid_options_and_writes() {
    for options in [
        "seed = -1",
        "seed = 1.0",
        "sensor_noise = 2.1",
        "background_temp = nan",
        "[parameters]\nheater_capacity = 0.0",
        "[initial_state]\nroom_temp = -300.0",
        "unexpected = true",
        "[parameters]\nextra = 1.0",
        "[initial_state]\nextra = 1.0",
    ] {
        assert!(
            ThermalPlantModel::from_options(&options.parse().unwrap()).is_err(),
            "{options}"
        );
    }
    let (mut io, mut driver, _) = timed(thermal("[parameters]\nmax_heater_power = 150.0"));
    for value in [
        Value::Integer(-1),
        Value::Integer(101),
        Value::Float(30.0),
        Value::Boolean(false),
    ] {
        assert_eq!(
            driver
                .write("power", &value, &mut io)
                .unwrap_err()
                .port_state,
            PortState::Synchronized
        );
    }
    assert!(
        driver
            .write("room_temperature", &Value::Float(30.0), &mut io)
            .is_err()
    );
    assert_eq!(driver.read("power", &mut io).unwrap(), Value::Integer(0));
    for value in 0..=100 {
        driver
            .write("power", &Value::Integer(value), &mut io)
            .unwrap();
        assert_eq!(
            driver.read("power", &mut io).unwrap(),
            Value::Integer(value)
        );
    }
}

const COUNTER: &str = "counter\n\"";

#[derive(Default)]
struct Counter(i64);

impl VirtualModel for Counter {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: COUNTER.into(),
            spec: Spec::Integer {
                min: i64::MIN,
                max: i64::MAX,
            },
            unit: "".into(),
            writable: true,
            default_poll_period: None,
        }]
    }

    fn actions(&self) -> Vec<ActionDescription> {
        vec![ActionDescription {
            name: "zero".into(),
            description: "Reset counter".into(),
        }]
    }

    fn read(&self, parameter: &str) -> Result<Value, String> {
        if parameter == COUNTER {
            Ok(Value::Integer(self.0))
        } else {
            Err("unknown\nparameter".into())
        }
    }

    fn write(&mut self, parameter: &str, value: Value) -> Result<(), String> {
        match value {
            Value::Integer(value) if parameter == COUNTER => {
                self.0 = value;
                Ok(())
            }
            _ => Err("invalid counter write".into()),
        }
    }

    fn invoke_action(&mut self, action: &str) -> Result<(), String> {
        if action != "zero" {
            return Err("unknown action".into());
        }
        self.0 = 0;
        Ok(())
    }

    fn step_period(&self) -> Duration {
        Duration::from_millis(25)
    }

    fn advance(&mut self, steps: u32) {
        self.0 += i64::from(steps);
    }
}

#[test]
fn another_model_reuses_catalog_transport_and_actions() {
    let (mut io, mut driver, clock) = timed(Counter::default());
    assert_eq!(driver.parameters()[0].id, COUNTER);
    assert_eq!(driver.actions()[0].name, "zero");
    let error = driver.read("invalid", &mut io).unwrap_err();
    assert_eq!(error.port_state, PortState::Synchronized);
    assert_eq!(error.message, "unknown\nparameter");
    assert!(io.fill_buf().unwrap().is_empty());
    clock.store(49, Ordering::Relaxed);
    assert_eq!(driver.read(COUNTER, &mut io).unwrap(), Value::Integer(1));
    clock.store(50, Ordering::Relaxed);
    assert_eq!(driver.read(COUNTER, &mut io).unwrap(), Value::Integer(2));
    driver
        .write(COUNTER, &Value::Integer(i64::MAX), &mut io)
        .unwrap();
    assert_eq!(
        driver.read(COUNTER, &mut io).unwrap(),
        Value::Integer(i64::MAX)
    );
    driver.invoke_action("zero", &mut io).unwrap();
    assert_eq!(driver.read(COUNTER, &mut io).unwrap(), Value::Integer(0));
}

#[test]
fn transport_rejects_incomplete_requests_and_overlapping_transactions() {
    let (mut io, mut driver, _) = timed(Counter::default());
    let mut request =
        serde_json::to_vec(&Request::Write(COUNTER.into(), WireValue::Integer(42))).unwrap();
    assert!(io.send(&request).is_err());
    assert_eq!(driver.read(COUNTER, &mut io).unwrap(), Value::Integer(0));
    request.push(b'\n');
    assert!(
        io.send(&[request.as_slice(), request.as_slice()].concat())
            .is_err()
    );
    io.send(&request).unwrap();
    let mut first = [0];
    io.read_exact(&mut first).unwrap();
    assert!(io.send(&request).is_err());
    let mut tail = Vec::new();
    io.read_to_end(&mut tail).unwrap();
    assert_eq!([&first[..], &tail].concat(), b"\"Complete\"\n");
    assert_eq!(driver.read(COUNTER, &mut io).unwrap(), Value::Integer(42));
}

#[test]
fn excess_catch_up_is_explicit_and_does_not_step_or_write() {
    let (mut io, mut driver, clock) = timed(Counter::default());
    let last_allowed = u64::from(MAX_STEPS_PER_TRANSACTION) * 25;
    clock.store(last_allowed, Ordering::Relaxed);
    let initial = Value::Integer(i64::from(MAX_STEPS_PER_TRANSACTION));
    assert_eq!(driver.read(COUNTER, &mut io).unwrap(), initial);
    clock.store(last_allowed - 1, Ordering::Relaxed);
    let error = driver.read(COUNTER, &mut io).unwrap_err();
    assert_eq!(error.port_state, PortState::Synchronized);
    assert!(error.message.contains("backwards"));

    clock.store(2 * last_allowed + 25, Ordering::Relaxed);
    let error = driver
        .write(COUNTER, &Value::Integer(1), &mut io)
        .unwrap_err();
    assert_eq!(error.port_state, PortState::Disconnected);
    assert!(error.message.contains("catch-up"));
    assert!(io.fill_buf().unwrap().is_empty());
    assert_eq!(io.model.read(COUNTER).unwrap(), initial);
    assert_eq!(io.last_elapsed, Duration::from_millis(last_allowed));
    clock.store(u64::MAX, Ordering::Relaxed);
    assert_eq!(
        driver.read(COUNTER, &mut io).unwrap_err().port_state,
        PortState::Disconnected
    );
}

#[test]
fn catch_up_limit_stops_worker_and_registry_device() {
    let (io, driver, clock) = timed(Counter::default());
    let mut device = DeviceInstance::new("counter", driver).unwrap();
    device
        .set_poll_period(COUNTER, Some(Duration::from_millis(100)))
        .unwrap();
    let (worker, channels) = PortWorker::new(io, vec![device], 2, 2).unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("virtual", channels.commands.clone())
        .unwrap();
    registry
        .register_device("virtual", &worker.devices()[0])
        .unwrap();
    assert_eq!(
        registry.availability("counter"),
        Some(Availability::Available)
    );

    clock.store(
        25 * (u64::from(MAX_STEPS_PER_TRANSACTION) + 1),
        Ordering::Relaxed,
    );
    let join = thread::spawn(move || worker.run());
    let wait = Duration::from_secs(5);
    let last = channels.observations.recv_timeout(wait);
    let terminal = channels.observations.recv_timeout(wait);
    if !matches!(terminal, Err(RecvTimeoutError::Disconnected)) {
        channels.commands.shutdown();
    }
    let reason = join.join().unwrap();

    let StopReason::Driver(error) = &reason else {
        panic!("unexpected stop reason: {reason:?}")
    };
    assert_eq!(error.port_state, PortState::Disconnected);
    assert!(error.message.contains("catch-up"));
    assert_eq!(
        last.unwrap().observation.reading,
        Err(error.message.clone())
    );
    assert!(matches!(terminal, Err(RecvTimeoutError::Disconnected)));
    assert!(!channels.commands.source_alive());
    assert_eq!(
        registry.availability("counter"),
        Some(Availability::Stopped { reason })
    );
}
