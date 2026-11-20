use std::time::Duration;

use serde::Deserialize;
use thermal_plant_model::{SimulationConfig, ThermalParams, ThermalPlant, ThermalState};

use super::VirtualModel;
use crate::model::{ParameterDescription, Spec, Value};

const STEP: Duration = Duration::from_millis(100);
const _: () = assert!(thermal_plant_model::MODEL_STEP_SECONDS == 0.1);
const POLL_PERIOD: Option<Duration> = Some(Duration::from_secs(1));
const POWER: &str = "power";
const POWER_SPEC: Spec = Spec::Integer { min: 0, max: 100 };
type Temperature = (&'static str, fn(ThermalState) -> f32);
const TEMPERATURES: [Temperature; 3] = [
    ("heater_temperature", |state| state.heater_temp),
    ("sample_temperature", |state| state.sample_temp),
    ("room_temperature", |state| state.room_temp),
];

pub struct ThermalPlantModel {
    plant: ThermalPlant,
}

impl ThermalPlantModel {
    pub fn from_options(options: &toml::Table) -> Result<Self, String> {
        let config = SimulationOptions::deserialize(options.clone()).map_err(|e| e.to_string())?;
        let plant = ThermalPlant::new(config)?;
        Ok(Self { plant })
    }
}

impl VirtualModel for ThermalPlantModel {
    fn parameters(&self) -> Vec<ParameterDescription> {
        TEMPERATURES
            .iter()
            .map(|(name, _)| ParameterDescription {
                id: (*name).into(),
                spec: Spec::Float {
                    min: f64::from(f32::MIN),
                    max: f64::from(f32::MAX),
                },
                unit: "degC".into(),
                writable: false,
                default_poll_period: POLL_PERIOD,
            })
            .chain([ParameterDescription {
                id: POWER.into(),
                spec: POWER_SPEC,
                unit: "%".into(),
                writable: true,
                default_poll_period: POLL_PERIOD,
            }])
            .collect()
    }

    fn read(&self, parameter: &str) -> Result<Value, String> {
        if parameter == POWER {
            // The upstream API stores the integer command as an f32 fraction.
            return Ok(Value::Integer(self.plant.power_percent().round() as i64));
        }
        let (_, select) = TEMPERATURES
            .iter()
            .find(|(name, _)| *name == parameter)
            .ok_or_else(|| format!("unknown thermal plant parameter {parameter:?}"))?;
        Ok(Value::Float(f64::from(select(
            self.plant.measured_temperatures(),
        ))))
    }

    fn write(&mut self, parameter: &str, value: Value) -> Result<(), String> {
        if parameter != POWER {
            return Err(format!(
                "thermal plant parameter {parameter:?} is not writable"
            ));
        }
        match value {
            Value::Integer(power) if POWER_SPEC.accepts(&value) => self
                .plant
                .set_power_percent(power as f32)
                .map_err(str::to_owned),
            _ => Err("power must be an integer from 0 to 100".into()),
        }
    }

    fn step_period(&self) -> Duration {
        STEP
    }

    fn advance(&mut self, steps: u32) {
        for _ in 0..steps {
            self.plant.step();
        }
    }
}

// Remote deserialization gives the upstream configuration a strict TOML
// schema while inheriting every omitted value from SimulationConfig::default.
#[derive(Deserialize)]
#[serde(
    remote = "SimulationConfig",
    default = "SimulationConfig::default",
    deny_unknown_fields
)]
struct SimulationOptions {
    #[serde(with = "ParameterOptions")]
    parameters: ThermalParams,
    #[serde(with = "InitialStateOptions")]
    initial_state: ThermalState,
    background_temp: f32,
    seed: u32,
    parameter_spread: f32,
    heater_power_fluctuation: f32,
    sensor_noise: f32,
    sensor_drift_amplitude: f32,
}

#[derive(Deserialize)]
#[serde(
    remote = "ThermalParams",
    default = "default_parameters",
    deny_unknown_fields
)]
struct ParameterOptions {
    heater_capacity: f32,
    sample_capacity: f32,
    room_capacity: f32,
    coupling: f32,
    heater_loss: f32,
    sample_loss: f32,
    room_loss: f32,
    heater_radiation: f32,
    sample_radiation: f32,
    max_heater_power: f32,
}

fn default_parameters() -> ThermalParams {
    SimulationConfig::default().parameters
}

#[derive(Deserialize)]
#[serde(
    remote = "ThermalState",
    default = "default_initial_state",
    deny_unknown_fields
)]
struct InitialStateOptions {
    heater_temp: f32,
    sample_temp: f32,
    room_temp: f32,
}

fn default_initial_state() -> ThermalState {
    SimulationConfig::default().initial_state
}
