//! Models hosted by the standard port worker through an in-memory request/reply transport.

use std::io::{self, BufRead, Cursor, Read};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::driver::{DeviceIo, Driver, DriverError, PortState};
use crate::model::{ActionDescription, ParameterDescription, Value};

mod thermal_plant;
pub use thermal_plant::ThermalPlantModel;

pub trait VirtualModel: Send {
    fn parameters(&self) -> Vec<ParameterDescription>;

    fn actions(&self) -> Vec<ActionDescription> {
        Vec::new()
    }

    fn read(&self, parameter: &str) -> Result<Value, String>;
    fn write(&mut self, parameter: &str, value: Value) -> Result<(), String>;

    fn invoke_action(&mut self, action: &str) -> Result<(), String> {
        Err(format!("unsupported action {action:?}"))
    }

    /// The model integrates in positive, fixed-duration steps.
    fn step_period(&self) -> Duration;
    fn advance(&mut self, steps: u32);
}

pub(crate) fn create_model(
    name: &str,
    options: &toml::Table,
) -> Result<Box<dyn VirtualModel>, String> {
    match name {
        "thermal_plant" => Ok(Box::new(ThermalPlantModel::from_options(options)?)),
        _ => Err(format!("unknown virtual model {name:?}")),
    }
}

// There is exactly one outstanding frame. The in-memory transport refuses to
// accept another request until the previous reply has been consumed.
#[derive(Serialize, Deserialize)]
enum Request {
    Read(String),
    Write(String, WireValue),
    Invoke(String),
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Reading(WireValue),
    Complete,
    Error(String),
}

// Keep all model values exact across the private wire format.
#[derive(Serialize, Deserialize)]
enum WireValue {
    FloatBits(u64),
    Integer(i64),
    Boolean(bool),
}

impl From<Value> for WireValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Float(value) => Self::FloatBits(value.to_bits()),
            Value::Integer(value) => Self::Integer(value),
            Value::Boolean(value) => Self::Boolean(value),
        }
    }
}

impl From<WireValue> for Value {
    fn from(value: WireValue) -> Self {
        match value {
            WireValue::FloatBits(bits) => Self::Float(f64::from_bits(bits)),
            WireValue::Integer(value) => Self::Integer(value),
            WireValue::Boolean(value) => Self::Boolean(value),
        }
    }
}

const MAX_STEPS_PER_TRANSACTION: u32 = 600;

pub struct VirtualDeviceIo {
    model: Box<dyn VirtualModel>,
    reply: Cursor<Vec<u8>>,
    clock: Box<dyn Fn() -> Duration + Send>,
    period: Duration,
    last_elapsed: Duration,
}

impl VirtualDeviceIo {
    pub fn new(model: Box<dyn VirtualModel>) -> Result<Self, String> {
        let epoch = Instant::now();
        Self::with_clock(model, move || epoch.elapsed())
    }

    /// `clock` reports monotonic elapsed time since model time zero.
    pub fn with_clock(
        model: Box<dyn VirtualModel>,
        clock: impl Fn() -> Duration + Send + 'static,
    ) -> Result<Self, String> {
        let period = model.step_period();
        if period.is_zero() {
            return Err("virtual model step period must be positive".into());
        }
        Ok(Self {
            model,
            reply: Cursor::new(Vec::new()),
            clock: Box::new(clock),
            period,
            last_elapsed: Duration::ZERO,
        })
    }

    fn advance_to_now(&mut self) -> Result<(), String> {
        let elapsed = (self.clock)();
        if elapsed < self.last_elapsed {
            return Err("virtual model clock moved backwards".into());
        }
        let period = self.period.as_nanos();
        let steps = elapsed.as_nanos() / period - self.last_elapsed.as_nanos() / period;
        if steps > u128::from(MAX_STEPS_PER_TRANSACTION) {
            return Err(format!(
                "virtual model catch-up needs {steps} steps, limit is {MAX_STEPS_PER_TRANSACTION}"
            ));
        }
        // Advance first so mutations use the old inputs for every completed
        // step. Fractions of a step stay aligned to the original clock epoch.
        if steps != 0 {
            self.model.advance(steps as u32);
        }
        self.last_elapsed = elapsed;
        Ok(())
    }

    fn execute(&mut self, request: Request) -> Result<Reply, String> {
        self.advance_to_now()?;
        match request {
            Request::Read(parameter) => self
                .model
                .read(&parameter)
                .map(|v| Reply::Reading(v.into())),
            Request::Write(parameter, value) => self
                .model
                .write(&parameter, value.into())
                .map(|()| Reply::Complete),
            Request::Invoke(action) => self.model.invoke_action(&action).map(|()| Reply::Complete),
        }
    }
}

impl Read for VirtualDeviceIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reply.read(buf)
    }
}

impl BufRead for VirtualDeviceIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.reply.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.reply.consume(amount);
    }
}

impl DeviceIo for VirtualDeviceIo {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        if !self.reply.fill_buf()?.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unread virtual reply",
            ));
        }
        let request = request
            .strip_suffix(b"\n")
            .filter(|line| !line.contains(&b'\n'))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected one virtual request frame",
                )
            })?;
        let request = serde_json::from_slice(request)?;
        let reply = self.execute(request).unwrap_or_else(Reply::Error);
        self.reply.get_mut().clear();
        self.reply.set_position(0);
        serde_json::to_writer(self.reply.get_mut(), &reply)?;
        self.reply.get_mut().push(b'\n');
        Ok(())
    }
}

pub struct VirtualDriver {
    parameters: Vec<ParameterDescription>,
    actions: Vec<ActionDescription>,
}

impl VirtualDriver {
    pub fn new(model: &dyn VirtualModel) -> Self {
        Self {
            parameters: model.parameters(),
            actions: model.actions(),
        }
    }

    fn exchange(request: Request, io: &mut dyn DeviceIo) -> Result<Reply, DriverError> {
        let mut frame = serde_json::to_vec(&request)
            .map_err(|error| DriverError::new(error.to_string(), PortState::Synchronized))?;
        frame.push(b'\n');
        io.send(&frame)?;
        let mut frame = Vec::new();
        if io.read_until(b'\n', &mut frame)? == 0 {
            return Err(DriverError::new(
                "empty virtual reply",
                PortState::Disconnected,
            ));
        }
        let payload = frame
            .strip_suffix(b"\n")
            .ok_or_else(|| malformed_reply("incomplete frame"))?;
        match serde_json::from_slice(payload).map_err(malformed_reply)? {
            Reply::Error(message) => Err(DriverError::new(message, PortState::Synchronized)),
            reply => Ok(reply),
        }
    }

    fn mutate(request: Request, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        match Self::exchange(request, io)? {
            Reply::Complete => Ok(()),
            _ => Err(malformed_reply("expected mutation acknowledgment")),
        }
    }
}

fn malformed_reply(reason: impl std::fmt::Display) -> DriverError {
    DriverError::new(
        format!("invalid virtual reply: {reason}"),
        PortState::Desynchronized,
    )
}

impl Driver for VirtualDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        self.parameters.clone()
    }

    fn actions(&self) -> Vec<ActionDescription> {
        self.actions.clone()
    }

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        match Self::exchange(Request::Read(parameter.into()), io)? {
            Reply::Reading(value) => Ok(value.into()),
            _ => Err(malformed_reply("expected reading")),
        }
    }

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError> {
        Self::mutate(Request::Write(parameter.into(), value.clone().into()), io)
    }

    fn invoke_action(&mut self, action: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        Self::mutate(Request::Invoke(action.into()), io)
    }
}

#[cfg(test)]
mod tests;
