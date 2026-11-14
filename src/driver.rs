use std::io::{self, BufRead, BufReader, Read, Write};

use crate::model::{ActionDescription, ParameterDescription, Value};

pub mod arduino;
pub mod metakon;

/// Whether another transaction may safely start on the shared physical stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortState {
    Synchronized,
    Desynchronized,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverError {
    pub message: String,
    pub port_state: PortState,
}

impl DriverError {
    /// Use Synchronized only if the transaction is known to be complete (or no
    /// I/O was attempted). An uncertain boundary must be Desynchronized.
    pub fn new(message: impl Into<String>, port_state: PortState) -> Self {
        Self {
            message: message.into(),
            port_state,
        }
    }
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DriverError {}

impl From<io::Error> for DriverError {
    fn from(error: io::Error) -> Self {
        let state = match error.kind() {
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof => PortState::Disconnected,
            _ => PortState::Desynchronized,
        };
        Self::new(error.to_string(), state)
    }
}

// Untyped errors have no evidence that the physical stream is still synchronized.
impl From<String> for DriverError {
    fn from(message: String) -> Self {
        Self::new(message, PortState::Desynchronized)
    }
}

impl From<&str> for DriverError {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

pub trait DeviceIo: BufRead {
    fn send(&mut self, request: &[u8]) -> io::Result<()>;
}

impl<T: Read + Write> DeviceIo for BufReader<T> {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        let port = self.get_mut();

        port.write_all(request)?;
        port.flush()
    }
}

pub trait Driver {
    fn parameters(&self) -> Vec<ParameterDescription>;

    fn actions(&self) -> Vec<ActionDescription> {
        Vec::new()
    }

    /// The same complete-transaction contract as write applies. An error does
    /// not prove the action had no effect. Never retry an uncertain transaction.
    fn invoke_action(&mut self, action: &str, _io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        Err(DriverError::new(
            format!("unsupported action {action:?}"),
            PortState::Synchronized,
        ))
    }

    /// Each call owns the entire transaction. Ok means a complete, correlated
    /// response has been consumed; no response tail may be left for another driver.
    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError>;

    /// The same transaction-boundary contract applies to writes. Returning an
    /// error after a write does not establish that the physical write failed.
    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), DriverError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_and_untyped_errors_never_claim_synchronization() {
        for kind in [
            io::ErrorKind::TimedOut,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::Other,
            io::ErrorKind::InvalidData,
        ] {
            assert_eq!(
                DriverError::from(io::Error::from(kind)).port_state,
                PortState::Desynchronized
            );
        }
        for kind in [
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::NotConnected,
        ] {
            assert_eq!(
                DriverError::from(io::Error::from(kind)).port_state,
                PortState::Disconnected
            );
        }
        assert_eq!(
            DriverError::from("untyped").port_state,
            PortState::Desynchronized
        );
    }
}
