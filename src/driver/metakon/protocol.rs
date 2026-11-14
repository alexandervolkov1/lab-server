use std::io;

use crate::driver::{DeviceIo, DriverError, PortState};
use crate::model::Value;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum WireType {
    Bool = 0,
    U8 = 1,
    I8 = 2,
    U16 = 3,
    I16 = 4,
}

fn malformed(message: &str) -> DriverError {
    DriverError::new(format!("metakon {message}"), PortState::Desynchronized)
}

impl WireType {
    pub(super) fn width(self) -> usize {
        match self {
            Self::U16 | Self::I16 => 2,
            _ => 1,
        }
    }

    pub(super) fn decode(self, bytes: &[u8]) -> Result<Value, DriverError> {
        if bytes.len() != self.width() {
            return Err(malformed("invalid value length"));
        }
        Ok(match self {
            Self::Bool => Value::Boolean(match bytes[0] {
                0 => false,
                0xff => true,
                _ => return Err(malformed("invalid Boolean encoding (expected 00/FF)")),
            }),
            Self::U8 => Value::Integer(i64::from(bytes[0])),
            Self::I8 => Value::Integer(i64::from(bytes[0] as i8)),
            Self::U16 => Value::Integer(i64::from(u16::from_le_bytes([bytes[0], bytes[1]]))),
            Self::I16 => Value::Integer(i64::from(i16::from_le_bytes([bytes[0], bytes[1]]))),
        })
    }
}

// Manufacturer's CRC-8, reflected polynomial 0x8C, init FF, no final XOR.
pub(super) fn crc(bytes: &[u8]) -> u8 {
    let mut crc = 0xff;
    for byte in bytes {
        crc ^= byte;
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0x8c } else { 0 };
        }
    }
    crc
}

pub(super) fn read_request(address: u8, channel: u8, register: u8) -> [u8; 5] {
    let mut frame = [address, channel, register, 0, 0];
    frame[4] = crc(&frame[..4]);
    frame
}

pub(super) fn write_request(
    address: u8,
    channel: u8,
    register: u8,
    wire: WireType,
    raw: &Value,
) -> Vec<u8> {
    let mut frame = vec![address, channel, register, 1, wire as u8];
    match raw {
        Value::Boolean(value) => frame.push(if *value { 0xff } else { 0 }),
        Value::Integer(value) => frame.extend_from_slice(&value.to_le_bytes()[..wire.width()]),
        Value::Float(_) => unreachable!("write value already validated and converted to raw"),
    }
    frame.push(crc(&frame));
    frame
}

pub(super) fn validate_reply(frame: &[u8], header: &[u8]) -> Result<(), DriverError> {
    if frame.len() < 5 || crc(&frame[..frame.len() - 1]) != frame[frame.len() - 1] {
        return Err(malformed("response CRC mismatch"));
    }
    if &frame[..4] != header {
        return Err(malformed(
            "response address/channel/register/command mismatch",
        ));
    }
    Ok(())
}

pub(super) fn exchange(
    io: &mut dyn DeviceIo,
    request: &[u8],
    length: usize,
) -> Result<Vec<u8>, DriverError> {
    io.send(request)?;
    let mut frame = Vec::with_capacity(length);
    while frame.len() < length {
        let available = match io.fill_buf() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "metakon incomplete response / EOF",
            )
            .into());
        }
        // There are no unsolicited responses or pipelined requests. Reject any
        // already-buffered tail instead of leaving it for the next transaction.
        if available.len() > length - frame.len() {
            return Err(malformed("response longer than expected"));
        }
        let count = available.len();
        frame.extend_from_slice(available);
        io.consume(count);
    }
    Ok(frame)
}
