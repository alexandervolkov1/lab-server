//! Modbus RTU unicast framing and register reads on a worker-owned stream.

use std::io;
use std::thread;
use std::time::Duration;

use super::{DeviceIo, DriverError, PortState};

/// Serial-line t3.5. Pass the format configured on the physical port and
/// increase the result if the device requires a longer silent interval.
pub fn interframe_silence(
    baud: u32,
    data_bits: u8,
    parity_bit: bool,
    stop_bits: u8,
) -> Result<Duration, DriverError> {
    if baud == 0 || data_bits != 8 || !(1..=2).contains(&stop_bits) {
        return Err(invalid("invalid RTU serial format"));
    }
    if baud > 19_200 {
        return Ok(Duration::from_micros(1_750));
    }
    let bits = 1 + u64::from(data_bits) + u64::from(parity_bit) + u64::from(stop_bits);
    Ok(Duration::from_nanos(
        (3_500_000_000 * bits).div_ceil(u64::from(baud)),
    ))
}

/// CRC-16/MODBUS. Serialize the result in little-endian byte order.
pub fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0xffff;
    for &byte in bytes {
        crc ^= u16::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 == 0 { 0 } else { 0xa001 };
        }
    }
    crc
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RegisterTable {
    Holding = 0x03,
    Input = 0x04,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadRegisters {
    slave: u8,
    table: RegisterTable,
    start: u16,
    count: u16,
}

impl ReadRegisters {
    /// Uses standard unicast addresses and zero-based register offsets.
    pub fn new(
        slave: u8,
        table: RegisterTable,
        start: u16,
        count: u16,
    ) -> Result<Self, DriverError> {
        if !(1..=247).contains(&slave) {
            return Err(invalid("slave address must be 1..=247"));
        }
        if !(1..=125).contains(&count) || start.checked_add(count - 1).is_none() {
            return Err(invalid(
                "invalid register range (expected 1..=125 registers)",
            ));
        }
        Ok(Self {
            slave,
            table,
            start,
            count,
        })
    }

    pub fn encode(self) -> [u8; 8] {
        let [start_hi, start_lo] = self.start.to_be_bytes();
        let [count_hi, count_lo] = self.count.to_be_bytes();
        let mut frame = [
            self.slave,
            self.table as u8,
            start_hi,
            start_lo,
            count_hi,
            count_lo,
            0,
            0,
        ];
        let crc = crc16(&frame[..6]);
        frame[6..].copy_from_slice(&crc.to_le_bytes());
        frame
    }

    /// Decodes exactly one complete ADU, returning raw unsigned registers.
    pub fn decode(self, frame: &[u8]) -> Result<Vec<u16>, DriverError> {
        let data = response_data(frame, self.slave, self.table as u8, self.response_len())?;
        if usize::from(data[0]) != data.len() - 1 {
            return Err(desync("response byte count mismatch"));
        }
        Ok(data[1..]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&word| u16::from_be_bytes(word))
            .collect())
    }

    /// Sends once and waits for a correlated reply. The caller supplies the
    /// larger of the line and device silence intervals. DeviceIo supplies the
    /// receive timeout. Host buffering obscures physical inter-character gaps.
    pub fn exchange(
        self,
        io: &mut dyn DeviceIo,
        silence: Duration,
    ) -> Result<Vec<u16>, DriverError> {
        let frame = exchange(io, &self.encode(), self.response_len(), silence)?;
        self.decode(&frame)
    }

    fn response_len(self) -> usize {
        5 + 2 * usize::from(self.count)
    }
}

fn invalid(message: &str) -> DriverError {
    DriverError::new(format!("modbus RTU: {message}"), PortState::Synchronized)
}

fn desync(message: &str) -> DriverError {
    DriverError::new(format!("modbus RTU: {message}"), PortState::Desynchronized)
}

fn response_len(
    slave: u8,
    function: u8,
    header: &[u8],
    normal: usize,
) -> Result<usize, DriverError> {
    if header[0] != slave {
        return Err(desync("response slave address mismatch"));
    }
    if header[1] == function {
        Ok(normal)
    } else if header[1] == function | 0x80 {
        Ok(5)
    } else {
        Err(desync("response function code mismatch"))
    }
}

fn response_data(
    frame: &[u8],
    slave: u8,
    function: u8,
    normal_len: usize,
) -> Result<&[u8], DriverError> {
    if frame.len() < 5 || frame.len() != response_len(slave, function, frame, normal_len)? {
        return Err(desync("invalid response length"));
    }
    let (body, crc) = frame.split_at(frame.len() - 2);
    if crc16(body).to_le_bytes() != crc {
        return Err(desync("response CRC mismatch"));
    }
    if frame[1] != function {
        return Err(DriverError::new(
            format!(
                "modbus RTU exception 0x{:02X} (function 0x{function:02X})",
                frame[2]
            ),
            PortState::Synchronized,
        ));
    }
    Ok(&body[2..])
}

// Function-specific codecs determine the expected normal length and validate
// their own payload. The wire exchange only distinguishes normal and exception.
fn exchange(
    io: &mut dyn DeviceIo,
    request: &[u8],
    normal_len: usize,
    silence: Duration,
) -> Result<Vec<u8>, DriverError> {
    // A different driver might have owned the preceding exchange on this bus.
    thread::sleep(silence);
    io.send(request)?;
    let mut frame = Vec::with_capacity(normal_len);
    let mut length = 2;
    while frame.len() < length {
        let available = match io.fill_buf() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if available.is_empty() {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "modbus RTU response EOF").into(),
            );
        }
        let count = available.len().min(length - frame.len());
        frame.extend_from_slice(&available[..count]);
        if frame.len() == 2 {
            length = response_len(request[0], request[1], &frame, normal_len)?;
        }
        if frame.len() == length && count < available.len() {
            return Err(desync("unexpected buffered bytes after response"));
        }
        io.consume(count);
    }
    // Keep port ownership until the end of the guard, including on exceptions.
    thread::sleep(silence);
    Ok(frame)
}

#[cfg(test)]
mod tests;
