use std::io::{self, BufRead, BufReader, Read, Write};

use crate::model::{SignalDescription, Value};

pub mod arduino;

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
    fn signals(&self) -> Vec<SignalDescription>;

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, String>;
}
