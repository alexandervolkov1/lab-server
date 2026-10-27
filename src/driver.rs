use std::io::{self, BufRead, BufReader, Read, Write};

use crate::model::{ParameterDescription, Value};

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
    fn parameters(&self) -> Vec<ParameterDescription>;

    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, String>;

    fn write(
        &mut self,
        parameter: &str,
        value: &Value,
        io: &mut dyn DeviceIo,
    ) -> Result<(), String>;
}
