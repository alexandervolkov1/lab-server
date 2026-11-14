//! Registered streams with real handles and deliberately unstarted workers.
//! Tests control publications/loss boundaries without background polling.
use std::io::{BufReader, Cursor};

use super::ObservationFanout;
use crate::device_registry::DeviceRegistry;
use crate::driver::{DeviceIo, Driver, DriverError};
use crate::model::{ParameterDescription, Value};
use crate::port_worker::{DeviceInstance, PortWorker};

struct CatalogDriver(Vec<ParameterDescription>);
impl Driver for CatalogDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        self.0.clone()
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        unreachable!("stream tests publish explicitly")
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        unreachable!("stream tests use TestBackend for commands")
    }
}

pub(crate) type StreamWorker = PortWorker<BufReader<Cursor<Vec<u8>>>>;

pub(crate) fn routed_fanout(
    capacity: usize,
    parameters: Vec<ParameterDescription>,
    ports: &[(&str, &str)],
) -> (ObservationFanout, Vec<StreamWorker>) {
    let mut registry = DeviceRegistry::new();
    let mut workers = Vec::new();
    for &(port, device) in ports {
        let instance = DeviceInstance::new(device, CatalogDriver(parameters.clone())).unwrap();
        let (worker, channels) = PortWorker::new(
            BufReader::new(Cursor::new(Vec::new())),
            vec![instance],
            1,
            1,
        )
        .unwrap();
        registry.register_port(port, channels.commands).unwrap();
        registry
            .register_device(port, &worker.devices()[0])
            .unwrap();
        workers.push(worker);
    }
    (ObservationFanout::routed(capacity, &registry), workers)
}
