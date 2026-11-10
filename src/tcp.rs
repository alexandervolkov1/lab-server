//! TCP adapter for the shared multi-device service. No second backend or cache.
use crate::connection::TcpConnection;
use crate::fanout::ObservationReceiver;
use crate::service::DeviceService;
use std::io;
use std::net::TcpStream;

/// Backward-compatible name for the shared backend.
pub use crate::service::DeviceService as MultiDeviceTcp;

impl DeviceService {
    /// Run the same transport as the legacy server for one accepted socket.
    /// The listener owner must retain a ClientSlots permit for this connection.
    pub fn serve_connection(&self, stream: TcpStream) -> io::Result<()> {
        let mut connection = TcpConnection::new(stream, 64, 64 * 1024)?;
        let observations = self.register_client()?;
        connection.run(self, &self.catalog, &self.latest, &observations)
    }

    pub fn drive_connection(
        &self,
        connection: &mut TcpConnection,
        observations: &ObservationReceiver,
    ) -> io::Result<bool> {
        connection.drive_once_with_observations(self, &self.catalog, &self.latest, observations)
    }
}
