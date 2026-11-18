//! TCP adapter for the shared multi-device service. No second backend or cache.
use crate::connection::TcpConnection;
use crate::fanout::ObservationReceiver;
use crate::outbound::V1_OUTPUT_BYTES;
use crate::service::DeviceService;
use std::io;
use std::net::TcpStream;

/// Backward-compatible name for the shared backend.
pub use crate::service::DeviceService as MultiDeviceTcp;

impl DeviceService {
    /// Run the configured server's TCP transport for one accepted socket.
    /// The listener owner must retain a ClientSlots permit for this connection.
    pub fn serve_connection(&self, stream: TcpStream) -> io::Result<()> {
        let mut connection = TcpConnection::new(stream, 64, V1_OUTPUT_BYTES)?;
        let observations = self.register_client()?;
        loop {
            if self.drive_connection(&mut connection, &observations)? {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    pub fn drive_connection(
        &self,
        connection: &mut TcpConnection,
        observations: &ObservationReceiver,
    ) -> io::Result<bool> {
        let _gate = self.lock_service();
        connection.drive_once_with_observations(self, &self.catalog, &self.latest, observations)
    }
}
