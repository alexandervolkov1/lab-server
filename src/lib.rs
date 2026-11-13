//! Models, drivers, transport sessions and configured multi-port server startup.
//! The legacy polling worker remains temporarily as a transport test fixture.

pub mod command_router;
pub mod config;
pub mod device_registry;
pub mod driver;
pub mod model;
pub mod observation_aggregator;
pub mod pending;
pub mod port_worker;
pub mod recorder;
pub mod startup;

pub mod client_limit;
pub mod connection;
pub mod fanout;
pub mod outbound;
pub mod protocol;
pub mod runtime;
pub mod service;
pub mod session;
pub mod tcp;
mod tcp_legacy;
pub mod websocket;
pub use protocol::observation_json;
pub use tcp_legacy::{queue_read, queue_set};
