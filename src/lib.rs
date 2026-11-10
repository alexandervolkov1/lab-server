//! Shared models, drivers and TCP components for legacy and routed backends.
//! The executable supports legacy Arduino and configured multi-port startup.

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
pub use tcp_legacy::{observation_json, queue_read, queue_set};
