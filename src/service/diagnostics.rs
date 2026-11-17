//! Read-only views of existing owners. Runtime only supplies lifecycle metadata
//! that the shared service cannot infer from device or subscription state.
use super::DeviceService;
use crate::{
    client_limit::ClientSlots,
    device_registry::Availability,
    recorder::{RecorderHandle, RecorderState},
};
use serde_json::{Value as Json, json};
use std::{net::SocketAddr, sync::Mutex, time::Instant};

pub(crate) struct Runtime {
    started: Instant,
    pub(crate) slots: ClientSlots,
    tcp: SocketAddr,
    websocket: Option<SocketAddr>,
    recorder: Option<RecorderHandle>,
    lifecycle: Mutex<Lifecycle>,
}
struct Lifecycle {
    state: &'static str,
    stopped_at: Option<Instant>,
    error: Option<String>,
    tcp_error: Option<String>,
    websocket_error: Option<String>,
}
impl Runtime {
    pub(crate) fn new(
        started: Instant,
        tcp: SocketAddr,
        websocket: Option<SocketAddr>,
        recorder: Option<RecorderHandle>,
    ) -> Self {
        Self {
            started,
            slots: ClientSlots::new(),
            tcp,
            websocket,
            recorder,
            lifecycle: Mutex::new(Lifecycle {
                state: "ready",
                stopped_at: None,
                error: None,
                tcp_error: None,
                websocket_error: None,
            }),
        }
    }
    pub(crate) fn transition(&self, state: &'static str, error: Option<String>) {
        let mut life = self.lifecycle.lock().unwrap();
        life.state = state;
        if state == "stopped" {
            life.stopped_at.get_or_insert_with(Instant::now);
        }
        if error.is_some() {
            life.error = error;
        }
    }
    pub(crate) fn listener_failed(&self, websocket: bool, error: String) {
        let mut life = self.lifecycle.lock().unwrap();
        if websocket {
            life.websocket_error = Some(error);
        } else {
            life.tcp_error = Some(error);
        }
    }
    fn server(&self) -> Json {
        let life = self.lifecycle.lock().unwrap();
        let listener = |address: Option<SocketAddr>, error: &Option<String>| {
            let state = if address.is_none() {
                "disabled"
            } else if error.is_some() {
                "failed"
            } else {
                match life.state {
                    "ready" => "bound",
                    "running" => "accepting",
                    "stopped" => "closed",
                    _ => "not_accepting",
                }
            };
            json!({"state":state,"address":address.map(|a| a.to_string()),"error":error})
        };
        json!({"state":life.state,"error":life.error,
            "uptime_ms":life.stopped_at.unwrap_or_else(Instant::now).duration_since(self.started).as_millis().min(u64::MAX as u128) as u64,
            "active_clients":self.slots.active(),
            "listeners":{"tcp":listener(Some(self.tcp), &life.tcp_error),"websocket":listener(self.websocket,&life.websocket_error)}})
    }
    fn recording(&self) -> Json {
        let Some(handle) = &self.recorder else {
            return json!({"enabled":false,"state":"disabled","session_id":null,"losses":null,"error":null});
        };
        let (state, error) = match handle.state() {
            RecorderState::Running => ("running", None),
            RecorderState::Closing => ("closing", None),
            RecorderState::Stopped => ("stopped", None),
            RecorderState::Failed(error) => ("failed", Some(error.to_string())),
        };
        let losses = handle.losses();
        json!({"enabled":true,"state":state,"session_id":handle.session_id(),"error":error,
            "losses":{"dropped_events":losses.dropped_events,
                "dropped_data_lost_events":losses.dropped_data_lost_events,
                "rejected_invalid_data":losses.rejected_invalid_data}})
    }
}

fn availability(value: Availability) -> (&'static str, Option<String>) {
    match value {
        Availability::Available => ("available", None),
        Availability::Unavailable { reason } => ("unavailable", Some(reason)),
        Availability::Stopped { reason } => ("stopped", Some(format!("{reason:?}"))),
    }
}

impl DeviceService {
    /// Independently sampled sources, no polling, queue draining or state changes.
    pub fn diagnostics(&self) -> Result<Json, String> {
        // Bound work and the reply below the existing 64 KiB output buffer.
        // A large registry fails explicitly, never returns a misleading partial list.
        let mut budget = 48 * 1024usize;
        let mut charge = |value: &Json| -> Result<(), String> {
            budget = budget
                .checked_sub(value.to_string().len() + 1)
                .ok_or("status_too_large")?;
            Ok(())
        };
        let server = self.runtime.get().map(Runtime::server);
        let recorder = self.runtime.get().map(Runtime::recording);
        charge(&json!([server, recorder]))?;
        let mut ports = Vec::new();
        for port in self.registry().ports() {
            let (state, reason) = availability(port.availability());
            let last_success = port
                .handle()
                .and_then(|h| h.last_success())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|d| u64::try_from(d.as_millis()).ok());
            let value = json!({"id":port.id(),"state":state,"reason":reason,
                "loss_epoch":port.loss_epoch(),"last_success_unix_ms":last_success});
            charge(&value)?;
            ports.push(value);
        }
        let mut devices = Vec::new();
        for device in self.registry().devices() {
            let (state, reason) = availability(
                self.registry()
                    .availability(&device.id)
                    .expect("registered port"),
            );
            let value = json!({"id":device.id,"port":device.port_id,"state":state,"reason":reason});
            charge(&value)?;
            devices.push(value);
        }
        let processing = self.processing.lock().unwrap().diagnostics();
        charge(&json!(processing))?;
        Ok(
            json!({"server":server,"ports":ports,"devices":devices,"processing":processing,"recorder":recorder}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_failure_is_visible_without_fabricating_a_health_flag() {
        let runtime = Runtime::new(
            Instant::now(),
            "127.0.0.1:7240".parse().unwrap(),
            Some("127.0.0.1:7241".parse().unwrap()),
            None,
        );
        assert_eq!(runtime.server()["listeners"]["tcp"]["state"], "bound");
        runtime.transition("running", None);
        runtime.listener_failed(true, "injected accept error".into());
        let status = runtime.server();
        assert_eq!(status["state"], "running");
        assert_eq!(status["listeners"]["tcp"]["state"], "accepting");
        assert_eq!(status["listeners"]["websocket"]["state"], "failed");
        assert_eq!(
            status["listeners"]["websocket"]["error"],
            "injected accept error"
        );
        assert!(status.get("healthy").is_none());
        runtime.transition("stopping", Some("injected accept error".into()));
        assert_eq!(
            runtime.server()["listeners"]["tcp"]["state"],
            "not_accepting"
        );
        runtime.transition("stopped", None);
        let stopped = runtime.server();
        assert_eq!(stopped["error"], "injected accept error");
        assert_eq!(stopped["listeners"]["tcp"]["state"], "closed");
        assert_eq!(stopped["uptime_ms"], runtime.server()["uptime_ms"]);
    }
}
