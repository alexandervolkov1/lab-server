//! Physical output bindings, ownership and pending controller writes.

use std::time::{Duration, Instant};

use super::{DeviceService, GateOperation};
use crate::config::StartupBinding;
use crate::pending::{Completion, Pending, Poll};
use crate::port_worker::{CommandError, StopReason};
use crate::processing::{Engine, State};

pub(super) struct Control {
    audit: Option<crate::audit::Operation>,
    owner: crate::output::Owner,
    target: String,
    timeout: Duration,
    pending: Option<Pending<Result<(), CommandError>>>,
}

impl Control {
    pub(super) fn binding(&self, incarnation: u64) -> Option<StartupBinding> {
        (self.owner.1 == incarnation).then(|| StartupBinding {
            target: self.target.clone(),
            command_timeout_ms: self.timeout.as_millis() as u64,
        })
    }
}

impl DeviceService {
    /// Permanently close mutation admission while leaving reads and completions available.
    pub fn stop_controls(&self) {
        let _gate = self.lock_service(GateOperation::StopControls);
        // Serialize the transition with direct CommandBackend management calls too.
        let mut engine = self.processing.lock().unwrap();
        let authority = self.router.authority();
        if !authority.close_admission() {
            return;
        }
        authority.release_all();
        for name in self.controls.lock().unwrap().keys() {
            engine.fault(name, "shutdown");
        }
    }

    pub(super) fn reconcile_controls(&self, engine: &mut Engine) -> bool {
        let mut controls = self.controls.lock().unwrap();
        let mut invalidated = false;
        for (name, c) in controls.iter_mut() {
            if let Some(pending) = c.pending.take() {
                match pending.poll(Instant::now()) {
                    Poll::Waiting(p) => c.pending = Some(p),
                    Poll::Finished(Completion::Received { value: Ok(()), .. }) => {
                        if let Some(a) = c.audit.take() {
                            a.event("controller_result", serde_json::json!({"status":"ok"}));
                        }
                    }
                    Poll::Finished(result) => {
                        let state = engine.state(name).filter(|(_, inc)| *inc == c.owner.1);
                        // These results are only possible before a physical call. Once
                        // Auto has been deliberately stopped, its queued write can no
                        // longer fault the controller just because it failed to start.
                        let ended_before_start = matches!(
                            result,
                            Completion::Timeout { .. }
                                | Completion::Received {
                                    value: Err(CommandError::RuntimeTimeout
                                        | CommandError::PortStopped(StopReason::Shutdown)),
                                    ..
                                }
                        );
                        let expected_cancellation = ended_before_start
                            && (matches!(state, Some((State::Stopped | State::Shadow, _)))
                                || self.router.authority().ensure_admission().is_err());
                        if let Some(a) = c.audit.take() {
                            let error = match result {
                                Completion::Received {
                                    value: Err(error), ..
                                } => error.to_string(),
                                Completion::Timeout { .. } => "runtime_timeout".into(),
                                Completion::Disconnected { .. } => "runtime_unavailable".into(),
                                Completion::OutcomeUnknown { .. } => "outcome_unknown".into(),
                                _ => unreachable!("success handled above"),
                            };
                            let status = if expected_cancellation {
                                "cancelled"
                            } else {
                                "error"
                            };
                            a.event(
                                "controller_result",
                                serde_json::json!({"status": status, "error": error}),
                            );
                        }

                        if state.is_some() && !expected_cancellation {
                            engine.fault(name, "output_failed");
                            invalidated = true;
                        }
                        self.router.authority().release(&c.owner);
                    }
                }
            }
            if engine.state(name) != Some((State::Auto, c.owner.1)) {
                self.router.authority().release(&c.owner);
            }
        }
        controls.retain(|name, c| {
            c.pending.is_some() || engine.state(name).is_some_and(|(_, inc)| inc == c.owner.1)
        });
        invalidated
    }

    /// An error means the submission faulted the controller and invalidated its outputs.
    pub(super) fn submit_proposal(
        &self,
        name: &str,
        incarnation: u64,
        value: f64,
    ) -> Result<(), String> {
        let mut engine = self.processing.lock().unwrap();
        if engine.state(name) != Some((State::Auto, incarnation)) {
            return Ok(());
        }
        let mut controls = self.controls.lock().unwrap();
        let Some(c) = controls.get_mut(name) else {
            return Ok(());
        };
        if c.owner.1 != incarnation || c.pending.is_some() {
            return Ok(());
        }
        let audit = self.audit.get().map(|recorder| {
            crate::audit::Operation::new(
                recorder,
                serde_json::json!({
                    "source": "controller", "component": name,
                    "incarnation": incarnation.to_string(),
                    "request": {"op": "set", "key": c.target, "proposal": value}
                }),
            )
        });
        let result = (|| {
            let deadline = engine
                .proposal_deadline(name)
                .ok_or("input_not_ready".to_owned())?
                .min(Instant::now() + c.timeout);
            let description = self
                .router
                .description(&c.target)
                .map_err(|e| e.to_string())?;
            let value = crate::output::convert(&description.description.spec, value)
                .map_err(str::to_owned)?;
            let physical = Self::physical_lineage(self.registry());
            let lineage = engine.lineage(&physical);
            let prefix = format!("processing/{name}/");
            let ports = lineage
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .flat_map(|(_, p)| p.iter().cloned())
                .collect::<std::collections::BTreeSet<_>>();
            let continuity = self.continuity.lock().unwrap();
            let sources = ports
                .iter()
                .filter_map(|p| {
                    self.registry()
                        .port(p)?
                        .handle()
                        .map(|h| (h.clone(), continuity[p].generation()))
                })
                .collect();
            if let Some(a) = &audit {
                a.event(
                    "requested",
                    serde_json::json!({"value":crate::audit::value(&value)}),
                );
            }
            self.router
                .with_operation(audit.clone())
                .set_control(
                    incarnation,
                    &c.target,
                    value,
                    deadline,
                    Some(&c.owner),
                    sources,
                )
                .map_err(|e| e.to_string())
        })();
        match result {
            Ok(p) => {
                c.pending = Some(p);
                c.audit = audit;
                Ok(())
            }
            Err(reason) => {
                if let Some(a) = audit {
                    a.event("rejected", serde_json::json!({"error":reason}));
                }
                engine.fault(name, &reason);
                self.router.authority().release(&c.owner);
                Err(reason)
            }
        }
    }

    pub(super) fn bind_control(
        &self,
        engine: &mut Engine,
        name: String,
        target: String,
        command_timeout_ms: u64,
        expected_revision: u64,
    ) -> Result<(), String> {
        if command_timeout_ms == 0 || command_timeout_ms > 10000 {
            return Err("invalid_timeout".into());
        }
        let (state, incarnation) = engine.state(&name).ok_or("unknown_instance")?;
        if state != State::Shadow {
            return Err("stop_in_shadow_before_bind".into());
        }
        let p = self
            .router
            .description(&target)
            .map_err(|e| e.to_string())?;
        if !p.description.writable || matches!(p.description.spec, crate::model::Spec::Boolean) {
            return Err("invalid_output_target".into());
        }
        let mut controls = self.controls.lock().unwrap();
        if controls.get(&name).is_some_and(|c| c.pending.is_some()) {
            return Err("output_busy".into());
        }
        engine.touch(expected_revision)?;
        controls.insert(
            name.clone(),
            Control {
                audit: None,
                owner: (name, incarnation),
                target,
                timeout: Duration::from_millis(command_timeout_ms),
                pending: None,
            },
        );
        Ok(())
    }

    pub(super) fn acquire_control(
        &self,
        engine: &Engine,
        name: &str,
        expected_revision: u64,
    ) -> Result<(), String> {
        let incarnation = engine.ready_for_auto(name, expected_revision)?;
        let controls = self.controls.lock().unwrap();
        let c = controls
            .get(name)
            .filter(|c| c.owner.1 == incarnation)
            .ok_or("output_not_bound")?;
        if c.pending.is_some() {
            return Err("output_busy".into());
        }
        self.router
            .authority()
            .acquire(&c.target, c.owner.clone())?;
        Ok(())
    }

    pub(super) fn component_page(
        &self,
        engine: &Engine,
        offset: usize,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let controls = self.controls.lock().unwrap();
        engine.page_with(offset, limit, |name, incarnation, instance| {
            instance["binding"] =
                serde_json::json!(controls.get(name).and_then(|c| c.binding(incarnation)));
        })
    }
}
