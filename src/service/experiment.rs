//! Snapshot under the service locks, stage on the client thread, then publish
//! under the same mutation boundary as the revision check.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde_json::{Value as Json, json};

use super::DeviceService;
use crate::audit::Operation;
use crate::config::{ExperimentDocument, StartupComponent};
use crate::session::{PendingRequests, SessionState};

struct Snapshot {
    target: PathBuf,
    revision: u64,
    document: ExperimentDocument,
}

struct Staged {
    target: PathBuf,
    revision: u64,
    temporary: tempfile::TempPath,
}

pub(crate) struct PendingSave {
    pub(crate) id: u64,
    snapshot: Snapshot,
    audit: Option<Operation>,
}

impl Snapshot {
    fn stage(self) -> io::Result<Staged> {
        let text = self.document.to_toml()?;
        let directory = self.target.parent().expect("experiment file has a parent");
        let mut file = tempfile::Builder::new()
            .prefix(".experiment-")
            .suffix(".tmp")
            .tempfile_in(directory)?;
        file.write_all(text.as_bytes())?;
        file.as_file().sync_all()?;
        Ok(Staged {
            target: self.target,
            revision: self.revision,
            temporary: file.into_temp_path(),
        })
    }
}

impl DeviceService {
    /// Called in an existing TCP/WS dispatch turn, while its gate is held.
    pub(super) fn defer_experiment_save(
        &self,
        id: u64,
        requests: &mut PendingRequests,
    ) -> Result<(), String> {
        requests
            .ensure_available(id)
            .map_err(|error| error.code().to_owned())?;
        if requests.experiment_save.is_some() {
            return Err("runtime_busy".into());
        }
        let audit = requests.audit.clone();
        let snapshot = self.capture_experiment(audit.as_ref())?;
        requests.experiment_save = Some(PendingSave {
            id,
            snapshot,
            audit,
        });
        Ok(())
    }

    /// Called after the transport releases its gate. The client's existing
    /// thread owns the work and queues the answer on the same session.
    pub(crate) fn finish_experiment_save<Input>(
        &self,
        session: &mut SessionState<Input>,
    ) -> io::Result<()> {
        if let Some(save) = session.pending.experiment_save.take() {
            let result = self.save_snapshot(save.snapshot, save.audit.as_ref());
            session.component_response(save.id, result)?;
        }
        Ok(())
    }

    pub(super) fn save_experiment(&self, audit: Option<&Operation>) -> Result<Json, String> {
        let snapshot = {
            let _gate = self.lock_service();
            self.capture_experiment(audit)?
        };
        self.save_snapshot(snapshot, audit)
    }

    fn capture_experiment(&self, audit: Option<&Operation>) -> Result<Snapshot, String> {
        let engine = self.processing.lock().expect("processing lock poisoned");
        let revision = engine.revision;
        let Some(target) = self.experiment_file.clone() else {
            if let Some(audit) = audit {
                audit.event(
                    "rejected",
                    json!({"revision":revision,"error":"no_experiment_file"}),
                );
            }
            return Err("no_experiment_file".into());
        };
        let controls = self.controls.lock().unwrap();
        let components = engine
            .configurations()
            .map(|(configuration, incarnation)| StartupComponent {
                binding: controls
                    .get(&configuration.name)
                    .and_then(|control| control.binding(incarnation)),
                configuration: configuration.clone(),
            })
            .collect();
        if let Some(audit) = audit {
            audit.accepted();
        }
        Ok(Snapshot {
            target,
            revision,
            document: ExperimentDocument::new(components),
        })
    }

    fn save_snapshot(&self, snapshot: Snapshot, audit: Option<&Operation>) -> Result<Json, String> {
        let revision = snapshot.revision;
        let target = snapshot.target.clone();
        let result = snapshot
            .stage()
            .map_err(|error| write_failed(&target, &error))
            .and_then(|staged| self.publish_experiment(staged));
        if let Some(audit) = audit {
            let details = match &result {
                Ok(()) => json!({"revision":revision,"status":"ok"}),
                Err(error) => json!({"revision":revision,"status":"error","error":error}),
            };
            audit.event("save_result", details);
        }
        result.map(|()| json!({"revision":revision}))
    }

    fn publish_experiment(&self, staged: Staged) -> Result<(), String> {
        let publication = {
            let _gate = self.lock_service();
            // Also excludes direct management callers that already serialize
            // through the Engine mutex. Cleanup happens after both guards drop.
            let engine = self.processing.lock().expect("processing lock poisoned");
            if engine.revision != staged.revision {
                return Err("revision_conflict".into());
            }
            staged.temporary.persist(&staged.target)
        };
        publication.map_err(|error| write_failed(&staged.target, &error.error))
    }
}

fn write_failed(target: &Path, error: &io::Error) -> String {
    eprintln!("Experiment save failed for {}: {error}", target.display());
    "experiment_write_failed".into()
}

#[cfg(test)]
mod tests;
