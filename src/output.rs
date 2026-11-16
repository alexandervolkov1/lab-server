//! Physical-output authority, independent of any controller algorithm.
use crate::model::{Spec, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
pub type Owner = (String, u64);
#[derive(Default)]
struct State {
    generation: u64,
    owner: Option<Owner>,
    active: usize,
    unknown: bool,
}
#[derive(Default)]
pub struct Authority {
    issuer: Arc<()>,
    slots: BTreeMap<String, Arc<Mutex<State>>>,
}
impl Authority {
    pub fn new(keys: impl Iterator<Item = String>) -> Self {
        Self {
            issuer: Arc::new(()),
            slots: keys
                .map(|k| (k, Arc::new(Mutex::new(State::default()))))
                .collect(),
        }
    }
    pub fn acquire(&self, key: &str, owner: Owner) -> Result<(), &'static str> {
        let mut s = self.slots.get(key).ok_or("unknown_output")?.lock().unwrap();
        if s.unknown {
            return Err("output_fault");
        }
        if s.active > 0 {
            return Err("output_busy");
        }
        if s.owner.is_some() {
            return Err("output_owned");
        }
        s.generation = s.generation.checked_add(1).ok_or("generation_exhausted")?;
        s.owner = Some(owner);
        Ok(())
    }
    pub fn release(&self, owner: &Owner) {
        for slot in self.slots.values() {
            let mut s = slot.lock().unwrap();
            if s.owner.as_ref() == Some(owner) {
                s.owner = None;
                if let Some(g) = s.generation.checked_add(1) {
                    s.generation = g;
                } else {
                    s.unknown = true;
                }
            }
        }
    }
    pub fn release_all(&self) {
        for slot in self.slots.values() {
            let mut s = slot.lock().unwrap();
            if s.owner.take().is_none() {
                continue;
            }
            if let Some(g) = s.generation.checked_add(1) {
                s.generation = g;
            } else {
                s.unknown = true;
            }
        }
    }
    pub fn permit(&self, keys: &[String], owner: Option<&Owner>) -> Result<Permit, &'static str> {
        let mut leases = Vec::new();
        for key in keys.iter().collect::<std::collections::BTreeSet<_>>() {
            let slot = self.slots.get(key).ok_or("unknown_output")?;
            let s = slot.lock().unwrap();
            if s.unknown && owner.is_some() {
                return Err("output_fault");
            }

            if s.owner.as_ref() != owner {
                return Err("output_owned");
            }
            leases.push((slot.clone(), s.generation));
        }
        Ok(Permit {
            issuer: self.issuer.clone(),
            keys: keys.iter().cloned().collect(),
            leases,
            started: false,
            finished: false,
            sources: Vec::new(),
            manual: owner.is_none(),
        })
    }
    pub(crate) fn contains(&self, key: &str) -> bool {
        self.slots.contains_key(key)
    }
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!(self.slots.iter().map(|(key,slot)|{let s=slot.lock().unwrap();serde_json::json!({"key":key,"mode":if s.owner.is_some(){"auto"}else{"manual"},"owner":s.owner,"generation":s.generation,"in_flight":s.active,"outcome_unknown":s.unknown})}).collect::<Vec<_>>())
    }
}
pub struct Permit {
    issuer: Arc<()>,
    keys: std::collections::BTreeSet<String>,
    manual: bool,
    sources: Vec<(crate::port_worker::PortHandle, u64)>,
    leases: Vec<(Arc<Mutex<State>>, u64)>,
    started: bool,
    finished: bool,
}
impl Permit {
    /// Exact immutable resource scope and issuer, independent of the current owner.
    pub(crate) fn covers(&self, authority: &Authority, keys: &[String]) -> bool {
        Arc::ptr_eq(&self.issuer, &authority.issuer) && self.keys == keys.iter().cloned().collect()
    }
    pub(crate) fn with_sources(
        mut self,
        sources: Vec<(crate::port_worker::PortHandle, u64)>,
    ) -> Self {
        self.sources = sources;
        self
    }
    pub(crate) fn start(&mut self) -> bool {
        let _boundaries = self
            .sources
            .iter()
            .map(|(h, _)| h.observation_boundary())
            .collect::<Vec<_>>();
        if self
            .sources
            .iter()
            .any(|(h, g)| h.loss_epoch() != *g || *g == u64::MAX || !h.source_alive())
        {
            return false;
        }
        let mut states = self
            .leases
            .iter()
            .map(|(s, _)| s.lock().unwrap())
            .collect::<Vec<_>>();
        if states
            .iter()
            .zip(&self.leases)
            .any(|(s, (_, g))| s.generation != *g || (s.unknown && !self.manual) || s.active > 0)
        {
            return false;
        }
        for s in &mut states {
            s.active += 1;
        }
        self.started = true;
        true
    }
    pub(crate) fn unknown(&self) {
        if self.started {
            for (slot, _) in &self.leases {
                slot.lock().unwrap().unknown = true;
            }
        }
    }
    pub(crate) fn finish(&mut self) {
        if self.started && !self.finished {
            for (slot, _) in &self.leases {
                slot.lock().unwrap().active -= 1;
            }
            self.finished = true;
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if self.started && !self.finished {
            self.unknown();
            self.finish();
        }
    }
}
/// Reject before conversion; Arduino power is integer percent, never a float set.
pub fn convert(spec: &Spec, value: f64) -> Result<Value, &'static str> {
    if !value.is_finite() {
        return Err("invalid_output");
    }
    let converted = match spec {
        Spec::Float { .. } => Value::Float(value),
        Spec::Integer { min, max } => {
            // Avoid i64 boundary rounding and saturating casts.
            if value < *min as f64
                || value > *max as f64
                || value < i64::MIN as f64
                || value >= -(i64::MIN as f64)
            {
                return Err("invalid_output");
            }
            Value::Integer(value.round() as i64)
        }
        Spec::Boolean => return Err("invalid_output"),
    };
    if spec.accepts(&converted) {
        Ok(converted)
    } else {
        Err("invalid_output")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pending::{Completion, Pending, Poll, SetExecution};
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };
    #[test]
    fn concurrent_revoke_and_driver_start_have_one_authority_boundary() {
        for _ in 0..64 {
            let a = Arc::new(Authority::new(["p".into()].into_iter()));
            let owner = ("c".into(), 1);
            a.acquire("p", owner.clone()).unwrap();
            let execution = SetExecution::authorized(
                Instant::now() + Duration::from_secs(5),
                a.permit(&["p".into()], Some(&owner)).unwrap(),
            );
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let b = barrier.clone();
            let e = execution.clone();
            let thread = std::thread::spawn(move || {
                b.wait();
                e.try_start()
            });
            barrier.wait();
            a.release(&owner);
            let started = thread.join().unwrap();
            if started {
                assert!(a.acquire("p", ("new".into(), 2)).is_err());
                execution.complete(|| {});
            }
            a.acquire("p", ("new".into(), 2)).unwrap();
            assert!(!execution.try_start());
        }
    }
    #[test]
    fn queued_manual_and_controller_generations_cannot_cross_handover() {
        let a = Authority::new(["a/power".into()].into_iter());
        let keys: Vec<String> = vec!["a/power".into()];
        let deadline = Instant::now() + Duration::from_secs(10);
        let manual = SetExecution::authorized(deadline, a.permit(&keys, None).unwrap());
        let owner = ("loop".into(), 1);
        a.acquire(&keys[0], owner.clone()).unwrap();
        assert!(!manual.try_start());
        assert!(a.permit(&keys, None).is_err());
        let queued = SetExecution::authorized(deadline, a.permit(&keys, Some(&owner)).unwrap());
        a.release(&owner);
        assert!(!queued.try_start());
        let manual = SetExecution::authorized(deadline, a.permit(&keys, None).unwrap());
        assert!(manual.try_start());
        assert!(a.acquire(&keys[0], owner.clone()).is_err());
        manual.complete(|| {});
        a.acquire(&keys[0], owner).unwrap();
    }
    #[test]
    fn timeout_latches_auto_even_after_late_success_and_explicit_manual_is_not_a_retry() {
        let a = Authority::new(["a/power".into()].into_iter());
        let keys: Vec<String> = vec!["a/power".into()];
        let owner = ("loop".into(), 1);
        a.acquire(&keys[0], owner.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let execution = SetExecution::authorized(deadline, a.permit(&keys, Some(&owner)).unwrap());
        let (tx, rx) = mpsc::sync_channel(1);
        let p = Pending::<()>::new(1, deadline, rx).with_execution(execution.clone());
        assert!(execution.try_start());
        assert!(matches!(
            p.poll(deadline),
            Poll::Finished(Completion::OutcomeUnknown { .. })
        ));
        a.release(&owner);
        assert!(a.acquire(&keys[0], ("new".into(), 2)).is_err());
        execution.complete(|| {
            let _ = tx.send(());
        });
        assert!(a.acquire(&keys[0], ("new".into(), 2)).is_err());
        let manual = SetExecution::authorized(deadline, a.permit(&keys, None).unwrap());
        assert!(manual.try_start());
        manual.complete(|| {});
    }
    #[test]
    fn integer_conversion_checks_before_rounding() {
        let spec = Spec::Integer { min: 0, max: 100 };
        assert_eq!(convert(&spec, 2.5), Ok(Value::Integer(3)));
        for bad in [-0.1, 100.1, f64::NAN, f64::INFINITY] {
            assert!(convert(&spec, bad).is_err());
        }
        assert!(
            convert(
                &Spec::Integer {
                    min: i64::MIN,
                    max: i64::MAX
                },
                i64::MAX as f64
            )
            .is_err()
        );
    }
}
