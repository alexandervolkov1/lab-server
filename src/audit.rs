//! Bounded audit facts. An operation owns a causal sequence, independent of wall time.
use crate::recorder::RecorderHandle;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

static CONNECTION: AtomicU64 = AtomicU64::new(1);
pub(crate) fn connection_id() -> u64 {
    CONNECTION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .expect("connection id exhausted")
}

#[derive(Clone)]
pub struct Operation(Arc<Inner>);
struct Inner {
    recorder: RecorderHandle,
    id: u64,
    context: Arc<str>,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    sequence: u64,
    admitted: bool,
    decided: bool,
    cancelled: bool,
}
#[derive(Clone)]
pub(crate) struct Event {
    pub operation: u64,
    pub sequence: u64,
    pub time_ns: i64,
    pub kind: &'static str,
    pub context: Arc<str>,
    pub details: String,
}
impl Operation {
    pub fn new(recorder: &RecorderHandle, context: Value) -> Self {
        Self(Arc::new(Inner {
            recorder: recorder.clone(),
            id: recorder.next_operation(),
            context: context.to_string().into(),
            state: Mutex::new(State::default()),
        }))
    }
    fn emit_locked(&self, state: &mut State, kind: &'static str, details: Value) {
        state.sequence += 1;
        let Ok(time_ns) = super::recorder::unix_ns(std::time::SystemTime::now()) else {
            self.0.recorder.audit_lost();
            return;
        };
        self.0.recorder.audit_event(Event {
            operation: self.0.id,
            sequence: state.sequence,
            time_ns,
            kind,
            context: self.0.context.clone(),
            details: details.to_string(),
        });
    }
    pub fn event(&self, kind: &'static str, details: Value) {
        let mut s = self.0.state.lock().unwrap();
        if kind == "rejected" {
            s.decided = true;
        }
        self.emit_locked(&mut s, kind, details);
    }
    pub fn accepted(&self) {
        let mut s = self.0.state.lock().unwrap();
        if !s.admitted {
            s.admitted = true;
            s.decided = true;
            self.emit_locked(&mut s, "accepted", json!({}));
        }
    }
    pub fn cancelled(&self, reason: &str) {
        let mut s = self.0.state.lock().unwrap();
        if !s.cancelled {
            s.cancelled = true;
            self.emit_locked(&mut s, "cancelled", json!({"reason":reason}));
        }
    }
    pub fn response(&self, response: Value) {
        let mut s = self.0.state.lock().unwrap();
        if !s.decided {
            self.emit_locked(&mut s, "rejected", response.clone());
        }
        self.emit_locked(&mut s, "response", response);
    }
}
pub(crate) fn mutation(v: &Value) -> bool {
    matches!(v["op"].as_str(), Some("set" | "invoke_action"))
        || (v["op"] == "components"
            && !matches!(
                v["command"]["action"].as_str(),
                Some("types" | "list" | "outputs")
            ))
}

pub(crate) fn value(v: &crate::model::Value) -> Value {
    match v {
        crate::model::Value::Float(v) => json!(v),
        crate::model::Value::Integer(v) => json!(v),
        crate::model::Value::Boolean(v) => json!(v),
    }
}
