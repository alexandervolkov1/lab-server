//! Transport-neutral command, subscription and completion state machine.

use std::collections::HashMap;
use std::io;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::model::{ActionDescription, Observation, ParameterDescription, Value};
use crate::outbound::{OutboundBuffer, is_output_error};
mod requests;
mod subscriptions;
use crate::protocol::{ParseError, Request, parse_request};
use requests::completion_json;
pub use requests::{FinishedRequest, PendingRequests};
pub use subscriptions::{SubscriptionChange, SubscriptionError, SubscriptionSet};

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError {
    DuplicateId,
    Full,
    RuntimeUnavailable,
    RuntimeBusy,
    UnknownKey,
    ReadOnly,
    InvalidValue,
}

impl EnqueueError {
    fn code(&self) -> &'static str {
        match self {
            Self::DuplicateId => "duplicate_id",
            Self::Full => "too_many_pending",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::RuntimeBusy => "runtime_busy",
            Self::UnknownKey => "unknown_key",
            Self::ReadOnly => "Parameter is read-only",
            Self::InvalidValue => "Value does not match parameter spec",
        }
    }
}

/// Shared asynchronous command admission for transport sessions.
/// Implementations only admit bounded asynchronous requests; no physical I/O.
pub trait CommandBackend {
    /// Global action names for describe; backends may expose no actions.
    fn actions(&self) -> Vec<ActionDescription> {
        Vec::new()
    }
    fn enqueue_action(
        &self,
        _pending: &mut PendingRequests,
        _id: u64,
        _deadline: Instant,
        _key: String,
    ) -> Result<(), EnqueueError> {
        Err(EnqueueError::UnknownKey)
    }
    fn enqueue_read(
        &self,
        pending: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        description: ParameterDescription,
    ) -> Result<(), EnqueueError>;
    fn enqueue_set(
        &self,
        pending: &mut PendingRequests,
        id: u64,
        deadline: Instant,
        key: String,
        value: Value,
    ) -> Result<(), EnqueueError>;
    fn available(&self, _key: &str) -> bool {
        true
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ParsedMessage {
    Request(Request),
    InvalidRequest { frame: String, error: ParseError },
}

impl ParsedMessage {
    pub(crate) fn parse(text: &str) -> Self {
        match parse_request(text) {
            Ok(request) => Self::Request(request),
            Err(error) => Self::InvalidRequest {
                frame: text.to_owned(),
                error,
            },
        }
    }
}

/// Transport-neutral session for complete JSON messages.
pub type Session = SessionState<()>;

/// Shared state machine. `Input` is only a compatibility slot for a transport
/// adapter; the application methods never inspect it. Use [`Session`] for
/// complete JSON texts, without allocating any NDJSON decoder.
pub struct SessionState<Input> {
    pub(crate) input: Input,
    input_closed: bool,
    pub pending: PendingRequests,
    pub outbound: OutboundBuffer,
    pub subscriptions: SubscriptionSet,
    pub(crate) terminal: bool,
    last_request_id: u64,
    observation_clock: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    observation_interest: Option<std::sync::Arc<crate::fanout::ObservationInterest>>,
    activated_after: HashMap<String, u64>,
}

impl<Input: Default> SessionState<Input> {
    pub fn new(max_pending: usize, max_output_bytes: usize) -> Self {
        Self {
            input: Input::default(),
            input_closed: false,
            pending: PendingRequests::new(max_pending),
            outbound: OutboundBuffer::new(max_output_bytes),
            subscriptions: SubscriptionSet::default(),
            terminal: false,
            last_request_id: 0,
            observation_clock: None,
            observation_interest: None,
            activated_after: HashMap::new(),
        }
    }
}

impl<Input> SessionState<Input> {
    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    /// Handle exactly one complete JSON text using the same parser and dispatcher
    /// as TCP. Whitespace inside the text is JSON whitespace, not framing.
    /// The transport must bound incoming messages and stop admission after EOF.
    pub fn receive_message(
        &mut self,
        text: &str,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        now: Instant,
    ) -> io::Result<()> {
        if self.terminal || self.input_closed {
            return Ok(());
        }
        self.dispatch_message(ParsedMessage::parse(text), commands, catalog, latest, now)?;
        Ok(())
    }

    /// Take one complete JSON response/event, without transport delimiters.
    /// A transport must keep its own in-flight sends bounded, and must not mix
    /// this interface with a partially completed NDJSON write.
    pub fn next_message(&mut self) -> io::Result<Option<String>> {
        self.outbound.pop_message()
    }

    /// Clean end of input: finish already admitted commands, but stop streaming.
    /// Dropping a session has the same Pending/SetExecution semantics as TCP;
    /// neither a disconnect nor EOF asserts that a started write did not happen.
    pub fn close_input(&mut self) {
        self.input_closed = true;
    }

    pub fn is_finished(&self) -> bool {
        (self.terminal || self.input_closed && self.pending.is_empty()) && self.outbound.is_empty()
    }

    pub(crate) fn queue_response(&mut self, response: &str) -> io::Result<()> {
        if let Err(error) = self.outbound.enqueue_message(response) {
            self.terminal = true;

            return Err(io::Error::other(error));
        }

        Ok(())
    }

    fn queue_error(&mut self, id: u64, error: &str) -> io::Result<()> {
        let mut response = json!({
            "id": id,
            "status": "error",
            "error": error
        });

        if error == "duplicate_id" {
            response.as_object_mut().expect("error object").remove("id");
        }

        self.queue_response(&response.to_string())
    }

    pub fn dispatch_set(
        &mut self,
        id: u64,
        key: String,
        value: Value,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        now: Instant,
    ) -> io::Result<()> {
        let Some(description) = catalog.get(&key) else {
            return self.queue_error(id, "unknown_key");
        };
        if !description.writable {
            return self.queue_error(id, "Parameter is read-only");
        }
        if !description.spec.accepts(&value) {
            return self.queue_error(id, "Value does not match parameter spec");
        }

        let result = commands.enqueue_set(
            &mut self.pending,
            id,
            now + Duration::from_secs(10),
            key,
            value,
        );

        match result {
            Ok(()) => Ok(()),

            Err(error) => self.queue_error(id, error.code()),
        }
    }

    pub fn dispatch_get(
        &mut self,
        id: u64,
        key: String,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        now: Instant,
    ) -> io::Result<()> {
        if self.pending.contains_id(id) {
            return self.queue_error(id, "duplicate_id");
        }

        let Some(description) = catalog.get(&key) else {
            return self.queue_error(id, "unknown_key");
        };

        if description.default_poll_period.is_some() {
            let response = {
                let guard = latest
                    .lock()
                    .map_err(|_| io::Error::other("observation lock poisoned"))?;

                if !commands.available(&key) {
                    return self.queue_error(id, "runtime_unavailable");
                }

                match guard.get(&key) {
                    Some(observation) => crate::observation_json(Some(id), observation)?,

                    None => json!({
                        "id": id,
                        "status": "no_data",
                        "key": key
                    })
                    .to_string(),
                }
            };

            return self.queue_response(&response);
        }

        let description = description.clone();

        let result = commands.enqueue_read(
            &mut self.pending,
            id,
            now + Duration::from_secs(10),
            description,
        );

        match result {
            Ok(()) => Ok(()),

            Err(error) => self.queue_error(id, error.code()),
        }
    }

    pub fn dispatch_describe(
        &mut self,
        id: u64,
        catalog: &HashMap<String, ParameterDescription>,
    ) -> io::Result<()> {
        self.describe_with_actions(id, catalog, Vec::new())
    }

    fn describe_with_actions(
        &mut self,
        id: u64,
        catalog: &HashMap<String, ParameterDescription>,
        mut actions: Vec<ActionDescription>,
    ) -> io::Result<()> {
        if self.pending.contains_id(id) {
            return self.queue_error(id, "duplicate_id");
        }

        let mut descriptions: Vec<_> = catalog.values().collect();
        descriptions.sort_unstable_by(|left, right| left.id.cmp(&right.id));

        let parameters: Vec<_> = descriptions
            .into_iter()
            .map(|description| {
                let (kind, min, max, unit) = match &description.spec {
                    crate::model::Spec::Float { min, max } => {
                        ("float", json!(min), json!(max), description.unit.as_str())
                    }
                    crate::model::Spec::Integer { min, max } => {
                        ("integer", json!(min), json!(max), description.unit.as_str())
                    }
                    crate::model::Spec::Boolean => ("boolean", json!(null), json!(null), ""),
                };

                json!({
                    "key": &description.id,
                    "type": kind,
                    "min": min,
                    "max": max,
                    "unit": unit,
                    "writable": description.writable,
                    "poll_period_ns": description
                        .default_poll_period
                        .map(|period| period.as_nanos())
                })
            })
            .collect();

        actions.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let actions: Vec<_> = actions
            .into_iter()
            .map(|action| json!({ "key": action.name, "description": action.description }))
            .collect();

        self.queue_response(
            &json!({
                "id": id,
                "status": "ok",
                "parameters": parameters,
                "actions": actions
            })
            .to_string(),
        )
    }

    pub fn dispatch_subscription(
        &mut self,
        id: u64,
        change: SubscriptionChange,
        keys: Vec<String>,
        catalog: &HashMap<String, ParameterDescription>,
    ) -> io::Result<()> {
        if self.pending.contains_id(id) {
            return self.queue_error(id, "duplicate_id");
        }

        let response = match self.subscriptions.apply(change, &keys, catalog) {
            Ok(keys) => {
                if let Some(interest) = &self.observation_interest {
                    interest.set_keys(&keys);
                }
                let boundary = self
                    .observation_clock
                    .as_ref()
                    .map_or(0, |clock| clock.load(std::sync::atomic::Ordering::SeqCst));
                self.activated_after
                    .retain(|key, _| self.subscriptions.contains(key));
                for key in &keys {
                    self.activated_after.entry(key.clone()).or_insert(boundary);
                }
                json!({ "id": id, "status": "ok", "keys": keys })
            }

            Err(SubscriptionError::UnknownKey(key)) => json!({
                "id": id,
                "status": "error",
                "error": "unknown_key",
                "key": key
            }),

            Err(SubscriptionError::NotSubscribable(key)) => json!({
                "id": id,
                "status": "error",
                "error": "not_subscribable",
                "key": key
            }),

            Err(SubscriptionError::Full) => json!({
                "id": id,
                "status": "error",
                "error": "too_many_subscriptions"
            }),
        };

        self.queue_response(&response.to_string())
    }

    pub(crate) fn dispatch_message(
        &mut self,
        incoming: ParsedMessage,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        now: Instant,
    ) -> io::Result<()> {
        let id = match &incoming {
            ParsedMessage::Request(
                Request::Hello { id }
                | Request::Describe { id }
                | Request::Ping { id }
                | Request::Get { id, .. }
                | Request::Set { id, .. }
                | Request::InvokeAction { id, .. }
                | Request::Subscribe { id, .. }
                | Request::Unsubscribe { id, .. },
            ) => Some(*id),
            ParsedMessage::InvalidRequest { frame, .. } => {
                crate::protocol::parse_json_object(frame)
                    .ok()
                    .and_then(|object| object.get("id").and_then(serde_json::Value::as_u64))
                    .filter(|id| *id > 0)
            }
        };
        if let Some(id) = id {
            if id <= self.last_request_id || self.pending.contains_id(id) {
                self.queue_response(
                    &json!({"status":"error", "error":"duplicate_id"}).to_string(),
                )?;
                return Ok(());
            }
            self.last_request_id = id;
        }

        match incoming {
            ParsedMessage::Request(Request::Subscribe { id, keys }) => {
                self.dispatch_subscription(id, SubscriptionChange::Subscribe, keys, catalog)?;
                Ok(())
            }

            ParsedMessage::Request(Request::Unsubscribe { id, keys }) => {
                self.dispatch_subscription(id, SubscriptionChange::Unsubscribe, keys, catalog)?;
                Ok(())
            }

            ParsedMessage::Request(Request::Describe { id }) => {
                self.describe_with_actions(id, catalog, commands.actions())?;
                Ok(())
            }

            ParsedMessage::Request(Request::Hello { id }) => {
                self.queue_response(
                    &json!({
                        "id": id,
                        "status": "ok",
                        "protocol": "lab-server",
                        "version": 1
                    })
                    .to_string(),
                )?;

                Ok(())
            }

            ParsedMessage::Request(Request::Ping { id }) => {
                self.queue_response(
                    &json!({
                        "id": id,
                        "status": "ok"
                    })
                    .to_string(),
                )?;

                Ok(())
            }

            ParsedMessage::Request(Request::Get { id, key }) => {
                self.dispatch_get(id, key, commands, catalog, latest, now)?;
                Ok(())
            }

            ParsedMessage::Request(Request::Set { id, key, value }) => {
                self.dispatch_set(id, key, value, commands, catalog, now)?;
                Ok(())
            }

            ParsedMessage::Request(Request::InvokeAction { id, key }) => {
                match commands.enqueue_action(
                    &mut self.pending,
                    id,
                    now + Duration::from_secs(10),
                    key,
                ) {
                    Ok(()) => Ok(()),
                    Err(error) => self.queue_error(id, error.code()),
                }
            }

            ParsedMessage::InvalidRequest { frame, error } => {
                use crate::protocol::{JsonError, RequestError};

                let code = match error {
                    ParseError::Json(JsonError::InvalidJson) => "invalid_json",
                    ParseError::Json(JsonError::ExpectedObject) => "expected_object",
                    ParseError::Json(JsonError::DuplicateKey) => "duplicate_key",

                    ParseError::Request(RequestError::InvalidId) => "invalid_id",
                    ParseError::Request(RequestError::UnsupportedOp) => "unsupported_op",
                    ParseError::Request(RequestError::InvalidKey) => "invalid_key",
                    ParseError::Request(RequestError::InvalidValue) => "invalid_value",
                    ParseError::Request(RequestError::InvalidKeys) => "invalid_keys",
                };

                let mut response = json!({
                    "status": "error",
                    "error": code
                });

                if let Ok(object) = crate::protocol::parse_json_object(&frame)
                    && let Some(id) = object
                        .get("id")
                        .and_then(serde_json::Value::as_u64)
                        .filter(|id| *id > 0)
                {
                    response["id"] = json!(id);
                }

                self.queue_response(&response.to_string())?;
                Ok(())
            }
        }
    }

    pub fn collect_ready(&mut self, now: Instant) -> io::Result<()> {
        if self.terminal {
            return Ok(());
        }

        for finished in self.pending.poll(now) {
            let response = completion_json(finished)?;

            self.queue_response(&response)?;
        }

        Ok(())
    }

    /// Bind the receiver before admitting subscriptions. Retain this receiver
    /// for the session's lifetime; use the same one with collect_observations.
    pub fn bind_observations(&mut self, observations: &crate::fanout::ObservationReceiver) {
        self.observation_clock = Some(observations.clock.clone());
        self.observation_interest = Some(observations.interest.clone());
        observations.interest.set_keys(&self.subscriptions.keys());
    }

    /// Drain a bounded turn of observations after command replies have been
    /// queued. Publication boundaries, sticky loss and port isolation are owned
    /// by the same fanout/interest objects as in TCP.
    pub fn collect_observations(
        &mut self,
        observations: &crate::fanout::ObservationReceiver,
    ) -> io::Result<()> {
        if self.terminal || self.input_closed {
            return Ok(());
        }
        const MAX_OBSERVATIONS_PER_TURN: usize = 32;

        for _ in 0..MAX_OBSERVATIONS_PER_TURN {
            match observations.try_recv() {
                Ok(published) => {
                    if self
                        .activated_after
                        .get(&published.observation.source)
                        .is_none_or(|boundary| published.sequence <= *boundary)
                    {
                        continue;
                    }
                    if let Err(error) = self.enqueue_observation(&published.observation) {
                        if is_output_error(&error) {
                            break;
                        }

                        return Err(error);
                    }
                }

                Err(std::sync::mpsc::TryRecvError::Empty) => break,

                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    if self.subscriptions.is_empty() && !observations.terminal_when_inactive() {
                        break;
                    }
                    self.terminal = true;

                    let response = json!({
                        "status": "error",
                        "error": observations.closed_error()
                    });

                    if let Err(error) = self.queue_response(&response.to_string())
                        && !is_output_error(&error)
                    {
                        return Err(error);
                    }

                    break;
                }
            }
        }

        Ok(())
    }

    pub fn enqueue_observation(&mut self, observation: &Observation) -> io::Result<()> {
        if self.terminal || !self.subscriptions.contains(&observation.source) {
            return Ok(());
        }

        let event = crate::observation_json(None, observation)?;
        self.queue_response(&event)
    }
}
