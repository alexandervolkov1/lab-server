//! Transport-neutral command, subscription and completion state machine.

use std::collections::HashMap;
use std::io;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};

use crate::model::{ActionDescription, Observation, ParameterDescription, Value};
use crate::outbound::{OutboundBuffer, is_output_error};
#[cfg(feature = "gate-metrics")]
pub mod metrics;
mod requests;
mod subscriptions;
use crate::protocol::{ParseError, Request, parse_request};
use requests::completion_json;
pub use requests::{FinishedRequest, PendingRequests};
pub use subscriptions::{SubscriptionChange, SubscriptionError, SubscriptionSet};

macro_rules! request_phase {
    ($phase:ident, $body:expr) => {{
        #[cfg(feature = "gate-metrics")]
        let _span = $crate::session::metrics::span($crate::session::metrics::Phase::$phase);
        $body
    }};
}
pub(crate) use request_phase;

pub(crate) fn describe_response(
    id: u64,
    catalog: &HashMap<String, ParameterDescription>,
    mut actions: Vec<ActionDescription>,
) -> String {
    #[cfg(feature = "gate-metrics")]
    let _describe = metrics::span(metrics::Phase::Describe);
    let descriptions = request_phase!(DescribeSort, {
        let mut descriptions: Vec<_> = catalog.values().collect();
        descriptions.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        descriptions
    });

    let parameters: Vec<_> = request_phase!(
        DescribeJson,
        descriptions
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
            .collect()
    );

    request_phase!(
        DescribeSort,
        actions.sort_unstable_by(|left, right| left.name.cmp(&right.name))
    );
    let response = request_phase!(DescribeJson, {
        let actions: Vec<_> = actions
            .into_iter()
            .map(|action| json!({ "key": action.name, "description": action.description }))
            .collect();
        json!({"id": id, "status": "ok", "parameters": parameters, "actions": actions})
    });
    request_phase!(DescribeSerialize, response.to_string())
}

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError {
    Authority(&'static str),
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
            Self::Authority(reason) => reason,
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
    fn audit_recorder(&self) -> Option<crate::recorder::RecorderHandle> {
        None
    }
    fn components_audited(
        &self,
        command: serde_json::Value,
        _audit: Option<&crate::audit::Operation>,
    ) -> Result<serde_json::Value, String> {
        self.components(command)
    }
    fn server_status(&self) -> Result<serde_json::Value, String> {
        Err("unsupported_op".into())
    }
    fn components(&self, _command: serde_json::Value) -> Result<serde_json::Value, String> {
        Err("unsupported_op".into())
    }
    fn catalog_snapshot(&self) -> Option<HashMap<String, ParameterDescription>> {
        None
    }
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
        #[cfg(feature = "gate-metrics")]
        let _parse = metrics::span(metrics::Phase::Parse);
        match parse_request(text) {
            Ok(request) => Self::Request(request),
            Err(error) => Self::InvalidRequest {
                frame: text.to_owned(),
                error,
            },
        }
    }

    fn id(&self) -> Option<u64> {
        match self {
            Self::Request(
                Request::ServerStatus { id }
                | Request::Components { id, .. }
                | Request::Hello { id }
                | Request::Describe { id }
                | Request::Ping { id }
                | Request::Get { id, .. }
                | Request::Set { id, .. }
                | Request::InvokeAction { id, .. }
                | Request::Subscribe { id, .. }
                | Request::Unsubscribe { id, .. },
            ) => Some(*id),
            Self::InvalidRequest { frame, .. } => crate::protocol::parse_json_object(frame)
                .ok()
                .and_then(|object| object.get("id").and_then(Json::as_u64))
                .filter(|id| *id > 0),
        }
    }

    fn audit_request(&self) -> (Json, Option<&str>) {
        match self {
            Self::Request(Request::Set { id, key, value }) => (
                json!({"id":id,"op":"set","key":key,"value":crate::audit::value(value)}),
                None,
            ),
            Self::Request(Request::InvokeAction { id, key }) => {
                (json!({"id":id,"op":"invoke_action","key":key}), None)
            }
            Self::Request(Request::Components { id, command }) => {
                (json!({"id":id,"op":"components","command":command}), None)
            }
            // Keep the audit representation for rejected mutations, including
            // frames with duplicate keys. ID validation uses the strict parser.
            Self::InvalidRequest { frame, .. } => (
                serde_json::from_str(frame).unwrap_or(Json::Null),
                Some(frame),
            ),
            _ => (Json::Null, None),
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
    connection_id: u64,
    pub(crate) audit_source: &'static str,
    audits: HashMap<u64, crate::audit::Operation>,
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
            connection_id: crate::audit::connection_id(),
            audit_source: "tcp",
            audits: HashMap::new(),
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
        #[cfg(feature = "gate-metrics")]
        let _response = metrics::span(metrics::Phase::Response);
        if !self.audits.is_empty()
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(response)
            && let Some(id) = v["id"].as_u64()
            && let Some(a) = self.audits.remove(&id)
        {
            a.response(v);
        }
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
        #[cfg(feature = "gate-metrics")]
        let _get = metrics::span(metrics::Phase::Get);
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

                if !request_phase!(Availability, commands.available(&key)) {
                    return self.queue_error(id, "runtime_unavailable");
                }

                request_phase!(
                    GetValue,
                    match guard.get(&key) {
                        Some(observation) => crate::observation_json(Some(id), observation)?,

                        None => json!({
                            "id": id,
                            "status": "no_data",
                            "key": key
                        })
                        .to_string(),
                    }
                )
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
        actions: Vec<ActionDescription>,
    ) -> io::Result<()> {
        if self.pending.contains_id(id) {
            return self.queue_error(id, "duplicate_id");
        }
        self.queue_response(&describe_response(id, catalog, actions))
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
        #[cfg(feature = "gate-metrics")]
        let dispatch = metrics::span(metrics::Phase::Dispatch);
        let id = incoming.id();
        let audit = commands.audit_recorder().and_then(|recorder| {
            let (request, invalid_frame) = incoming.audit_request();
            crate::audit::mutation(&request).then(|| {
                crate::audit::Operation::new(
                    &recorder,
                    json!({
                        "source": self.audit_source,
                        "connection_id": self.connection_id.to_string(),
                        "request_id": id.map(|v| v.to_string()),
                        "request": request,
                        "invalid_frame": invalid_frame
                    }),
                )
            })
        });
        if let Some(id) = id {
            if id <= self.last_request_id || self.pending.contains_id(id) {
                if let Some(a) = audit {
                    a.response(json!({"status":"error","error":"duplicate_id"}));
                }
                #[cfg(feature = "gate-metrics")]
                drop(dispatch);
                return self.queue_error(id, "duplicate_id");
            }
            self.last_request_id = id;
        }

        self.pending.audit = audit.clone();
        if let (Some(id), Some(a)) = (id, audit.as_ref()) {
            self.audits.insert(id, a.clone());
        }
        #[cfg(feature = "gate-metrics")]
        drop(dispatch);
        let result = match incoming {
            ParsedMessage::Request(request) => {
                self.dispatch_request(request, commands, catalog, latest, now, audit.as_ref())
            }
            ParsedMessage::InvalidRequest { error, .. } => {
                self.dispatch_invalid_request(id, error, audit.as_ref())
            }
        };
        self.pending.audit = None;
        result
    }

    fn dispatch_request(
        &mut self,
        request: Request,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        now: Instant,
        audit: Option<&crate::audit::Operation>,
    ) -> io::Result<()> {
        let snapshot = match request {
            Request::Describe { .. }
            | Request::Get { .. }
            | Request::Set { .. }
            | Request::Subscribe { .. }
            | Request::Unsubscribe { .. } => request_phase!(Catalog, commands.catalog_snapshot()),
            _ => None,
        };
        let catalog = snapshot.as_ref().unwrap_or(catalog);

        match request {
            Request::ServerStatus { id } => match commands.server_status() {
                Ok(data) => {
                    self.queue_response(&json!({"id":id,"status":"ok","data":data}).to_string())
                }
                Err(error) => self.queue_error(id, &error),
            },
            Request::Components { id, command } => {
                match commands.components_audited(command, audit) {
                    Ok(data) => {
                        self.queue_response(&json!({"id":id,"status":"ok","data":data}).to_string())
                    }
                    Err(error) => self.queue_error(id, &error),
                }
            }
            Request::Subscribe { id, keys } => {
                self.dispatch_subscription(id, SubscriptionChange::Subscribe, keys, catalog)
            }
            Request::Unsubscribe { id, keys } => {
                self.dispatch_subscription(id, SubscriptionChange::Unsubscribe, keys, catalog)
            }
            Request::Describe { id } => {
                self.describe_with_actions(id, catalog, request_phase!(Actions, commands.actions()))
            }
            Request::Hello { id } => self.queue_response(
                &json!({
                    "id": id,
                    "status": "ok",
                    "protocol": "lab-server",
                    "version": 1
                })
                .to_string(),
            ),
            Request::Ping { id } => {
                self.queue_response(&json!({"id":id,"status":"ok"}).to_string())
            }
            Request::Get { id, key } => self.dispatch_get(id, key, commands, catalog, latest, now),
            Request::Set { id, key, value } => {
                self.dispatch_set(id, key, value, commands, catalog, now)
            }
            Request::InvokeAction { id, key } => {
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
        }
    }

    fn dispatch_invalid_request(
        &mut self,
        id: Option<u64>,
        error: ParseError,
        audit: Option<&crate::audit::Operation>,
    ) -> io::Result<()> {
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

        let mut response = json!({"status":"error","error":code});
        if let Some(id) = id {
            response["id"] = json!(id);
        } else if let Some(a) = audit {
            a.response(response.clone());
        }
        self.queue_response(&response.to_string())
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

impl<Input> Drop for SessionState<Input> {
    fn drop(&mut self) {
        for a in self.audits.values() {
            a.event(
                "detached",
                json!({"reason":"session_dropped_without_response"}),
            );
        }
    }
}
