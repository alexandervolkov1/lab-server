use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::model::{Observation, ParameterDescription, Value};
use crate::pending::{Completion, Pending, Poll};
use crate::protocol::{FrameDecoder, FrameError, ParseError, TcpRequest, parse_tcp_request};

#[derive(Debug, PartialEq, Eq)]
pub enum EnqueueError {
    DuplicateId,
    Full,
    RuntimeUnavailable,
    RuntimeBusy,
}

pub enum FinishedRequest {
    Read(Completion<Observation>),
    Set(Completion<Result<(), String>>),
}

fn completion_json(completion: FinishedRequest) -> io::Result<String> {
    match completion {
        FinishedRequest::Read(Completion::Received { id, value }) => {
            crate::observation_json(Some(id), &value)
        }

        FinishedRequest::Set(Completion::Received { id, value }) => {
            let response = match value {
                Ok(()) => json!({
                    "id": id,
                    "status": "ok"
                }),

                Err(error) => json!({
                    "id": id,
                    "status": "error",
                    "error": error
                }),
            };

            Ok(response.to_string())
        }

        FinishedRequest::Read(Completion::OutcomeUnknown { id })
        | FinishedRequest::Set(Completion::OutcomeUnknown { id }) => Ok(json!({
            "id": id, "status": "error", "error": "outcome_unknown"
        })
        .to_string()),

        FinishedRequest::Read(Completion::Timeout { id })
        | FinishedRequest::Set(Completion::Timeout { id }) => Ok(json!({
            "id": id,
            "status": "error",
            "error": "runtime_timeout"
        })
        .to_string()),

        FinishedRequest::Read(Completion::Disconnected { id })
        | FinishedRequest::Set(Completion::Disconnected { id }) => Ok(json!({
            "id": id,
            "status": "error",
            "error": "runtime_unavailable"
        })
        .to_string()),
    }
}

enum Entry {
    Read {
        id: u64,
        pending: Pending<Observation>,
    },
    Set {
        id: u64,
        pending: Pending<Result<(), String>>,
    },
}

impl Entry {
    fn id(&self) -> u64 {
        match self {
            Self::Read { id, .. } | Self::Set { id, .. } => *id,
        }
    }
}

pub struct PendingRequests {
    limit: usize,
    entries: VecDeque<Entry>,
}

impl PendingRequests {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn contains_id(&self, id: u64) -> bool {
        self.entries.iter().any(|entry| entry.id() == id)
    }

    fn ensure_available(&self, id: u64) -> Result<(), EnqueueError> {
        if self.contains_id(id) {
            return Err(EnqueueError::DuplicateId);
        }

        if self.entries.len() >= self.limit {
            return Err(EnqueueError::Full);
        }

        Ok(())
    }

    pub fn add_read(
        &mut self,
        id: u64,
        deadline: Instant,
        enqueue: impl FnOnce() -> Result<Receiver<Observation>, EnqueueError>,
    ) -> Result<(), EnqueueError> {
        self.ensure_available(id)?;

        let receiver = enqueue()?;

        self.entries.push_back(Entry::Read {
            id,
            pending: Pending::new(id, deadline, receiver),
        });

        Ok(())
    }

    pub fn add_set(
        &mut self,
        id: u64,
        deadline: Instant,
        enqueue: impl FnOnce(
            std::sync::Arc<crate::pending::SetExecution>,
        ) -> Result<Receiver<Result<(), String>>, EnqueueError>,
    ) -> Result<(), EnqueueError> {
        self.ensure_available(id)?;

        let execution = crate::pending::SetExecution::new(deadline);
        let receiver = enqueue(execution.clone())?;

        self.entries.push_back(Entry::Set {
            id,
            pending: Pending::new(id, deadline, receiver).with_execution(execution),
        });

        Ok(())
    }

    pub fn poll(&mut self, now: Instant) -> Vec<FinishedRequest> {
        let mut finished = Vec::new();
        let count = self.entries.len();

        for _ in 0..count {
            let entry = self.entries.pop_front().expect("entry count is known");

            match entry {
                Entry::Read { id, pending } => match pending.poll(now) {
                    Poll::Waiting(pending) => {
                        self.entries.push_back(Entry::Read { id, pending });
                    }
                    Poll::Finished(result) => {
                        finished.push(FinishedRequest::Read(result));
                    }
                },

                Entry::Set { id, pending } => match pending.poll(now) {
                    Poll::Waiting(pending) => {
                        self.entries.push_back(Entry::Set { id, pending });
                    }
                    Poll::Finished(result) => {
                        finished.push(FinishedRequest::Set(result));
                    }
                },
            }
        }

        finished
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum OutputError {
    InvalidLine,
    Full,
}

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to queue TCP response: {self:?}")
    }
}

impl std::error::Error for OutputError {}

fn is_output_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<OutputError>())
}

pub struct OutboundBuffer {
    max_bytes: usize,
    bytes: VecDeque<u8>,
}

impl OutboundBuffer {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            bytes: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn enqueue_line(&mut self, line: &str) -> Result<(), OutputError> {
        if line.is_empty() || line.as_bytes().contains(&b'\n') {
            return Err(OutputError::InvalidLine);
        }

        let needed = line.len().checked_add(1).ok_or(OutputError::Full)?;

        if needed > self.max_bytes.saturating_sub(self.bytes.len()) {
            return Err(OutputError::Full);
        }

        self.bytes.extend(line.bytes());
        self.bytes.push_back(b'\n');

        Ok(())
    }

    pub fn flush_to<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        while !self.bytes.is_empty() {
            let result = {
                let (front, back) = self.bytes.as_slices();
                let chunk = if front.is_empty() { back } else { front };

                writer.write(chunk)
            };

            match result {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "TCP writer returned zero bytes",
                    ));
                }

                Ok(written) => {
                    for _ in 0..written {
                        self.bytes.pop_front();
                    }
                }

                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(());
                }

                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }

                Err(error) => return Err(error),
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum Incoming {
    Request(TcpRequest),
    InvalidRequest { frame: String, error: ParseError },
    FramingError(FrameError),
}

const MAX_SUBSCRIPTIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionChange {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    UnknownKey(String),
    NotSubscribable(String),
    Full,
}

#[derive(Default)]
pub struct SubscriptionSet {
    keys: std::collections::BTreeSet<String>,
}

impl SubscriptionSet {
    pub fn contains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    pub fn keys(&self) -> Vec<String> {
        self.keys.iter().cloned().collect()
    }

    pub fn apply(
        &mut self,
        change: SubscriptionChange,
        keys: &[String],
        catalog: &HashMap<String, ParameterDescription>,
    ) -> Result<Vec<String>, SubscriptionError> {
        let mut next = self.keys.clone();

        for key in keys {
            let Some(description) = catalog.get(key) else {
                return Err(SubscriptionError::UnknownKey(key.clone()));
            };

            if description.default_poll_period.is_none() {
                return Err(SubscriptionError::NotSubscribable(key.clone()));
            }

            match change {
                SubscriptionChange::Subscribe => {
                    next.insert(key.clone());
                }

                SubscriptionChange::Unsubscribe => {
                    next.remove(key);
                }
            }
        }

        if next.len() > MAX_SUBSCRIPTIONS {
            return Err(SubscriptionError::Full);
        }

        self.keys = next;
        Ok(self.keys())
    }
}

pub struct ClientSession {
    decoder: FrameDecoder,
    pub pending: PendingRequests,
    pub outbound: OutboundBuffer,
    pub subscriptions: SubscriptionSet,
    terminal: bool,
    last_request_id: u64,
    observation_clock: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    activated_after: HashMap<String, u64>,
}

impl ClientSession {
    pub fn new(max_pending: usize, max_output_bytes: usize) -> Self {
        Self {
            decoder: FrameDecoder::default(),
            pending: PendingRequests::new(max_pending),
            outbound: OutboundBuffer::new(max_output_bytes),
            subscriptions: SubscriptionSet::default(),
            terminal: false,
            last_request_id: 0,
            observation_clock: None,
            activated_after: HashMap::new(),
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn receive(&mut self, bytes: &[u8]) -> Vec<Incoming> {
        if self.terminal {
            return Vec::new();
        }

        let (frames, framing_error) = match self.decoder.push(bytes) {
            Ok(frames) => (frames, None),

            Err(failure) => (failure.frames, Some(failure.error)),
        };

        let mut incoming = Vec::new();

        for frame in frames {
            match parse_tcp_request(&frame) {
                Ok(request) => incoming.push(Incoming::Request(request)),

                Err(error) => incoming.push(Incoming::InvalidRequest { frame, error }),
            }
        }

        if let Some(error) = framing_error {
            self.terminal = true;
            incoming.push(Incoming::FramingError(error));
        }

        incoming
    }

    fn queue_response(&mut self, response: &str) -> io::Result<()> {
        if let Err(error) = self.outbound.enqueue_line(response) {
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
        commands: &SyncSender<crate::runtime::DeviceCommand>,
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

        let result = self
            .pending
            .add_set(id, now + Duration::from_secs(10), |execution| {
                crate::queue_set(commands, key, value, execution)
            });

        match result {
            Ok(()) => Ok(()),

            Err(error) => {
                let code = match error {
                    EnqueueError::DuplicateId => "duplicate_id",
                    EnqueueError::Full => "too_many_pending",
                    EnqueueError::RuntimeUnavailable => "runtime_unavailable",
                    EnqueueError::RuntimeBusy => "runtime_busy",
                };

                self.queue_error(id, code)
            }
        }
    }

    pub fn dispatch_get(
        &mut self,
        id: u64,
        key: String,
        commands: &SyncSender<crate::runtime::DeviceCommand>,
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

        let result = self
            .pending
            .add_read(id, now + Duration::from_secs(10), || {
                crate::queue_read(commands, description)
            });

        match result {
            Ok(()) => Ok(()),

            Err(error) => {
                let code = match error {
                    EnqueueError::DuplicateId => "duplicate_id",
                    EnqueueError::Full => "too_many_pending",
                    EnqueueError::RuntimeUnavailable => "runtime_unavailable",
                    EnqueueError::RuntimeBusy => "runtime_busy",
                };

                self.queue_error(id, code)
            }
        }
    }

    pub fn dispatch_describe(
        &mut self,
        id: u64,
        catalog: &HashMap<String, ParameterDescription>,
    ) -> io::Result<()> {
        if self.pending.contains_id(id) {
            return self.queue_error(id, "duplicate_id");
        }

        let mut descriptions: Vec<_> = catalog.values().collect();
        descriptions.sort_unstable_by(|left, right| left.id.cmp(&right.id));

        let parameters: Vec<_> = descriptions
            .into_iter()
            .map(|description| {
                let (kind, min, max) = match &description.spec {
                    crate::model::Spec::Float { min, max } => ("float", json!(min), json!(max)),
                    crate::model::Spec::Integer { min, max } => ("integer", json!(min), json!(max)),
                };

                json!({
                    "key": &description.id,
                    "type": kind,
                    "min": min,
                    "max": max,
                    "unit": &description.unit,
                    "writable": description.writable,
                    "poll_period_ns": description
                        .default_poll_period
                        .map(|period| period.as_nanos())
                })
            })
            .collect();

        self.queue_response(
            &json!({
                "id": id,
                "status": "ok",
                "parameters": parameters
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

    pub fn dispatch_incoming(
        &mut self,
        incoming: Incoming,
        commands: &SyncSender<crate::runtime::DeviceCommand>,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        now: Instant,
    ) -> io::Result<Option<Incoming>> {
        let id = match &incoming {
            Incoming::Request(
                TcpRequest::Hello { id }
                | TcpRequest::Describe { id }
                | TcpRequest::Ping { id }
                | TcpRequest::Get { id, .. }
                | TcpRequest::Set { id, .. }
                | TcpRequest::Subscribe { id, .. }
                | TcpRequest::Unsubscribe { id, .. },
            ) => Some(*id),
            Incoming::InvalidRequest { frame, .. } => crate::protocol::parse_json_object(frame)
                .ok()
                .and_then(|object| object.get("id").and_then(serde_json::Value::as_u64))
                .filter(|id| *id > 0),
            Incoming::FramingError(_) => None,
        };
        if let Some(id) = id {
            if id <= self.last_request_id || self.pending.contains_id(id) {
                self.queue_response(
                    &json!({"status":"error", "error":"duplicate_id"}).to_string(),
                )?;
                return Ok(None);
            }
            self.last_request_id = id;
        }

        match incoming {
            Incoming::Request(TcpRequest::Subscribe { id, keys }) => {
                self.dispatch_subscription(id, SubscriptionChange::Subscribe, keys, catalog)?;
                Ok(None)
            }

            Incoming::Request(TcpRequest::Unsubscribe { id, keys }) => {
                self.dispatch_subscription(id, SubscriptionChange::Unsubscribe, keys, catalog)?;
                Ok(None)
            }

            Incoming::Request(TcpRequest::Describe { id }) => {
                self.dispatch_describe(id, catalog)?;
                Ok(None)
            }

            Incoming::Request(TcpRequest::Hello { id }) => {
                self.queue_response(
                    &json!({
                        "id": id,
                        "status": "ok",
                        "protocol": "lab-server",
                        "version": 1
                    })
                    .to_string(),
                )?;

                Ok(None)
            }

            Incoming::Request(TcpRequest::Ping { id }) => {
                self.queue_response(
                    &json!({
                        "id": id,
                        "status": "ok"
                    })
                    .to_string(),
                )?;

                Ok(None)
            }

            Incoming::Request(TcpRequest::Get { id, key }) => {
                self.dispatch_get(id, key, commands, catalog, latest, now)?;
                Ok(None)
            }

            Incoming::Request(TcpRequest::Set { id, key, value }) => {
                self.dispatch_set(id, key, value, commands, catalog, now)?;
                Ok(None)
            }

            Incoming::InvalidRequest { frame, error } => {
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
                Ok(None)
            }

            Incoming::FramingError(error) => {
                self.terminal = true;

                let code = match error {
                    FrameError::EmptyFrame => "empty_frame",
                    FrameError::FrameTooLong => "frame_too_long",
                    FrameError::InvalidUtf8 => "invalid_utf8",
                    FrameError::IncompleteFrame => "incomplete_frame",
                };

                self.queue_response(
                    &json!({
                        "status": "error",
                        "error": code
                    })
                    .to_string(),
                )?;

                Ok(None)
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

    pub fn enqueue_observation(&mut self, observation: &Observation) -> io::Result<()> {
        if self.terminal || !self.subscriptions.contains(&observation.source) {
            return Ok(());
        }

        let event = crate::observation_json(None, observation)?;
        self.queue_response(&event)
    }

    #[cfg(test)]
    pub fn finish(self) -> Result<(), FrameError> {
        self.decoder.finish()
    }
}

#[derive(Debug, PartialEq)]
pub struct IoStep {
    pub incoming: Vec<Incoming>,
    pub read_closed: bool,
}

pub struct TcpConnection {
    pub session: ClientSession,
    stream: TcpStream,
    read_closed: bool,
}

impl TcpConnection {
    pub fn new(stream: TcpStream, max_pending: usize, max_output_bytes: usize) -> io::Result<Self> {
        stream.set_nonblocking(true)?;

        Ok(Self {
            session: ClientSession::new(max_pending, max_output_bytes),
            stream,
            read_closed: false,
        })
    }

    pub fn pump(&mut self) -> io::Result<IoStep> {
        const READ_CHUNK_BYTES: usize = 4096;
        const MAX_READS_PER_PUMP: usize = 16;

        let mut incoming = Vec::new();

        if !self.read_closed && !self.session.is_terminal() {
            let mut bytes = [0u8; READ_CHUNK_BYTES];

            for _ in 0..MAX_READS_PER_PUMP {
                match self.stream.read(&mut bytes) {
                    Ok(0) => {
                        self.read_closed = true;

                        if let Err(error) = self.session.decoder.finish() {
                            self.session.terminal = true;
                            incoming.push(Incoming::FramingError(error));
                        }

                        break;
                    }

                    Ok(count) => {
                        incoming.extend(self.session.receive(&bytes[..count]));

                        if self.session.is_terminal() {
                            break;
                        }
                    }

                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        break;
                    }

                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        continue;
                    }

                    Err(error) => return Err(error),
                }
            }
        }

        if let Err(error) = self.session.collect_ready(Instant::now()) {
            if is_output_error(&error) {
                incoming.clear();
            } else {
                return Err(error);
            }
        }

        self.session.outbound.flush_to(&mut self.stream)?;

        Ok(IoStep {
            incoming,
            read_closed: self.read_closed,
        })
    }

    pub fn drive_once(
        &mut self,
        commands: &SyncSender<crate::runtime::DeviceCommand>,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
    ) -> io::Result<bool> {
        let step = self.pump()?;
        let now = Instant::now();

        for incoming in step.incoming {
            match self
                .session
                .dispatch_incoming(incoming, commands, catalog, latest, now)
            {
                Ok(None) => {}

                Ok(Some(_)) => {
                    return Err(io::Error::other("unhandled TCP event"));
                }

                Err(error) if is_output_error(&error) => {
                    break;
                }

                Err(error) => return Err(error),
            }
        }

        self.session.outbound.flush_to(&mut self.stream)?;

        let input_finished = step.read_closed || self.session.is_terminal();

        let pending_finished = self.session.is_terminal() || self.session.pending.is_empty();

        let output_finished = self.session.outbound.is_empty();

        Ok(input_finished && pending_finished && output_finished)
    }

    pub fn drive_once_with_observations(
        &mut self,
        commands: &SyncSender<crate::runtime::DeviceCommand>,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        observations: &crate::fanout::ObservationReceiver,
    ) -> io::Result<bool> {
        self.session.observation_clock = Some(observations.clock.clone());
        let finished = self.drive_once(commands, catalog, latest)?;

        if finished || self.read_closed || self.session.is_terminal() {
            return Ok(finished);
        }

        const MAX_OBSERVATIONS_PER_TURN: usize = 32;

        for _ in 0..MAX_OBSERVATIONS_PER_TURN {
            match observations.try_recv() {
                Ok(published) => {
                    if self
                        .session
                        .activated_after
                        .get(&published.observation.source)
                        .is_none_or(|boundary| published.sequence <= *boundary)
                    {
                        continue;
                    }
                    if let Err(error) = self.session.enqueue_observation(&published.observation) {
                        if is_output_error(&error) {
                            break;
                        }

                        return Err(error);
                    }
                }

                Err(std::sync::mpsc::TryRecvError::Empty) => break,

                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.session.terminal = true;

                    let response = json!({
                        "status": "error",
                        "error": observations.closed_error()
                    });

                    if let Err(error) = self.session.queue_response(&response.to_string())
                        && !is_output_error(&error)
                    {
                        return Err(error);
                    }

                    break;
                }
            }
        }

        self.session.outbound.flush_to(&mut self.stream)?;

        Ok(self.session.is_terminal() && self.session.outbound.is_empty())
    }

    pub fn run(
        &mut self,
        commands: &SyncSender<crate::runtime::DeviceCommand>,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        observations: &crate::fanout::ObservationReceiver,
    ) -> io::Result<()> {
        loop {
            if self.drive_once_with_observations(commands, catalog, latest, observations)? {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

#[cfg(test)]
mod tests;
