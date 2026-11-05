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
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::io::{BufRead, BufReader};
    use std::net::{Shutdown, TcpListener};
    use std::sync::mpsc;
    use std::time::{Duration, UNIX_EPOCH};

    use crate::model::Value;

    #[test]
    fn ids_are_monotonic_across_operations_and_invalid_requests() {
        let now = Instant::now();
        for duplicate in [
            r#"{"id":10,"op":"set","key":"unknown","value":1}"#,
            r#"{"id":10,"op":"ping"}"#,
            r#"{"id":10,"op":"get","key":"unknown"}"#,
            r#"{"id":10,"op":"subscribe","keys":[]}"#,
            r#"{"id":10,"op":"unsubscribe","keys":[]}"#,
            r#"{"id":10,"op":"set"}"#,
            r#"{"id":9,"op":"ping"}"#,
        ] {
            let mut session = ClientSession::new(2, 2048);
            let (tx, rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
            let catalog = power_catalog();
            let latest = Mutex::new(HashMap::new());
            for frame in [
                r#"{"id":10,"op":"set","key":"power","value":20}"#,
                duplicate,
            ] {
                for event in session.receive(format!("{frame}\n").as_bytes()) {
                    session
                        .dispatch_incoming(event, &tx, &catalog, &latest, now)
                        .unwrap();
                }
            }
            let crate::runtime::DeviceCommand::Set(command) = rx.try_recv().unwrap() else {
                panic!()
            };
            command.reply.send(Ok(())).unwrap();
            session.collect_ready(now).unwrap();
            for event in session.receive(b"{\"id\":10,\"op\":\"ping\"}\n") {
                session
                    .dispatch_incoming(event, &tx, &catalog, &latest, now)
                    .unwrap();
            }
            let mut bytes = Vec::new();
            session.outbound.flush_to(&mut bytes).unwrap();
            let responses: Vec<serde_json::Value> = std::str::from_utf8(&bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(
                responses.iter().filter(|r| r.get("id").is_some()).count(),
                1,
                "{duplicate}"
            );
            assert_eq!(responses[0]["error"], "duplicate_id");
            assert_eq!(responses[1]["id"], 10);
            assert_eq!(responses[2]["error"], "duplicate_id");
            assert!(rx.try_recv().is_err());
        }
    }

    #[test]
    fn request_ids_are_independent_between_clients() {
        let (tx, _rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        for _ in 0..2 {
            let mut session = ClientSession::new(1, 128);
            session
                .dispatch_incoming(
                    Incoming::Request(TcpRequest::Ping { id: 1 }),
                    &tx,
                    &HashMap::new(),
                    &Mutex::new(HashMap::new()),
                    Instant::now(),
                )
                .unwrap();
            let mut bytes = Vec::new();
            session.outbound.flush_to(&mut bytes).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["status"],
                "ok"
            );
        }
    }

    fn observation() -> Observation {
        Observation {
            source: "sample_temperature".to_owned(),
            unit: "degC".to_owned(),
            completed_at: UNIX_EPOCH,
            reading: Ok(Value::Float(20.0)),
        }
    }

    #[test]
    fn handles_read_and_set_without_blocking() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        let mut pending = PendingRequests::new(2);

        let (read_tx, read_rx) = mpsc::channel();
        let (set_tx, set_rx) = mpsc::channel();

        assert_eq!(pending.add_read(1, deadline, || Ok(read_rx)), Ok(()));
        assert_eq!(pending.add_set(2, deadline, |_| Ok(set_rx)), Ok(()));

        assert_eq!(pending.len(), 2);
        assert!(pending.poll(now).is_empty());

        set_tx.send(Ok(())).unwrap();

        let finished = pending.poll(now);

        assert_eq!(finished.len(), 1);
        assert!(matches!(
            &finished[0],
            FinishedRequest::Set(Completion::Received {
                id: 2,
                value: Ok(())
            })
        ));
        assert_eq!(pending.len(), 1);

        read_tx.send(observation()).unwrap();

        let finished = pending.poll(now);

        assert_eq!(finished.len(), 1);

        match &finished[0] {
            FinishedRequest::Read(Completion::Received { id, value }) => {
                assert_eq!(*id, 1);
                assert_eq!(value.source, "sample_temperature");
            }
            _ => panic!("expected completed Read"),
        }

        assert!(pending.is_empty());
    }

    #[test]
    fn rejects_duplicate_and_full_before_queueing() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        let mut pending = PendingRequests::new(1);
        let (_read_tx, read_rx) = mpsc::channel();

        assert_eq!(pending.add_read(7, deadline, || Ok(read_rx)), Ok(()));

        let called = Cell::new(false);

        assert_eq!(
            pending.add_set(7, deadline, |_| {
                called.set(true);
                Err(EnqueueError::RuntimeUnavailable)
            }),
            Err(EnqueueError::DuplicateId)
        );

        assert!(!called.get());

        assert_eq!(
            pending.add_set(8, deadline, |_| {
                called.set(true);
                Err(EnqueueError::RuntimeUnavailable)
            }),
            Err(EnqueueError::Full)
        );

        assert!(!called.get());
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn failed_enqueue_does_not_consume_capacity() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        let mut pending = PendingRequests::new(1);

        assert_eq!(
            pending.add_set(1, deadline, |_| Err(EnqueueError::RuntimeUnavailable)),
            Err(EnqueueError::RuntimeUnavailable)
        );

        assert!(pending.is_empty());

        let (_sender, receiver) = mpsc::channel();

        assert_eq!(pending.add_set(1, deadline, |_| Ok(receiver)), Ok(()));
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn timeout_and_disconnect_release_capacity() {
        let now = Instant::now();
        let mut pending = PendingRequests::new(2);

        let (read_tx, read_rx) = mpsc::channel();
        drop(read_tx);

        let (_set_tx, set_rx) = mpsc::channel();

        pending
            .add_read(1, now + Duration::from_secs(10), || Ok(read_rx))
            .unwrap();

        pending.add_set(2, now, |_| Ok(set_rx)).unwrap();

        let finished = pending.poll(now);

        assert_eq!(finished.len(), 2);

        assert!(matches!(
            &finished[0],
            FinishedRequest::Read(Completion::Disconnected { id: 1 })
        ));

        assert!(matches!(
            &finished[1],
            FinishedRequest::Set(Completion::Timeout { id: 2 })
        ));

        assert!(pending.is_empty());
    }

    enum WriteAction {
        Take(usize),
        WouldBlock,
        Interrupted,
    }

    struct ScriptedWriter {
        actions: VecDeque<WriteAction>,
        written: Vec<u8>,
    }

    impl Write for ScriptedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            match self.actions.pop_front() {
                Some(WriteAction::Take(limit)) => {
                    let count = limit.min(bytes.len());
                    self.written.extend_from_slice(&bytes[..count]);
                    Ok(count)
                }

                Some(WriteAction::WouldBlock) => Err(io::ErrorKind::WouldBlock.into()),

                Some(WriteAction::Interrupted) => Err(io::ErrorKind::Interrupted.into()),

                None => {
                    self.written.extend_from_slice(bytes);
                    Ok(bytes.len())
                }
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn preserves_partial_writes_and_message_order() {
        let mut buffer = OutboundBuffer::new(128);

        buffer.enqueue_line(r#"{"id":1,"status":"ok"}"#).unwrap();
        buffer.enqueue_line(r#"{"event":"measurement"}"#).unwrap();

        let mut writer = ScriptedWriter {
            actions: VecDeque::from(vec![
                WriteAction::Take(5),
                WriteAction::WouldBlock,
                WriteAction::Interrupted,
            ]),
            written: Vec::new(),
        };

        buffer.flush_to(&mut writer).unwrap();

        assert!(!buffer.is_empty());
        assert_eq!(writer.written, b"{\"id\"");

        buffer.flush_to(&mut writer).unwrap();

        assert!(buffer.is_empty());

        assert_eq!(
            String::from_utf8(writer.written).unwrap(),
            "{\"id\":1,\"status\":\"ok\"}\n{\"event\":\"measurement\"}\n"
        );
    }

    #[test]
    fn enforces_output_capacity_and_line_framing() {
        let mut buffer = OutboundBuffer::new(6);

        assert_eq!(buffer.enqueue_line(""), Err(OutputError::InvalidLine));
        assert_eq!(buffer.enqueue_line("a\nb"), Err(OutputError::InvalidLine));

        assert_eq!(buffer.enqueue_line("abcde"), Ok(()));
        assert_eq!(buffer.len(), 6);

        assert_eq!(buffer.enqueue_line("x"), Err(OutputError::Full));
        assert_eq!(buffer.len(), 6);

        let mut writer = Vec::new();
        buffer.flush_to(&mut writer).unwrap();

        assert_eq!(writer, b"abcde\n");
        assert!(buffer.is_empty());

        assert_eq!(buffer.enqueue_line("x"), Ok(()));
    }

    #[test]
    fn reports_zero_byte_write_without_losing_buffer() {
        let mut buffer = OutboundBuffer::new(16);
        buffer.enqueue_line("{}").unwrap();

        let mut writer = ScriptedWriter {
            actions: VecDeque::from(vec![WriteAction::Take(0)]),
            written: Vec::new(),
        };

        let error = buffer.flush_to(&mut writer).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn session_accepts_multiple_requests_across_reads() {
        let mut session = ClientSession::new(2, 128);

        assert!(session.receive(b"{\"id\":1,\"op\":\"pin").is_empty());

        assert_eq!(
            session.receive(b"g\"}\n{\"id\":2,\"op\":\"get\",\"key\":\"power\"}\n"),
            vec![
                Incoming::Request(TcpRequest::Ping { id: 1 }),
                Incoming::Request(TcpRequest::Get {
                    id: 2,
                    key: "power".to_owned(),
                }),
            ]
        );

        assert!(!session.is_terminal());
        assert!(session.pending.is_empty());

        session
            .outbound
            .enqueue_line(r#"{"id":1,"status":"ok"}"#)
            .unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        assert_eq!(output, b"{\"id\":1,\"status\":\"ok\"}\n");
    }

    #[test]
    fn session_preserves_requests_before_framing_error() {
        use crate::protocol::JsonError;

        let mut session = ClientSession::new(2, 128);

        let incoming = session
            .receive(b"{\"id\":1,\"op\":\"ping\"}\nnot-json\n{\"id\":2,\"op\":\"ping\"}\n\n");

        assert_eq!(
            incoming,
            vec![
                Incoming::Request(TcpRequest::Ping { id: 1 }),
                Incoming::InvalidRequest {
                    frame: "not-json".to_owned(),
                    error: ParseError::Json(JsonError::InvalidJson),
                },
                Incoming::Request(TcpRequest::Ping { id: 2 }),
                Incoming::FramingError(FrameError::EmptyFrame),
            ]
        );

        assert!(session.is_terminal());

        assert!(session.receive(b"{\"id\":3,\"op\":\"ping\"}\n").is_empty());
    }

    #[test]
    fn session_detects_incomplete_frame_at_eof() {
        let mut session = ClientSession::new(2, 128);

        assert!(session.receive(b"{\"id\":1").is_empty());

        assert_eq!(session.finish(), Err(FrameError::IncompleteFrame));
    }

    fn loopback_pair() -> (TcpConnection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();

        (TcpConnection::new(server, 2, 256).unwrap(), client)
    }

    fn wait_for_step(connection: &mut TcpConnection, ready: impl Fn(&IoStep) -> bool) -> IoStep {
        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            let step = connection.pump().unwrap();

            if ready(&step) {
                return step;
            }

            assert!(Instant::now() < deadline, "TCP test timed out");
            std::thread::yield_now();
        }
    }

    fn collect_until_read_closed(connection: &mut TcpConnection) -> Vec<Incoming> {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut incoming = Vec::new();

        loop {
            let step = connection.pump().unwrap();
            incoming.extend(step.incoming);

            if step.read_closed {
                return incoming;
            }

            assert!(Instant::now() < deadline, "TCP EOF test timed out");
            std::thread::yield_now();
        }
    }

    #[test]
    fn tcp_reads_request_and_writes_response() {
        let (mut connection, mut client) = loopback_pair();

        let idle = connection.pump().unwrap();
        assert!(idle.incoming.is_empty());
        assert!(!idle.read_closed);

        client.write_all(b"{\"id\":7,\"op\":\"ping\"}\n").unwrap();

        let step = wait_for_step(&mut connection, |step| !step.incoming.is_empty());

        assert_eq!(
            step.incoming,
            vec![Incoming::Request(TcpRequest::Ping { id: 7 })]
        );

        connection
            .session
            .outbound
            .enqueue_line(r#"{"id":7,"status":"ok"}"#)
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        while !connection.session.outbound.is_empty() {
            connection.pump().unwrap();

            assert!(Instant::now() < deadline, "TCP write timed out");
            std::thread::yield_now();
        }

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut reader = BufReader::new(client);
        let mut response = String::new();

        reader.read_line(&mut response).unwrap();

        assert_eq!(response, "{\"id\":7,\"status\":\"ok\"}\n");
    }

    #[test]
    fn tcp_detects_incomplete_frame_after_half_close() {
        let (mut connection, mut client) = loopback_pair();

        client
            .write_all(b"{\"id\":7,\"op\":\"ping\"}\n{\"id\":8")
            .unwrap();

        client.shutdown(Shutdown::Write).unwrap();

        let incoming = collect_until_read_closed(&mut connection);

        assert_eq!(
            incoming,
            vec![
                Incoming::Request(TcpRequest::Ping { id: 7 }),
                Incoming::FramingError(FrameError::IncompleteFrame),
            ]
        );

        assert!(connection.session.is_terminal());

        assert_eq!(
            connection.session.finish(),
            Err(FrameError::IncompleteFrame)
        );
    }

    #[test]
    fn tcp_clean_half_close_is_not_a_framing_error() {
        let (mut connection, mut client) = loopback_pair();

        client.write_all(b"{\"id\":9,\"op\":\"ping\"}\n").unwrap();

        client.shutdown(Shutdown::Write).unwrap();

        let incoming = collect_until_read_closed(&mut connection);

        assert_eq!(
            incoming,
            vec![Incoming::Request(TcpRequest::Ping { id: 9 })]
        );

        assert!(!connection.session.is_terminal());
        assert_eq!(connection.session.finish(), Ok(()));
    }

    #[test]
    fn tcp_clean_eof_preserves_pending_response() {
        let (mut connection, mut client) = loopback_pair();
        let now = Instant::now();

        let (reply_tx, reply_rx) = mpsc::channel();

        connection
            .session
            .pending
            .add_set(51, now + Duration::from_secs(10), |_| Ok(reply_rx))
            .unwrap();

        client.shutdown(Shutdown::Write).unwrap();

        assert!(collect_until_read_closed(&mut connection).is_empty());
        assert!(!connection.session.is_terminal());
        assert_eq!(connection.session.pending.len(), 1);

        reply_tx.send(Ok(())).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            connection.pump().unwrap();

            if connection.session.pending.is_empty() && connection.session.outbound.is_empty() {
                break;
            }

            assert!(Instant::now() < deadline, "TCP response timed out");
            std::thread::yield_now();
        }

        drop(connection);

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut wire = String::new();
        client.read_to_string(&mut wire).unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&wire).unwrap(),
            serde_json::json!({"id": 51, "status": "ok"})
        );
    }

    #[test]
    fn tcp_preserves_request_before_terminal_framing_error() {
        let (mut connection, mut client) = loopback_pair();

        client.write_all(b"{\"id\":1,\"op\":\"ping\"}\n\n").unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut incoming = Vec::new();

        loop {
            let step = connection.pump().unwrap();
            incoming.extend(step.incoming);

            if connection.session.is_terminal() {
                break;
            }

            assert!(Instant::now() < deadline, "TCP framing test timed out");
            std::thread::yield_now();
        }

        assert_eq!(
            incoming,
            vec![
                Incoming::Request(TcpRequest::Ping { id: 1 }),
                Incoming::FramingError(FrameError::EmptyFrame),
            ]
        );

        assert!(connection.session.is_terminal());
    }

    #[test]
    fn tcp_flushes_terminal_error_before_close() {
        let (mut connection, mut client) = loopback_pair();
        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        client
            .write_all(b"{\"id\":23,\"op\":\"ping\"}\n{\"id\":24")
            .unwrap();
        client.shutdown(Shutdown::Write).unwrap();

        let incoming = collect_until_read_closed(&mut connection);

        assert_eq!(
            incoming,
            vec![
                Incoming::Request(TcpRequest::Ping { id: 23 }),
                Incoming::FramingError(FrameError::IncompleteFrame),
            ]
        );

        for event in incoming {
            assert!(
                connection
                    .session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                    .unwrap()
                    .is_none()
            );
        }

        assert!(connection.session.is_terminal());

        let deadline = Instant::now() + Duration::from_secs(1);

        while !connection.session.outbound.is_empty() {
            connection.pump().unwrap();
            assert!(Instant::now() < deadline, "TCP output drain timed out");
            std::thread::yield_now();
        }

        drop(connection);

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut wire = String::new();
        client.read_to_string(&mut wire).unwrap();

        assert!(wire.ends_with('\n'));

        let responses: Vec<serde_json::Value> = wire
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(
            responses,
            vec![
                serde_json::json!({"id": 23, "status": "ok"}),
                serde_json::json!({
                    "status": "error",
                    "error": "incomplete_frame"
                }),
            ]
        );
    }

    #[test]
    fn session_encodes_completed_read_and_set() {
        let mut session = ClientSession::new(2, 1024);
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        let (read_tx, read_rx) = mpsc::channel();
        let (set_tx, set_rx) = mpsc::channel();

        session
            .pending
            .add_read(7, deadline, || Ok(read_rx))
            .unwrap();

        session
            .pending
            .add_set(8, deadline, |_| Ok(set_rx))
            .unwrap();

        session.collect_ready(now).unwrap();
        assert!(session.outbound.is_empty());

        read_tx.send(observation()).unwrap();
        set_tx.send(Err("invalid_value".to_owned())).unwrap();

        session.collect_ready(now).unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0]["id"], 7);
        assert_eq!(responses[0]["status"], "ok");
        assert_eq!(responses[0]["value"], 20.0);

        assert_eq!(responses[1]["id"], 8);
        assert_eq!(responses[1]["status"], "error");
        assert_eq!(responses[1]["error"], "invalid_value");

        assert!(session.pending.is_empty());
    }

    #[test]
    fn session_encodes_timeout_and_disconnection() {
        let mut session = ClientSession::new(2, 1024);
        let now = Instant::now();

        let (read_tx, read_rx) = mpsc::channel();
        drop(read_tx);

        let (_set_tx, set_rx) = mpsc::channel();

        session
            .pending
            .add_read(1, now + Duration::from_secs(10), || Ok(read_rx))
            .unwrap();

        session.pending.add_set(2, now, |_| Ok(set_rx)).unwrap();

        session.collect_ready(now).unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[0]["error"], "runtime_unavailable");

        assert_eq!(responses[1]["id"], 2);
        assert_eq!(responses[1]["error"], "runtime_timeout");
    }

    #[test]
    fn tcp_sends_completed_set_without_new_input() {
        let (mut connection, client) = loopback_pair();
        let now = Instant::now();

        let (reply_tx, reply_rx) = mpsc::channel();

        connection
            .session
            .pending
            .add_set(13, now + Duration::from_secs(10), |_| Ok(reply_rx))
            .unwrap();

        reply_tx.send(Ok(())).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            connection.pump().unwrap();

            if connection.session.pending.is_empty() && connection.session.outbound.is_empty() {
                break;
            }

            assert!(Instant::now() < deadline, "TCP response timed out");
            std::thread::yield_now();
        }

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut reader = BufReader::new(client);
        let mut line = String::new();

        reader.read_line(&mut line).unwrap();

        let response: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(response["id"], 13);
        assert_eq!(response["status"], "ok");
    }

    #[test]
    fn output_overflow_makes_session_terminal() {
        let mut session = ClientSession::new(1, 2);
        let now = Instant::now();

        let (reply_tx, reply_rx) = mpsc::channel();

        session
            .pending
            .add_set(1, now + Duration::from_secs(10), |_| Ok(reply_rx))
            .unwrap();

        reply_tx.send(Ok(())).unwrap();

        assert!(session.collect_ready(now).is_err());
        assert!(session.is_terminal());
    }

    #[test]
    fn drive_once_drains_buffer_after_pending_output_overflow() {
        let (mut connection, mut client) = loopback_pair();
        connection.session.outbound = OutboundBuffer::new(25);

        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        let (first_tx, first_rx) = mpsc::channel();
        let (second_tx, second_rx) = mpsc::channel();

        connection
            .session
            .pending
            .add_set(1, deadline, |_| Ok(first_rx))
            .unwrap();

        connection
            .session
            .pending
            .add_set(2, deadline, |_| Ok(second_rx))
            .unwrap();

        first_tx.send(Ok(())).unwrap();
        second_tx.send(Ok(())).unwrap();

        let timeout = Instant::now() + Duration::from_secs(1);

        loop {
            if connection
                .drive_once(&command_tx, &catalog, &latest)
                .unwrap()
            {
                break;
            }

            assert!(Instant::now() < timeout, "TCP drain timed out");
            std::thread::yield_now();
        }

        assert!(connection.session.is_terminal());
        assert!(connection.session.outbound.is_empty());

        drop(connection);

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut wire = String::new();
        client.read_to_string(&mut wire).unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&wire).unwrap(),
            serde_json::json!({"id": 1, "status": "ok"})
        );
    }

    #[test]
    fn drive_once_closes_when_response_exceeds_output_capacity() {
        let (mut connection, mut client) = loopback_pair();
        connection.session.outbound = OutboundBuffer::new(2);

        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        client.write_all(b"{\"id\":81,\"op\":\"ping\"}\n").unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            if connection
                .drive_once(&command_tx, &catalog, &latest)
                .unwrap()
            {
                break;
            }

            assert!(Instant::now() < deadline, "TCP close timed out");
            std::thread::yield_now();
        }

        assert!(connection.session.is_terminal());
        assert!(connection.session.outbound.is_empty());

        drop(connection);

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut wire = String::new();
        client.read_to_string(&mut wire).unwrap();

        assert!(wire.is_empty());
    }

    fn power_catalog() -> HashMap<String, ParameterDescription> {
        let description = ParameterDescription {
            id: "power".to_owned(),
            spec: crate::model::Spec::Integer { min: 0, max: 100 },
            unit: "%".to_owned(),
            writable: true,
            default_poll_period: None,
        };

        HashMap::from([(description.id.clone(), description)])
    }

    #[test]
    fn validates_set_spec_before_queueing() {
        for (writable, spec, value, expected) in [
            (
                false,
                crate::model::Spec::Integer { min: 0, max: 100 },
                Value::Integer(20),
                Some("Parameter is read-only"),
            ),
            (
                true,
                crate::model::Spec::Integer { min: 0, max: 100 },
                Value::Float(20.0),
                Some("Value does not match parameter spec"),
            ),
            (
                true,
                crate::model::Spec::Integer { min: 0, max: 100 },
                Value::Integer(101),
                Some("Value does not match parameter spec"),
            ),
            (
                true,
                crate::model::Spec::Float {
                    min: 0.0,
                    max: 100.0,
                },
                Value::Float(f64::NAN),
                Some("Value does not match parameter spec"),
            ),
            (
                true,
                crate::model::Spec::Integer { min: 100, max: 0 },
                Value::Integer(20),
                Some("Value does not match parameter spec"),
            ),
            (
                true,
                crate::model::Spec::Integer { min: 0, max: 100 },
                Value::Integer(20),
                None,
            ),
        ] {
            let mut catalog = power_catalog();
            let description = catalog.get_mut("power").unwrap();
            description.writable = writable;
            description.spec = spec;
            let (tx, rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
            let mut session = ClientSession::new(1, 1024);
            session
                .dispatch_set(1, "power".into(), value, &tx, &catalog, Instant::now())
                .unwrap();
            if let Some(expected) = expected {
                assert!(rx.try_recv().is_err(), "invalid Set reached device queue");
                assert!(session.pending.is_empty());
                let mut bytes = Vec::new();
                session.outbound.flush_to(&mut bytes).unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["error"],
                    expected
                );
            } else {
                assert!(matches!(
                    rx.try_recv(),
                    Ok(crate::runtime::DeviceCommand::Set(_))
                ));
                assert!(rx.try_recv().is_err());
                assert_eq!(session.pending.len(), 1);
            }
        }
    }

    #[test]
    fn dispatch_set_rejects_requests_before_device_queue() {
        let now = Instant::now();
        let catalog = power_catalog();

        let mut session = ClientSession::new(1, 512);
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        session
            .dispatch_set(
                1,
                "power".to_owned(),
                Value::Integer(25),
                &command_tx,
                &catalog,
                now,
            )
            .unwrap();

        let first_command = command_rx.try_recv().unwrap();

        session
            .dispatch_set(
                1,
                "power".to_owned(),
                Value::Integer(30),
                &command_tx,
                &catalog,
                now,
            )
            .unwrap();

        session
            .dispatch_set(
                2,
                "power".to_owned(),
                Value::Integer(40),
                &command_tx,
                &catalog,
                now,
            )
            .unwrap();

        session
            .dispatch_set(
                3,
                "unknown".to_owned(),
                Value::Integer(50),
                &command_tx,
                &catalog,
                now,
            )
            .unwrap();

        assert!(matches!(
            command_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0]["error"], "duplicate_id");
        assert_eq!(responses[1]["error"], "too_many_pending");
        assert_eq!(responses[2]["error"], "unknown_key");

        match first_command {
            crate::runtime::DeviceCommand::Set(command) => {
                assert_eq!(command.parameter, "power");
                assert_eq!(command.value, Value::Integer(25));
                command.reply.send(Ok(())).unwrap();
            }

            _ => panic!("expected Set command"),
        }

        session.collect_ready(now).unwrap();

        assert!(session.pending.is_empty());
    }

    #[test]
    fn tcp_accepts_next_request_while_set_is_pending() {
        let (mut connection, mut client) = loopback_pair();
        let catalog = power_catalog();

        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        client
            .write_all(b"{\"id\":10,\"op\":\"set\",\"key\":\"power\",\"value\":25}\n")
            .unwrap();

        let first = wait_for_step(&mut connection, |step| !step.incoming.is_empty());

        for incoming in first.incoming {
            match incoming {
                Incoming::Request(TcpRequest::Set { id, key, value }) => {
                    connection
                        .session
                        .dispatch_set(id, key, value, &command_tx, &catalog, Instant::now())
                        .unwrap();
                }

                _ => panic!("expected Set request"),
            }
        }

        let command = command_rx.try_recv().unwrap();

        assert_eq!(connection.session.pending.len(), 1);

        client.write_all(b"{\"id\":11,\"op\":\"ping\"}\n").unwrap();

        let second = wait_for_step(&mut connection, |step| !step.incoming.is_empty());

        assert_eq!(
            second.incoming,
            vec![Incoming::Request(TcpRequest::Ping { id: 11 })]
        );

        match command {
            crate::runtime::DeviceCommand::Set(command) => {
                assert_eq!(command.value, Value::Integer(25));
                command.reply.send(Ok(())).unwrap();
            }

            _ => panic!("expected Set command"),
        }

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            connection.pump().unwrap();

            if connection.session.pending.is_empty() && connection.session.outbound.is_empty() {
                break;
            }

            assert!(Instant::now() < deadline, "TCP response timed out");
            std::thread::yield_now();
        }

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut reader = BufReader::new(client);
        let mut line = String::new();

        reader.read_line(&mut line).unwrap();

        let response: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(response["id"], 10);
        assert_eq!(response["status"], "ok");
    }

    fn get_catalog() -> HashMap<String, ParameterDescription> {
        let on_demand = ParameterDescription {
            id: "configuration".to_owned(),
            spec: crate::model::Spec::Integer { min: 0, max: 100 },
            unit: "".to_owned(),
            writable: false,
            default_poll_period: None,
        };

        let polled = ParameterDescription {
            id: "sample_temperature".to_owned(),
            spec: crate::model::Spec::Float {
                min: -100.0,
                max: 500.0,
            },
            unit: "degC".to_owned(),
            writable: false,
            default_poll_period: Some(Duration::from_millis(100)),
        };

        HashMap::from([
            (on_demand.id.clone(), on_demand),
            (polled.id.clone(), polled),
        ])
    }

    #[test]
    fn dispatch_get_uses_latest_for_polled_signal() {
        let mut session = ClientSession::new(1, 512);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let now = Instant::now();

        session
            .dispatch_get(
                1,
                "sample_temperature".to_owned(),
                &command_tx,
                &catalog,
                &latest,
                now,
            )
            .unwrap();

        latest
            .lock()
            .unwrap()
            .insert("sample_temperature".to_owned(), observation());

        session
            .dispatch_get(
                2,
                "sample_temperature".to_owned(),
                &command_tx,
                &catalog,
                &latest,
                now,
            )
            .unwrap();

        assert!(session.pending.is_empty());

        assert!(matches!(
            command_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[0]["status"], "no_data");

        assert_eq!(responses[1]["id"], 2);
        assert_eq!(responses[1]["status"], "ok");
        assert_eq!(responses[1]["value"], 20.0);
    }

    #[test]
    fn dispatch_get_queues_on_demand_read() {
        let mut session = ClientSession::new(1, 512);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let now = Instant::now();

        session
            .dispatch_get(
                7,
                "configuration".to_owned(),
                &command_tx,
                &catalog,
                &latest,
                now,
            )
            .unwrap();

        assert_eq!(session.pending.len(), 1);
        assert!(session.outbound.is_empty());

        let command = command_rx.try_recv().unwrap();

        match command {
            crate::runtime::DeviceCommand::Read { description, reply } => {
                assert_eq!(description.id, "configuration");

                reply
                    .send(Observation {
                        source: "configuration".to_owned(),
                        unit: "".to_owned(),
                        completed_at: UNIX_EPOCH,
                        reading: Ok(Value::Integer(30)),
                    })
                    .unwrap();
            }

            _ => panic!("expected Read command"),
        }

        session.collect_ready(now).unwrap();

        assert!(session.pending.is_empty());

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let response: serde_json::Value =
            serde_json::from_slice(&output[..output.len() - 1]).unwrap();

        assert_eq!(response["id"], 7);
        assert_eq!(response["status"], "ok");
        assert_eq!(response["value"], 30);
    }

    #[test]
    fn cached_get_works_while_pending_queue_is_full() {
        let mut session = ClientSession::new(1, 512);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let now = Instant::now();

        session
            .dispatch_get(
                7,
                "configuration".to_owned(),
                &command_tx,
                &catalog,
                &latest,
                now,
            )
            .unwrap();

        assert_eq!(session.pending.len(), 1);

        session
            .dispatch_get(
                8,
                "sample_temperature".to_owned(),
                &command_tx,
                &catalog,
                &latest,
                now,
            )
            .unwrap();

        session
            .dispatch_get(
                7,
                "sample_temperature".to_owned(),
                &command_tx,
                &catalog,
                &latest,
                now,
            )
            .unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0]["id"], 8);
        assert_eq!(responses[0]["status"], "no_data");

        assert!(responses[1].get("id").is_none());
        assert_eq!(responses[1]["error"], "duplicate_id");

        assert_eq!(session.pending.len(), 1);
    }

    #[test]
    fn dispatch_incoming_routes_ping_get_and_set() {
        let mut session = ClientSession::new(4, 1024);

        let mut catalog = get_catalog();
        catalog.extend(power_catalog());

        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let input = concat!(
            "{\"id\":1,\"op\":\"ping\"}\n",
            "{\"id\":2,\"op\":\"get\",\"key\":\"sample_temperature\"}\n",
            "{\"id\":3,\"op\":\"set\",\"key\":\"power\",\"value\":25}\n"
        );

        let incoming = session.receive(input.as_bytes());

        assert_eq!(incoming.len(), 3);

        for event in incoming {
            let remaining = session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now())
                .unwrap();

            assert!(remaining.is_none());
        }

        assert_eq!(session.pending.len(), 1);

        match command_rx.try_recv().unwrap() {
            crate::runtime::DeviceCommand::Set(command) => {
                assert_eq!(command.parameter, "power");
                assert_eq!(command.value, Value::Integer(25));
            }
            _ => panic!("expected Set command"),
        }

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0], serde_json::json!({"id": 1, "status": "ok"}));

        assert_eq!(
            responses[1],
            serde_json::json!({
                "id": 2,
                "status": "no_data",
                "key": "sample_temperature"
            })
        );
    }

    #[test]
    fn dispatch_incoming_handles_subscribe_and_invalid_request() {
        let mut session = ClientSession::new(2, 512);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let input = concat!(
            "{\"id\":4,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\"]}\n",
            "{\"id\":5,\"op\":\"unknown\"}\n"
        );

        let incoming = session.receive(input.as_bytes());

        assert_eq!(incoming.len(), 2);

        let remaining: Vec<Incoming> = incoming
            .into_iter()
            .filter_map(|event| {
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now())
                    .unwrap()
            })
            .collect();

        assert!(remaining.is_empty());

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0]["id"], 4);
        assert_eq!(responses[0]["status"], "ok");
        assert_eq!(
            responses[0]["keys"],
            serde_json::json!(["sample_temperature"])
        );

        assert_eq!(responses[1]["id"], 5);
        assert_eq!(responses[1]["status"], "error");
        assert_eq!(responses[1]["error"], "unsupported_op");

        assert_eq!(session.subscriptions.keys(), vec!["sample_temperature"]);

        assert!(session.pending.is_empty());
    }

    #[test]
    fn hello_responds_immediately_and_rejects_pending_id() {
        let mut session = ClientSession::new(2, 512);
        let now = Instant::now();

        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let (_pending_tx, pending_rx) = mpsc::channel::<Observation>();

        session
            .pending
            .add_read(7, now + Duration::from_secs(10), || Ok(pending_rx))
            .unwrap();

        let input = concat!(
            "{\"id\":1,\"op\":\"hello\"}\n",
            "{\"id\":7,\"op\":\"hello\"}\n"
        );

        for event in session.receive(input.as_bytes()) {
            let remaining = session
                .dispatch_incoming(event, &command_tx, &catalog, &latest, now)
                .unwrap();

            assert!(remaining.is_none());
        }

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(
            responses[0],
            serde_json::json!({
                "id": 1,
                "status": "ok",
                "protocol": "lab-server",
                "version": 1
            })
        );

        assert!(responses[1].get("id").is_none());
        assert_eq!(responses[1]["status"], "error");
        assert_eq!(responses[1]["error"], "duplicate_id");

        assert_eq!(session.pending.len(), 1);
        assert!(session.outbound.is_empty());
        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn describe_serializes_catalog_without_device_commands() {
        let mut session = ClientSession::new(1, 2048);
        let now = Instant::now();

        let mut catalog = get_catalog();
        catalog.extend(power_catalog());

        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let (_pending_tx, pending_rx) = mpsc::channel::<Observation>();

        session
            .pending
            .add_read(7, now + Duration::from_secs(10), || Ok(pending_rx))
            .unwrap();

        let input = concat!(
            "{\"id\":42,\"op\":\"describe\"}\n",
            "{\"id\":7,\"op\":\"describe\"}\n"
        );

        for event in session.receive(input.as_bytes()) {
            assert!(
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, now,)
                    .unwrap()
                    .is_none()
            );
        }

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["id"], 42);
        assert_eq!(responses[0]["status"], "ok");

        let parameters = responses[0]["parameters"].as_array().unwrap();

        assert_eq!(parameters.len(), 3);

        let keys: Vec<_> = parameters
            .iter()
            .map(|parameter| parameter["key"].as_str().unwrap())
            .collect();

        assert_eq!(keys, vec!["configuration", "power", "sample_temperature"]);

        assert_eq!(parameters[0]["type"], "integer");
        assert_eq!(parameters[0]["min"], 0);
        assert_eq!(parameters[0]["max"], 100);
        assert!(parameters[0]["poll_period_ns"].is_null());

        assert_eq!(parameters[1]["unit"], "%");
        assert_eq!(parameters[1]["writable"], true);

        assert_eq!(parameters[2]["type"], "float");
        assert_eq!(parameters[2]["unit"], "degC");
        assert_eq!(parameters[2]["min"], -100.0);
        assert_eq!(parameters[2]["max"], 500.0);
        assert_eq!(parameters[2]["poll_period_ns"].as_u64(), Some(100_000_000));

        assert!(responses[1].get("id").is_none());
        assert_eq!(responses[1]["error"], "duplicate_id");

        assert_eq!(session.pending.len(), 1);
        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn subscriptions_are_atomic_and_idempotent() {
        let mut catalog = get_catalog();

        let mut heater = catalog["sample_temperature"].clone();
        heater.id = "heater_temperature".to_owned();
        catalog.insert(heater.id.clone(), heater);

        let mut subscriptions = SubscriptionSet::default();

        let result = subscriptions
            .apply(
                SubscriptionChange::Subscribe,
                &[
                    "sample_temperature".to_owned(),
                    "heater_temperature".to_owned(),
                ],
                &catalog,
            )
            .unwrap();

        assert_eq!(result, vec!["heater_temperature", "sample_temperature"]);

        assert_eq!(
            subscriptions
                .apply(
                    SubscriptionChange::Subscribe,
                    &["sample_temperature".to_owned()],
                    &catalog,
                )
                .unwrap(),
            result
        );

        assert_eq!(
            subscriptions.apply(
                SubscriptionChange::Subscribe,
                &["configuration".to_owned()],
                &catalog,
            ),
            Err(SubscriptionError::NotSubscribable(
                "configuration".to_owned()
            ))
        );

        assert_eq!(
            subscriptions.apply(
                SubscriptionChange::Unsubscribe,
                &["sample_temperature".to_owned(), "unknown".to_owned(),],
                &catalog,
            ),
            Err(SubscriptionError::UnknownKey("unknown".to_owned()))
        );

        assert_eq!(subscriptions.keys(), result);

        assert_eq!(
            subscriptions
                .apply(
                    SubscriptionChange::Unsubscribe,
                    &["sample_temperature".to_owned()],
                    &catalog,
                )
                .unwrap(),
            vec!["heater_temperature"]
        );

        assert_eq!(
            subscriptions
                .apply(
                    SubscriptionChange::Unsubscribe,
                    &["sample_temperature".to_owned()],
                    &catalog,
                )
                .unwrap(),
            vec!["heater_temperature"]
        );

        assert_eq!(
            subscriptions
                .apply(SubscriptionChange::Unsubscribe, &[], &catalog)
                .unwrap(),
            vec!["heater_temperature"]
        );
    }

    #[test]
    fn subscriptions_enforce_limit_and_are_per_session() {
        let seed = get_catalog();
        let template = seed["sample_temperature"].clone();

        let mut catalog = HashMap::new();
        let mut keys = Vec::new();

        for index in 0..65 {
            let mut description = template.clone();
            description.id = format!("signal_{index:02}");

            keys.push(description.id.clone());
            catalog.insert(description.id.clone(), description);
        }

        let mut first = ClientSession::new(1, 1024);
        let second = ClientSession::new(1, 1024);

        assert_eq!(
            first
                .subscriptions
                .apply(SubscriptionChange::Subscribe, &keys, &catalog,),
            Err(SubscriptionError::Full)
        );

        assert!(first.subscriptions.keys().is_empty());

        let result = first
            .subscriptions
            .apply(SubscriptionChange::Subscribe, &keys[..64], &catalog)
            .unwrap();

        assert_eq!(result.len(), 64);

        assert_eq!(
            first
                .subscriptions
                .apply(
                    SubscriptionChange::Subscribe,
                    &["signal_00".to_owned()],
                    &catalog,
                )
                .unwrap()
                .len(),
            64
        );

        assert!(first.subscriptions.contains("signal_00"));
        assert!(second.subscriptions.keys().is_empty());
    }

    #[test]
    fn dispatches_subscription_requests_and_responses() {
        let mut session = ClientSession::new(2, 4096);
        let mut catalog = get_catalog();

        let mut heater = catalog["sample_temperature"].clone();
        heater.id = "heater_temperature".to_owned();
        catalog.insert(heater.id.clone(), heater);

        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let input = concat!(
            "{\"id\":1,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\",\"configuration\"]}\n",
            "{\"id\":2,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\",\"heater_temperature\"]}\n",
            "{\"id\":3,\"op\":\"subscribe\",\"keys\":[\"missing\"]}\n",
            "{\"id\":4,\"op\":\"unsubscribe\",\"keys\":[]}\n",
            "{\"id\":5,\"op\":\"unsubscribe\",\"keys\":[\"sample_temperature\"]}\n"
        );

        for event in session.receive(input.as_bytes()) {
            assert!(
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                    .unwrap()
                    .is_none()
            );
        }

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 5);

        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[0]["error"], "not_subscribable");
        assert_eq!(responses[0]["key"], "configuration");

        assert_eq!(responses[1]["id"], 2);
        assert_eq!(responses[1]["status"], "ok");
        assert_eq!(
            responses[1]["keys"],
            serde_json::json!(["heater_temperature", "sample_temperature"])
        );

        assert_eq!(responses[2]["id"], 3);
        assert_eq!(responses[2]["error"], "unknown_key");
        assert_eq!(responses[2]["key"], "missing");

        assert_eq!(responses[3]["id"], 4);
        assert_eq!(responses[3]["keys"], responses[1]["keys"]);

        assert_eq!(responses[4]["id"], 5);
        assert_eq!(
            responses[4]["keys"],
            serde_json::json!(["heater_temperature"])
        );

        assert_eq!(session.subscriptions.keys(), vec!["heater_temperature"]);

        assert!(session.pending.is_empty());
        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn invalid_requests_return_codes_without_ambiguous_ids() {
        let mut session = ClientSession::new(2, 4096);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let input = concat!(
            "{\"id\":10,\"op\":\"unknown\"}\n",
            "{\"id\":11,\"op\":\"get\"}\n",
            "{\"id\":12,\"op\":\"get\",\"key\":\"a\",\"key\":\"b\"}\n",
            "not-json\n",
            "[]\n",
            "{\"op\":\"ping\"}\n",
            "{\"id\":13,\"op\":\"ping\"}\n"
        );

        let incoming = session.receive(input.as_bytes());
        assert_eq!(incoming.len(), 7);

        for event in incoming {
            assert!(
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                    .unwrap()
                    .is_none()
            );
        }

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 7);

        assert_eq!(responses[0]["id"], 10);
        assert_eq!(responses[0]["error"], "unsupported_op");

        assert_eq!(responses[1]["id"], 11);
        assert_eq!(responses[1]["error"], "invalid_key");

        assert_eq!(responses[2]["error"], "duplicate_key");
        assert!(responses[2].get("id").is_none());

        assert_eq!(responses[3]["error"], "invalid_json");
        assert!(responses[3].get("id").is_none());

        assert_eq!(responses[4]["error"], "expected_object");
        assert!(responses[4].get("id").is_none());

        assert_eq!(responses[5]["error"], "invalid_id");
        assert!(responses[5].get("id").is_none());

        assert_eq!(responses[6]["id"], 13);
        assert_eq!(responses[6]["status"], "ok");

        assert!(!session.is_terminal());
        assert!(session.pending.is_empty());
        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn framing_error_preserves_valid_reply_before_terminal_error() {
        let mut session = ClientSession::new(2, 512);
        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let incoming = session.receive(b"{\"id\":1,\"op\":\"ping\"}\n\n");

        assert_eq!(
            incoming,
            vec![
                Incoming::Request(TcpRequest::Ping { id: 1 }),
                Incoming::FramingError(FrameError::EmptyFrame),
            ]
        );

        for event in incoming {
            assert!(
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                    .unwrap()
                    .is_none()
            );
        }

        assert!(session.is_terminal());
        assert!(session.receive(b"{\"id\":2,\"op\":\"ping\"}\n").is_empty());

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0], serde_json::json!({"id": 1, "status": "ok"}));

        assert_eq!(
            responses[1],
            serde_json::json!({
                "status": "error",
                "error": "empty_frame"
            })
        );

        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn terminal_session_does_not_append_pending_responses() {
        let mut session = ClientSession::new(1, 256);
        let now = Instant::now();

        let (reply_tx, reply_rx) = mpsc::channel();

        session
            .pending
            .add_set(7, now + Duration::from_secs(10), |_| Ok(reply_rx))
            .unwrap();

        let incoming = session.receive(b"\n");

        assert_eq!(
            incoming,
            vec![Incoming::FramingError(FrameError::EmptyFrame)]
        );

        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        for event in incoming {
            assert!(
                session
                    .dispatch_incoming(event, &command_tx, &catalog, &latest, now)
                    .unwrap()
                    .is_none()
            );
        }

        reply_tx.send(Ok(())).unwrap();
        session.collect_ready(now).unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(
            responses,
            vec![serde_json::json!({
                "status": "error",
                "error": "empty_frame"
            })]
        );

        assert!(session.is_terminal());
    }

    #[test]
    fn framing_errors_are_reported_without_request_id() {
        let cases = [
            (b"\xff\n".to_vec(), FrameError::InvalidUtf8, "invalid_utf8"),
            (
                vec![b'x'; crate::protocol::MAX_FRAME_BYTES + 1],
                FrameError::FrameTooLong,
                "frame_too_long",
            ),
        ];

        for (bytes, expected_error, expected_code) in cases {
            let mut session = ClientSession::new(1, 256);
            let catalog = HashMap::new();
            let latest = Mutex::new(HashMap::new());
            let (command_tx, _command_rx) =
                mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

            let incoming = session.receive(&bytes);

            assert_eq!(incoming, vec![Incoming::FramingError(expected_error)]);

            for event in incoming {
                assert!(
                    session
                        .dispatch_incoming(event, &command_tx, &catalog, &latest, Instant::now(),)
                        .unwrap()
                        .is_none()
                );
            }

            assert!(session.is_terminal());

            let mut output = Vec::new();
            session.outbound.flush_to(&mut output).unwrap();

            let response: serde_json::Value = serde_json::from_slice(&output).unwrap();

            assert_eq!(response["status"], "error");
            assert_eq!(response["error"], expected_code);
            assert!(response.get("id").is_none());
        }
    }

    #[test]
    fn drive_once_waits_for_pending_after_clean_eof() {
        let (mut connection, mut client) = loopback_pair();

        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let (reply_tx, reply_rx) = mpsc::channel();

        connection
            .session
            .pending
            .add_set(61, Instant::now() + Duration::from_secs(10), |_| {
                Ok(reply_rx)
            })
            .unwrap();

        client.shutdown(Shutdown::Write).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            let ready = connection
                .drive_once(&command_tx, &catalog, &latest)
                .unwrap();

            assert!(!ready);

            if connection.read_closed {
                break;
            }

            assert!(Instant::now() < deadline, "TCP EOF timed out");
            std::thread::yield_now();
        }

        assert_eq!(connection.session.pending.len(), 1);

        reply_tx.send(Ok(())).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            if connection
                .drive_once(&command_tx, &catalog, &latest)
                .unwrap()
            {
                break;
            }

            assert!(Instant::now() < deadline, "TCP drain timed out");
            std::thread::yield_now();
        }

        drop(connection);

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut wire = String::new();
        client.read_to_string(&mut wire).unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&wire).unwrap(),
            serde_json::json!({"id": 61, "status": "ok"})
        );
    }

    #[test]
    fn drive_once_drains_terminal_error_before_close() {
        let (mut connection, mut client) = loopback_pair();

        let catalog = HashMap::new();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        client
            .write_all(b"{\"id\":71,\"op\":\"ping\"}\n\n")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            if connection
                .drive_once(&command_tx, &catalog, &latest)
                .unwrap()
            {
                break;
            }

            assert!(Instant::now() < deadline, "TCP terminal drain timed out");
            std::thread::yield_now();
        }

        assert!(connection.session.is_terminal());
        assert!(connection.session.outbound.is_empty());

        drop(connection);

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut wire = String::new();
        client.read_to_string(&mut wire).unwrap();

        let responses: Vec<serde_json::Value> = wire
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(
            responses,
            vec![
                serde_json::json!({"id": 71, "status": "ok"}),
                serde_json::json!({
                    "status": "error",
                    "error": "empty_frame"
                }),
            ]
        );
    }

    #[test]
    fn subscribe_excludes_old_publications_and_orders_ack_first() {
        let (mut connection, mut client) = loopback_pair();
        connection.session.outbound = OutboundBuffer::new(4096);
        let fanout = crate::fanout::ObservationFanout::new(8);
        let observations = fanout.register().unwrap();
        let (tx, _rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        fanout.publish(&observation()).unwrap();
        connection.session.observation_clock = Some(observations.clock.clone());
        connection
            .session
            .dispatch_subscription(
                1,
                SubscriptionChange::Subscribe,
                vec!["sample_temperature".into()],
                &catalog,
            )
            .unwrap();
        for value in [21.0, 22.0] {
            let mut observation = observation();
            observation.reading = Ok(Value::Float(value));
            fanout.publish(&observation).unwrap();
        }
        connection
            .drive_once_with_observations(&tx, &catalog, &latest, &observations)
            .unwrap();
        drop(connection);
        let mut text = String::new();
        client.read_to_string(&mut text).unwrap();
        let messages: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["id"], 1);
        assert_eq!(messages[1]["value"], 21.0);
        assert_eq!(messages[2]["value"], 22.0);
    }

    #[test]
    fn tcp_disconnect_releases_slot_for_reconnection() {
        let slots = crate::client_limit::ClientSlots::new();
        let held: Vec<_> = (1..crate::client_limit::MAX_CLIENTS)
            .map(|_| slots.try_acquire().unwrap())
            .collect();
        for _ in 0..2 {
            let slot = slots.try_acquire().unwrap();
            assert!(slots.try_acquire().is_none());
            let (mut connection, client) = loopback_pair();
            let worker = std::thread::spawn(move || {
                let _slot = slot;
                let (tx, _rx) = mpsc::sync_channel(1);
                let fanout = crate::fanout::ObservationFanout::new(1);
                let observations = fanout.register().unwrap();
                connection
                    .run(
                        &tx,
                        &HashMap::new(),
                        &Mutex::new(HashMap::new()),
                        &observations,
                    )
                    .unwrap();
            });
            client.shutdown(Shutdown::Both).unwrap();
            worker.join().unwrap();
        }
        drop(held);
    }

    #[test]
    fn tcp_reports_upstream_data_loss() {
        let (mut connection, mut client) = loopback_pair();
        let fanout = crate::fanout::ObservationFanout::new(2);
        let observations = fanout.register().unwrap();
        let (tx, _rx) = mpsc::sync_channel(1);
        fanout
            .loss_epoch()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(
            connection
                .drive_once_with_observations(
                    &tx,
                    &get_catalog(),
                    &Mutex::new(HashMap::new()),
                    &observations
                )
                .unwrap()
        );
        drop(connection);
        let mut text = String::new();
        client.read_to_string(&mut text).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap()["error"],
            "observation_data_lost"
        );
    }

    #[test]
    fn runtime_queue_overflow_releases_pending_capacity() {
        let (tx, _rx) = mpsc::sync_channel(1);
        let catalog = power_catalog();
        let mut session = ClientSession::new(4, 1024);
        let now = Instant::now();
        session
            .dispatch_set(1, "power".into(), Value::Integer(1), &tx, &catalog, now)
            .unwrap();
        session
            .dispatch_set(2, "power".into(), Value::Integer(2), &tx, &catalog, now)
            .unwrap();
        session
            .dispatch_get(
                3,
                "power".into(),
                &tx,
                &catalog,
                &Mutex::new(HashMap::new()),
                now,
            )
            .unwrap();
        assert_eq!(session.pending.len(), 1);
        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();
        let messages: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["error"], "runtime_busy");
        assert_eq!(messages[1]["error"], "runtime_busy");
    }

    #[test]
    fn subscription_boundaries_are_per_key_and_per_client() {
        let fanout = crate::fanout::ObservationFanout::new(16);
        let catalog = get_catalog();
        let (tx, _rx) = mpsc::sync_channel(1);
        let latest = Mutex::new(HashMap::new());
        let (mut first, mut first_client) = loopback_pair();
        let (mut second, mut second_client) = loopback_pair();
        first.session.outbound = OutboundBuffer::new(4096);
        second.session.outbound = OutboundBuffer::new(4096);
        let first_rx = fanout.register().unwrap();
        let second_rx = fanout.register().unwrap();
        for session in [&mut first.session, &mut second.session] {
            session.observation_clock = Some(first_rx.clock.clone());
        }
        let keys = vec!["sample_temperature".into()];
        first
            .session
            .dispatch_subscription(1, SubscriptionChange::Subscribe, keys.clone(), &catalog)
            .unwrap();
        fanout.publish(&observation()).unwrap();
        second
            .session
            .dispatch_subscription(1, SubscriptionChange::Subscribe, keys.clone(), &catalog)
            .unwrap();
        // Idempotent subscribe must preserve the first client's queued event.
        first
            .session
            .dispatch_subscription(2, SubscriptionChange::Subscribe, keys.clone(), &catalog)
            .unwrap();
        first
            .drive_once_with_observations(&tx, &catalog, &latest, &first_rx)
            .unwrap();
        second
            .drive_once_with_observations(&tx, &catalog, &latest, &second_rx)
            .unwrap();
        first
            .session
            .dispatch_subscription(3, SubscriptionChange::Unsubscribe, keys.clone(), &catalog)
            .unwrap();
        fanout.publish(&observation()).unwrap();
        first
            .session
            .dispatch_subscription(4, SubscriptionChange::Subscribe, keys, &catalog)
            .unwrap();
        first
            .drive_once_with_observations(&tx, &catalog, &latest, &first_rx)
            .unwrap();
        second
            .drive_once_with_observations(&tx, &catalog, &latest, &second_rx)
            .unwrap();
        drop(first);
        drop(second);
        for client in [&mut first_client, &mut second_client] {
            let mut text = String::new();
            client.read_to_string(&mut text).unwrap();
            let events = text
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|message| message.get("event").is_some())
                .count();
            assert_eq!(events, 1);
        }
    }

    #[test]
    fn tcp_streams_only_subscribed_observations() {
        let (mut connection, mut client) = loopback_pair();

        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());

        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        let source_tx = crate::fanout::ObservationFanout::new(8);
        let source_rx = source_tx.register().unwrap();

        client
            .write_all(b"{\"id\":1,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\"]}\n")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        while !connection
            .session
            .subscriptions
            .contains("sample_temperature")
        {
            assert!(
                !connection
                    .drive_once_with_observations(&command_tx, &catalog, &latest, &source_rx,)
                    .unwrap()
            );

            assert!(Instant::now() < deadline, "subscribe timed out");
            std::thread::yield_now();
        }

        let mut ignored = observation();
        ignored.source = "configuration".to_owned();

        source_tx.publish(&ignored).unwrap();
        source_tx.publish(&observation()).unwrap();

        assert!(
            !connection
                .drive_once_with_observations(&command_tx, &catalog, &latest, &source_rx,)
                .unwrap()
        );

        assert!(connection.session.outbound.is_empty());

        drop(connection);

        let mut output = String::new();
        client.read_to_string(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(
            responses[0],
            serde_json::json!({
                "id": 1,
                "status": "ok",
                "keys": ["sample_temperature"]
            })
        );

        assert_eq!(responses[1]["event"], "measurement");
        assert_eq!(responses[1]["key"], "sample_temperature");
        assert_eq!(responses[1]["value"], 20.0);
        assert!(responses[1].get("id").is_none());

        assert!(command_rx.try_recv().is_err());
    }

    #[test]
    fn tcp_unsubscribe_stops_measurements_after_ack() {
        let (mut connection, mut client) = loopback_pair();

        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());

        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        let observation_tx = crate::fanout::ObservationFanout::new(8);
        let observation_rx = observation_tx.register().unwrap();

        client
            .write_all(b"{\"id\":1,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\"]}\n")
            .unwrap();

        let mut deadline = Instant::now() + Duration::from_secs(1);

        while !connection
            .session
            .subscriptions
            .contains("sample_temperature")
        {
            assert!(
                !connection
                    .drive_once_with_observations(&command_tx, &catalog, &latest, &observation_rx,)
                    .unwrap()
            );

            assert!(Instant::now() < deadline, "subscribe timed out");
            std::thread::yield_now();
        }

        observation_tx.publish(&observation()).unwrap();

        assert!(
            !connection
                .drive_once_with_observations(&command_tx, &catalog, &latest, &observation_rx,)
                .unwrap()
        );

        assert!(connection.session.outbound.is_empty());

        client
            .write_all(b"{\"id\":2,\"op\":\"unsubscribe\",\"keys\":[\"sample_temperature\"]}\n")
            .unwrap();

        deadline = Instant::now() + Duration::from_secs(1);

        while connection
            .session
            .subscriptions
            .contains("sample_temperature")
        {
            assert!(
                !connection
                    .drive_once_with_observations(&command_tx, &catalog, &latest, &observation_rx,)
                    .unwrap()
            );

            assert!(Instant::now() < deadline, "unsubscribe timed out");
            std::thread::yield_now();
        }

        observation_tx.publish(&observation()).unwrap();

        assert!(
            !connection
                .drive_once_with_observations(&command_tx, &catalog, &latest, &observation_rx,)
                .unwrap()
        );

        assert!(connection.session.outbound.is_empty());
        assert!(connection.session.subscriptions.keys().is_empty());

        drop(connection);

        let mut output = String::new();
        client.read_to_string(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 3);

        assert_eq!(
            responses[0],
            serde_json::json!({
                "id": 1,
                "status": "ok",
                "keys": ["sample_temperature"]
            })
        );

        assert_eq!(responses[1]["event"], "measurement");
        assert_eq!(responses[1]["key"], "sample_temperature");
        assert!(responses[1].get("id").is_none());

        assert_eq!(
            responses[2],
            serde_json::json!({
                "id": 2,
                "status": "ok",
                "keys": []
            })
        );
    }

    #[test]
    fn subscription_enqueues_only_selected_measurements() {
        let mut session = ClientSession::new(1, 2048);
        let catalog = get_catalog();

        session
            .dispatch_subscription(
                7,
                SubscriptionChange::Subscribe,
                vec!["sample_temperature".to_owned()],
                &catalog,
            )
            .unwrap();

        session.enqueue_observation(&observation()).unwrap();

        let mut other = observation();
        other.source = "configuration".to_owned();

        session.enqueue_observation(&other).unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 2);

        assert_eq!(responses[0]["id"], 7);
        assert_eq!(
            responses[0]["keys"],
            serde_json::json!(["sample_temperature"])
        );

        assert_eq!(responses[1]["event"], "measurement");
        assert_eq!(responses[1]["key"], "sample_temperature");
        assert_eq!(responses[1]["value"], 20.0);
        assert!(responses[1].get("id").is_none());
    }

    #[test]
    fn unsubscribe_stops_new_measurements_after_ack() {
        let mut session = ClientSession::new(1, 2048);
        let catalog = get_catalog();
        let key = "sample_temperature".to_owned();

        session
            .dispatch_subscription(
                1,
                SubscriptionChange::Subscribe,
                vec![key.clone()],
                &catalog,
            )
            .unwrap();

        session.enqueue_observation(&observation()).unwrap();

        session
            .dispatch_subscription(2, SubscriptionChange::Unsubscribe, vec![key], &catalog)
            .unwrap();

        session.enqueue_observation(&observation()).unwrap();

        let mut output = Vec::new();
        session.outbound.flush_to(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 3);

        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[0]["status"], "ok");

        assert_eq!(responses[1]["event"], "measurement");
        assert_eq!(responses[1]["key"], "sample_temperature");

        assert_eq!(responses[2]["id"], 2);
        assert_eq!(responses[2]["status"], "ok");
        assert_eq!(responses[2]["keys"], serde_json::json!([]));

        assert!(session.subscriptions.keys().is_empty());
    }

    #[test]
    fn tcp_closes_after_observation_channel_overflow() {
        let (mut connection, mut client) = loopback_pair();

        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        let (command_tx, _command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);

        let fanout = crate::fanout::ObservationFanout::new(1);
        let observations = fanout.register().unwrap();

        client
            .write_all(b"{\"id\":1,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\"]}\n")
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        while !connection
            .session
            .subscriptions
            .contains("sample_temperature")
        {
            assert!(
                !connection
                    .drive_once_with_observations(&command_tx, &catalog, &latest, &observations,)
                    .unwrap()
            );

            assert!(Instant::now() < deadline, "subscribe timed out");
            std::thread::yield_now();
        }

        fanout.publish(&observation()).unwrap();

        let mut second = observation();
        second.reading = Ok(Value::Float(21.0));

        fanout.publish(&second).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);

        loop {
            let finished = connection
                .drive_once_with_observations(&command_tx, &catalog, &latest, &observations)
                .unwrap();

            if finished {
                break;
            }

            assert!(Instant::now() < deadline, "TCP close timed out");
            std::thread::yield_now();
        }

        assert!(connection.session.is_terminal());
        assert!(connection.session.outbound.is_empty());

        drop(connection);

        let mut output = String::new();
        client.read_to_string(&mut output).unwrap();

        let responses: Vec<serde_json::Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(responses.len(), 3);

        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[0]["status"], "ok");

        assert_eq!(responses[1]["event"], "measurement");
        assert_eq!(responses[1]["value"], 20.0);

        assert_eq!(
            responses[2],
            serde_json::json!({
                "status": "error",
                "error": "observation_stream_closed"
            })
        );
    }

    #[test]
    fn tcp_run_combines_commands_and_observation_stream() {
        let (mut connection, mut client) = loopback_pair();

        let (command_tx, command_rx) = mpsc::sync_channel(crate::runtime::COMMAND_QUEUE_CAPACITY);
        let observation_tx = crate::fanout::ObservationFanout::new(2);
        let observation_rx = observation_tx.register().unwrap();

        let worker = std::thread::spawn(move || {
            let catalog = get_catalog();
            let latest = Mutex::new(HashMap::new());

            connection.run(&command_tx, &catalog, &latest, &observation_rx)
        });

        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut line = String::new();

        client
            .write_all(b"{\"id\":1,\"op\":\"subscribe\",\"keys\":[\"sample_temperature\"]}\n")
            .unwrap();

        reader.read_line(&mut line).unwrap();

        let response: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(response["id"], 1);
        assert_eq!(response["status"], "ok");

        observation_tx.publish(&observation()).unwrap();

        line.clear();
        reader.read_line(&mut line).unwrap();

        let event: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(event["event"], "measurement");
        assert_eq!(event["key"], "sample_temperature");
        assert_eq!(event["value"], 20.0);

        client.write_all(b"{\"id\":2,\"op\":\"ping\"}\n").unwrap();

        line.clear();
        reader.read_line(&mut line).unwrap();

        let response: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(response["id"], 2);
        assert_eq!(response["status"], "ok");

        client.shutdown(Shutdown::Write).unwrap();

        worker.join().unwrap().unwrap();

        assert!(command_rx.try_recv().is_err());
    }
}
