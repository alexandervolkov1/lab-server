use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, Sender};
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
        enqueue: impl FnOnce() -> Option<Receiver<Observation>>,
    ) -> Result<(), EnqueueError> {
        self.ensure_available(id)?;

        let receiver = enqueue().ok_or(EnqueueError::RuntimeUnavailable)?;

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
        enqueue: impl FnOnce() -> Option<Receiver<Result<(), String>>>,
    ) -> Result<(), EnqueueError> {
        self.ensure_available(id)?;

        let receiver = enqueue().ok_or(EnqueueError::RuntimeUnavailable)?;

        self.entries.push_back(Entry::Set {
            id,
            pending: Pending::new(id, deadline, receiver),
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

pub struct ClientSession {
    decoder: FrameDecoder,
    pub pending: PendingRequests,
    pub outbound: OutboundBuffer,
    terminal: bool,
}

impl ClientSession {
    pub fn new(max_pending: usize, max_output_bytes: usize) -> Self {
        Self {
            decoder: FrameDecoder::default(),
            pending: PendingRequests::new(max_pending),
            outbound: OutboundBuffer::new(max_output_bytes),
            terminal: false,
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

            return Err(io::Error::other(format!(
                "failed to queue TCP response: {error:?}"
            )));
        }

        Ok(())
    }

    fn queue_error(&mut self, id: u64, error: &str) -> io::Result<()> {
        let response = json!({
            "id": id,
            "status": "error",
            "error": error
        });

        self.queue_response(&response.to_string())
    }

    pub fn dispatch_set(
        &mut self,
        id: u64,
        key: String,
        value: Value,
        commands: &Sender<crate::runtime::DeviceCommand>,
        catalog: &HashMap<String, ParameterDescription>,
        now: Instant,
    ) -> io::Result<()> {
        if !catalog.contains_key(&key) {
            return self.queue_error(id, "unknown_key");
        }

        let result = self.pending.add_set(id, now + Duration::from_secs(10), || {
            crate::queue_set(commands, key, value)
        });

        match result {
            Ok(()) => Ok(()),

            Err(error) => {
                let code = match error {
                    EnqueueError::DuplicateId => "duplicate_id",
                    EnqueueError::Full => "too_many_pending",
                    EnqueueError::RuntimeUnavailable => "runtime_unavailable",
                };

                self.queue_error(id, code)
            }
        }
    }

    pub fn dispatch_get(
        &mut self,
        id: u64,
        key: String,
        commands: &Sender<crate::runtime::DeviceCommand>,
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
                };

                self.queue_error(id, code)
            }
        }
    }

    pub fn collect_ready(&mut self, now: Instant) -> io::Result<()> {
        for finished in self.pending.poll(now) {
            let response = completion_json(finished)?;

            self.queue_response(&response)?;
        }

        Ok(())
    }

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

        self.session.collect_ready(Instant::now())?;
        self.session.outbound.flush_to(&mut self.stream)?;

        Ok(IoStep {
            incoming,
            read_closed: self.read_closed,
        })
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

        assert_eq!(pending.add_read(1, deadline, || Some(read_rx)), Ok(()));
        assert_eq!(pending.add_set(2, deadline, || Some(set_rx)), Ok(()));

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

        assert_eq!(pending.add_read(7, deadline, || Some(read_rx)), Ok(()));

        let called = Cell::new(false);

        assert_eq!(
            pending.add_set(7, deadline, || {
                called.set(true);
                None
            }),
            Err(EnqueueError::DuplicateId)
        );

        assert!(!called.get());

        assert_eq!(
            pending.add_set(8, deadline, || {
                called.set(true);
                None
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
            pending.add_set(1, deadline, || None),
            Err(EnqueueError::RuntimeUnavailable)
        );

        assert!(pending.is_empty());

        let (_sender, receiver) = mpsc::channel();

        assert_eq!(pending.add_set(1, deadline, || Some(receiver)), Ok(()));
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
            .add_read(1, now + Duration::from_secs(10), || Some(read_rx))
            .unwrap();

        pending.add_set(2, now, || Some(set_rx)).unwrap();

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

        client.write_all(b"{\"id\":8").unwrap();
        client.shutdown(Shutdown::Write).unwrap();

        let step = wait_for_step(&mut connection, |step| step.read_closed);

        assert!(step.incoming.is_empty());

        assert_eq!(
            connection.session.finish(),
            Err(FrameError::IncompleteFrame)
        );
    }

    #[test]
    fn tcp_preserves_request_before_terminal_framing_error() {
        let (mut connection, mut client) = loopback_pair();

        client.write_all(b"{\"id\":1,\"op\":\"ping\"}\n\n").unwrap();

        let step = wait_for_step(&mut connection, |step| !step.incoming.is_empty());

        assert_eq!(
            step.incoming,
            vec![
                Incoming::Request(TcpRequest::Ping { id: 1 }),
                Incoming::FramingError(FrameError::EmptyFrame),
            ]
        );

        assert!(connection.session.is_terminal());
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
            .add_read(7, deadline, || Some(read_rx))
            .unwrap();

        session
            .pending
            .add_set(8, deadline, || Some(set_rx))
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
            .add_read(1, now + Duration::from_secs(10), || Some(read_rx))
            .unwrap();

        session.pending.add_set(2, now, || Some(set_rx)).unwrap();

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
            .add_set(13, now + Duration::from_secs(10), || Some(reply_rx))
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
            .add_set(1, now + Duration::from_secs(10), || Some(reply_rx))
            .unwrap();

        reply_tx.send(Ok(())).unwrap();

        assert!(session.collect_ready(now).is_err());
        assert!(session.is_terminal());
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
    fn dispatch_set_rejects_requests_before_device_queue() {
        let now = Instant::now();
        let catalog = power_catalog();

        let mut session = ClientSession::new(1, 512);
        let (command_tx, command_rx) = mpsc::channel();

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

        let (command_tx, command_rx) = mpsc::channel();

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
        let (command_tx, command_rx) = mpsc::channel();

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
        let (command_tx, command_rx) = mpsc::channel();

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
        let (command_tx, _command_rx) = mpsc::channel();

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

        assert_eq!(responses[1]["id"], 7);
        assert_eq!(responses[1]["error"], "duplicate_id");

        assert_eq!(session.pending.len(), 1);
    }
}
