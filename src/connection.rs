//! NDJSON framing, partial writes and socket scheduling for the shared session.
use std::collections::HashMap;
use std::io::{self, IoSlice, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::model::{Observation, ParameterDescription};
use crate::outbound::is_output_error;
use crate::protocol::FrameDecoder;
use crate::protocol::{FrameError, ParseError, TcpRequest};
use crate::session::{ParsedMessage, SessionState};
use serde_json::json;
// Preserve the existing library paths, field access and method signatures.
pub use crate::outbound::{OutboundBuffer, OutputError};
pub use crate::session::{
    CommandBackend, EnqueueError, FinishedRequest, PendingRequests, SubscriptionChange,
    SubscriptionError, SubscriptionSet,
};

#[derive(Debug, PartialEq)]
pub enum Incoming {
    Request(TcpRequest),
    InvalidRequest { frame: String, error: ParseError },
    FramingError(FrameError),
}

impl Incoming {
    fn parse(text: &str) -> Self {
        match ParsedMessage::parse(text) {
            ParsedMessage::Request(request) => Self::Request(request),
            ParsedMessage::InvalidRequest { frame, error } => Self::InvalidRequest { frame, error },
        }
    }
}

#[derive(Default)]
pub struct NdjsonInput(FrameDecoder);

/// Compatibility session with the existing receive(bytes) NDJSON interface.
/// New message transports use session::Session instead, with the same core.
pub type ClientSession = SessionState<NdjsonInput>;

impl ClientSession {
    pub fn receive(&mut self, bytes: &[u8]) -> Vec<Incoming> {
        if self.terminal {
            return Vec::new();
        }

        let (frames, framing_error) = match self.input.0.push(bytes) {
            Ok(frames) => (frames, None),

            Err(failure) => (failure.frames, Some(failure.error)),
        };

        let mut incoming = Vec::new();

        incoming.extend(frames.iter().map(|frame| Incoming::parse(frame)));

        if let Some(error) = framing_error {
            self.terminal = true;
            incoming.push(Incoming::FramingError(error));
        }

        incoming
    }

    /// Compatibility entry point, including TCP-only framing failures. Parsed
    /// requests and JSON errors always enter the one shared dispatcher.
    pub fn dispatch_incoming(
        &mut self,
        incoming: Incoming,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        now: Instant,
    ) -> io::Result<Option<Incoming>> {
        let message = match incoming {
            Incoming::Request(request) => ParsedMessage::Request(request),
            Incoming::InvalidRequest { frame, error } => {
                ParsedMessage::InvalidRequest { frame, error }
            }
            Incoming::FramingError(error) => {
                self.terminal = true;
                let code = match error {
                    FrameError::EmptyFrame => "empty_frame",
                    FrameError::FrameTooLong => "frame_too_long",
                    FrameError::InvalidUtf8 => "invalid_utf8",
                    FrameError::IncompleteFrame => "incomplete_frame",
                };
                self.queue_response(&json!({"status":"error", "error":code}).to_string())?;
                return Ok(None);
            }
        };
        self.dispatch_message(message, commands, catalog, latest, now)?;
        Ok(None)
    }

    #[cfg(test)]
    pub fn finish(self) -> Result<(), crate::protocol::FrameError> {
        self.input.0.finish()
    }
}

impl OutboundBuffer {
    /// Compatibility name; JSON serialization itself belongs to the session.
    pub fn enqueue_line(&mut self, line: &str) -> Result<(), OutputError> {
        self.enqueue_message(line)
    }

    /// Encode complete messages as NDJSON; release capacity for exactly the
    /// bytes written, including LF. A blocked write retains its front cursor.
    pub fn flush_to<W: Write>(&mut self, writer: &mut W) -> io::Result<()> {
        while let Some(message) = self.messages.front() {
            let chunks = [
                IoSlice::new(&message.as_bytes()[self.front_offset..]),
                IoSlice::new(b"\n"),
            ];
            match writer.write_vectored(&chunks) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "TCP writer returned zero bytes",
                    ));
                }
                Ok(written) => {
                    self.front_offset += written;
                    self.remaining_bytes -= written;
                    if self.front_offset == message.len() + 1 {
                        self.messages.pop_front();
                        self.front_offset = 0;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
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
                        self.session.close_input();

                        if let Err(error) = self.session.input.0.finish() {
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
        commands: &impl CommandBackend,
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

        Ok(self.session.is_finished())
    }

    pub fn drive_once_with_observations(
        &mut self,
        commands: &impl CommandBackend,
        catalog: &HashMap<String, ParameterDescription>,
        latest: &Mutex<HashMap<String, Observation>>,
        observations: &crate::fanout::ObservationReceiver,
    ) -> io::Result<bool> {
        self.session.bind_observations(observations);
        let finished = self.drive_once(commands, catalog, latest)?;

        if finished || self.read_closed || self.session.is_terminal() {
            return Ok(finished);
        }

        self.session.collect_observations(observations)?;

        self.session.outbound.flush_to(&mut self.stream)?;

        Ok(self.session.is_terminal() && self.session.outbound.is_empty())
    }

    pub fn run(
        &mut self,
        commands: &impl CommandBackend,
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
