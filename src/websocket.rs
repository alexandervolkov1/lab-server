//! Synchronous, bounded WebSocket transport for the existing Session/DeviceService.
//! No TLS, authentication, proxy assumptions or second command engine.
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tungstenite::handshake::HandshakeError;
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::{HeaderValue, StatusCode};
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::{Error, Message, WebSocket};

use crate::fanout::ObservationReceiver;
use crate::service::DeviceService;
use crate::session::Session;

pub const PATH: &str = "/application/v1";
pub const SUBPROTOCOL: &str = "lab-server.application.v1";
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024;
pub const MAX_FRAME_BYTES: usize = 16 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024;
// Largest Session text + frame header + reserved room for automatic control frames.
pub const MAX_WRITE_BUFFER_BYTES: usize = MAX_OUTPUT_BYTES + 1024;
pub const MAX_UPGRADE_BYTES: usize = 16 * 1024;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_WAIT: Duration = Duration::from_millis(2);
const IO_CALLS_PER_TURN: usize = 64;
const MESSAGES_PER_TURN: usize = 16;

fn config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(4096)
        .write_buffer_size(0)
        .max_write_buffer_size(MAX_WRITE_BUFFER_BYTES)
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES))
}

// A read() in tungstenite can consume many continuation frames before returning
// a message. Bound underlying calls too, including empty-frame floods, so every
// connection returns to the stop/deadline check. No blocking socket operations.
struct BoundedIo<S> {
    inner: S,
    calls: usize,
    upgrade_left: Option<usize>,
    deadline: Option<Instant>,
}

impl<S> BoundedIo<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            calls: IO_CALLS_PER_TURN,
            upgrade_left: None,
            deadline: None,
        }
    }

    fn turn(&mut self) {
        self.calls = IO_CALLS_PER_TURN;
    }

    fn allow(&mut self) -> io::Result<()> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(io::ErrorKind::TimedOut.into());
        }
        if self.calls == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.calls -= 1;
        Ok(())
    }
}

impl<S: Read> Read for BoundedIo<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.allow()?;
        let limit = buf
            .len()
            .min(4096)
            .min(self.upgrade_left.unwrap_or(usize::MAX));
        if limit == 0 && !buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebSocket Upgrade exceeds 16 KiB",
            ));
        }
        let count = self.inner.read(&mut buf[..limit])?;
        if let Some(left) = &mut self.upgrade_left {
            *left -= count;
        }
        Ok(count)
    }
}

impl<S: Write> Write for BoundedIo<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.allow()?;
        self.inner.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.allow()?;
        self.inner.flush()
    }
}

fn reject(status: StatusCode, message: &str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(message.into()));
    *response.status_mut() = status;
    response
}

#[allow(clippy::result_large_err)] // tungstenite's callback signature
fn upgrade(
    request: &Request,
    mut response: Response,
    origins: &[String],
) -> Result<Response, ErrorResponse> {
    if request.uri().path_and_query().map(|path| path.as_str()) != Some(PATH) {
        return Err(reject(StatusCode::NOT_FOUND, "unknown WebSocket endpoint"));
    }
    let protocols = request.headers().get_all("sec-websocket-protocol");
    let supported = protocols.iter().any(|value| {
        value.to_str().is_ok_and(|text| {
            text.split(',')
                .any(|protocol| protocol.trim() == SUBPROTOCOL)
        })
    });
    if !supported {
        return Err(reject(
            StatusCode::BAD_REQUEST,
            "required subprotocol: lab-server.application.v1",
        ));
    }
    let mut supplied = request.headers().get_all("origin").iter();
    if let Some(origin) = supplied.next()
        && (supplied.next().is_some()
            || !origin
                .to_str()
                .is_ok_and(|text| origins.iter().any(|allowed| allowed == text)))
    {
        return Err(reject(StatusCode::FORBIDDEN, "Origin is not allowed"));
    }
    response.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_static(SUBPROTOCOL),
    );
    Ok(response)
}

fn blocked(error: &Error) -> bool {
    matches!(error, Error::Io(error) if error.kind() == io::ErrorKind::WouldBlock)
}

fn closed(error: &Error) -> bool {
    matches!(error, Error::ConnectionClosed | Error::AlreadyClosed)
}

/// The caller owns a ClientSlots permit, also during HTTP Upgrade. One thread
/// handles this entire connection; the shared service never performs socket I/O.
pub fn serve_connection(
    stream: TcpStream,
    service: &DeviceService,
    origins: &[String],
    stop: &AtomicBool,
) -> io::Result<()> {
    stream.set_nonblocking(true)?;
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut io = BoundedIo::new(stream);
    io.upgrade_left = Some(MAX_UPGRADE_BYTES);
    io.deadline = Some(deadline);
    #[allow(clippy::result_large_err)] // required by tungstenite's Callback
    let callback = |request: &Request, response: Response| upgrade(request, response, origins);
    let mut attempt = tungstenite::accept_hdr_with_config(io, callback, Some(config()));
    let mut socket = loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        match attempt {
            Ok(socket) => break socket,
            Err(HandshakeError::Failure(error)) => return Err(io::Error::other(error)),
            Err(HandshakeError::Interrupted(mut handshake)) => {
                if Instant::now() >= deadline {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                std::thread::sleep(IDLE_WAIT);
                handshake.get_mut().get_mut().turn();
                attempt = handshake.handshake();
            }
        }
    };
    socket.get_mut().upgrade_left = None;
    socket.get_mut().deadline = None;
    // Allocate an application session/receiver only after a validated Upgrade.
    let mut connection = Connection::new(socket, service.register_client()?);
    loop {
        if connection.step(service, stop.load(Ordering::Acquire), Instant::now())? {
            return Ok(());
        }
        std::thread::sleep(IDLE_WAIT);
    }
}

struct Connection<S> {
    socket: WebSocket<BoundedIo<S>>,
    session: Session,
    observations: ObservationReceiver,
    // Only WriteBufferFull returns ownership. WouldBlock means tungstenite has
    // already retained the frame: retry flush, NEVER re-enqueue that message.
    retry: Option<Message>,
    sending_since: Option<Instant>,
    finish_by: Option<Instant>,
    closing: bool,
}

impl<S: Read + Write> Connection<S> {
    fn new(socket: WebSocket<BoundedIo<S>>, observations: ObservationReceiver) -> Self {
        let mut session = Session::new(64, MAX_OUTPUT_BYTES);
        session.bind_observations(&observations);
        Self {
            socket,
            session,
            observations,
            retry: None,
            sending_since: None,
            finish_by: None,
            closing: false,
        }
    }

    fn begin_close(&mut self, code: CloseCode, now: Instant) -> io::Result<bool> {
        self.finish_by.get_or_insert(now + CLOSE_TIMEOUT);
        self.closing = true;
        self.session.close_input();
        match self.socket.close(Some(CloseFrame {
            code,
            reason: "".into(),
        })) {
            Ok(()) => Ok(false),
            Err(error) if blocked(&error) => Ok(false),
            Err(error) if closed(&error) => Ok(true),
            Err(error) => Err(io::Error::other(error)),
        }
    }

    // Flush all prior bytes before taking another Session message. Retry owns
    // at most ONE rejected frame, while tungstenite's buffer has a hard cap.
    fn send_one(&mut self, now: Instant) -> io::Result<bool> {
        match self.socket.flush() {
            Ok(()) => {}
            Err(error) if blocked(&error) => {
                self.sending_since.get_or_insert(now);
                return Ok(false);
            }
            Err(error) if closed(&error) => return Ok(true),
            Err(error) => return Err(io::Error::other(error)),
        }
        if self.closing {
            return Ok(false);
        }
        if self.retry.is_none() {
            self.sending_since = None;
            self.retry = self
                .session
                .next_message()?
                .map(|text| Message::Text(text.into()));
        }
        if let Some(message) = self.retry.take() {
            self.sending_since.get_or_insert(now);
            match self.socket.write(message) {
                Ok(()) => {}
                Err(Error::WriteBufferFull(message)) => self.retry = Some(*message),
                Err(error) if blocked(&error) => {}
                Err(error) if closed(&error) => return Ok(true),
                Err(error) => return Err(io::Error::other(error)),
            }
        }
        Ok(false)
    }

    fn step(&mut self, service: &DeviceService, stop: bool, now: Instant) -> io::Result<bool> {
        self.socket.get_mut().turn();
        if self.finish_by.is_some_and(|deadline| now >= deadline) {
            return Ok(true);
        }
        if stop && !self.closing && self.begin_close(CloseCode::Away, now)? {
            return Ok(true);
        }
        if self
            .sending_since
            .is_some_and(|since| now.saturating_duration_since(since) >= WRITE_TIMEOUT)
            && !self.closing
            && self.begin_close(CloseCode::Again, now)?
        {
            return Ok(true);
        }
        if !self.closing
            && !self.session.is_terminal()
            && let Err(error) = self.session.collect_ready(now)
            && !self.session.is_terminal()
        {
            return Err(error);
        }
        for _ in 0..MESSAGES_PER_TURN {
            if self.session.is_terminal() && !self.closing {
                break;
            }
            match self.socket.read() {
                Ok(Message::Text(text)) if !self.closing => {
                    if let Err(error) = service.receive_message(&mut self.session, &text, now)
                        && !self.session.is_terminal()
                    {
                        return Err(error);
                    }
                }
                Ok(Message::Binary(_)) if !self.closing => {
                    if self.begin_close(CloseCode::Unsupported, now)? {
                        return Ok(true);
                    }
                    break;
                }
                Ok(Message::Close(_)) => {
                    self.session.close_input();
                    self.closing = true;
                    self.finish_by.get_or_insert(now + CLOSE_TIMEOUT);
                    break; // tungstenite already queued the matching Close
                }
                Ok(_) => {} // automatic Pong/Close, no application interpretation
                Err(error) if blocked(&error) => break,
                Err(error) if closed(&error) => return Ok(true),
                Err(Error::Capacity(_)) => {
                    if self.begin_close(CloseCode::Size, now)? {
                        return Ok(true);
                    }
                    break;
                }
                Err(Error::Protocol(_)) => {
                    if self.begin_close(CloseCode::Protocol, now)? {
                        return Ok(true);
                    }
                    break;
                }
                Err(Error::Utf8(_)) => {
                    if self.begin_close(CloseCode::Invalid, now)? {
                        return Ok(true);
                    }
                    break;
                }
                Err(error) => return Err(io::Error::other(error)),
            }
        }
        if !self.closing {
            self.session.collect_observations(&self.observations)?;
            if self.session.is_terminal() {
                self.finish_by.get_or_insert(now + CLOSE_TIMEOUT);
            }
        }
        if self.send_one(now)? {
            return Ok(true);
        }
        if !self.closing
            && self.session.is_finished()
            && self.retry.is_none()
            && self.sending_since.is_none()
        {
            return self.begin_close(CloseCode::Normal, now);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
