use super::*;
use crate::command_router::CommandRouter;
use crate::device_registry::DeviceRegistry;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::Role;

struct GatedSocket {
    socket: TcpStream,
    allowance: Arc<AtomicUsize>,
}
impl Read for GatedSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.socket.read(buf)
    }
}
impl Write for GatedSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let limit = self.allowance.load(Ordering::SeqCst).min(buf.len());
        if limit == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = self.socket.write(&buf[..limit])?;
        self.allowance.fetch_sub(n, Ordering::SeqCst);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn pair(
    service: &DeviceService,
) -> (
    Connection<GatedSocket>,
    WebSocket<TcpStream>,
    Arc<AtomicUsize>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let client = std::thread::spawn(move || {
        let stream = TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut request = format!("ws://{address}{PATH}")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
        tungstenite::client(request, stream).unwrap().0
    });
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let allowance = Arc::new(AtomicUsize::new(usize::MAX));
    let io = BoundedIo::new(GatedSocket {
        socket: stream,
        allowance: allowance.clone(),
    });
    #[allow(clippy::result_large_err)] // required by tungstenite's Callback
    let callback = |request: &Request, response: Response| upgrade(request, response, &[]);
    let socket = tungstenite::accept_hdr_with_config(io, callback, Some(config())).unwrap();
    socket.get_ref().inner.socket.set_nonblocking(true).unwrap();
    (
        Connection::new(socket, service.register_client().unwrap()),
        client.join().unwrap(),
        allowance,
    )
}
fn empty_service() -> DeviceService {
    DeviceService::new(CommandRouter::new(DeviceRegistry::new()), 8)
}
fn json_id(client: &mut WebSocket<TcpStream>) -> u64 {
    let Message::Text(text) = client.read().unwrap() else {
        panic!("expected text")
    };
    serde_json::from_str::<serde_json::Value>(&text).unwrap()["id"]
        .as_u64()
        .unwrap()
}
fn admit_two(
    connection: &mut Connection<GatedSocket>,
    client: &mut WebSocket<TcpStream>,
    service: &DeviceService,
) {
    for id in 1..=2 {
        client
            .send(Message::Text(
                format!("{{\"id\":{id},\"op\":\"ping\"}}").into(),
            ))
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while connection.sending_since.is_none() || connection.session.outbound.queued_bytes() == 0 {
        assert!(Instant::now() < deadline);
        assert!(!connection.step(service, false, Instant::now()).unwrap());
        std::thread::yield_now();
    }
}

#[test]
fn concurrent_blocked_ws_retains_one_frame_and_resumes_partial_write_without_replay() {
    let service = empty_service();
    let (mut connection, mut client, allowance) = pair(&service);
    allowance.store(0, Ordering::SeqCst);
    admit_two(&mut connection, &mut client, &service);
    let queued = connection.session.outbound.queued_bytes();
    assert_eq!(queued, 23); // second ping stays inside the 64 KiB session budget
    assert!(connection.retry.is_none()); // WouldBlock is already owned by tungstenite
    for _ in 0..20 {
        connection.step(&service, false, Instant::now()).unwrap();
        assert_eq!(connection.session.outbound.queued_bytes(), queued);
    }
    // A separate real WS connection progresses while this socket cannot write.
    let (mut healthy, mut other, _) = pair(&service);
    other
        .send(Message::Text("{\"id\":7,\"op\":\"ping\"}".into()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while healthy.sending_since.is_none() {
        assert!(Instant::now() < deadline);
        healthy.step(&service, false, Instant::now()).unwrap();
    }
    assert_eq!(json_id(&mut other), 7);
    allowance.store(3, Ordering::SeqCst);
    connection.step(&service, false, Instant::now()).unwrap();
    assert_eq!(allowance.load(Ordering::SeqCst), 0);
    assert_eq!(connection.session.outbound.queued_bytes(), queued);
    allowance.store(usize::MAX, Ordering::SeqCst);
    connection.step(&service, false, Instant::now()).unwrap();
    connection.step(&service, false, Instant::now()).unwrap();
    assert_eq!(json_id(&mut client), 1);
    assert_eq!(json_id(&mut client), 2);
    assert!(connection.session.outbound.is_empty());
    assert!(connection.sending_since.is_none());
}

#[test]
fn concurrent_write_buffer_full_retries_only_returned_frame_and_keeps_fifo() {
    let service = empty_service();
    let (mut connection, mut client, _) = pair(&service);
    // Force the library's rejection branch: production allows any Session frame.
    connection
        .socket
        .set_config(|config| config.max_write_buffer_size = 23);
    admit_two(&mut connection, &mut client, &service);
    assert!(connection.retry.is_some());
    let since = connection.sending_since;
    for _ in 0..20 {
        connection.step(&service, false, Instant::now()).unwrap();
        assert_eq!(connection.session.outbound.queued_bytes(), 23);
        assert_eq!(connection.sending_since, since);
    }
    connection
        .socket
        .set_config(|config| config.max_write_buffer_size = MAX_WRITE_BUFFER_BYTES);
    for _ in 0..3 {
        connection.step(&service, false, Instant::now()).unwrap();
    }
    assert_eq!(json_id(&mut client), 1);
    assert_eq!(json_id(&mut client), 2);
    assert!(connection.retry.is_none());
    assert!(connection.sending_since.is_none());
}

#[test]
fn concurrent_write_and_close_deadlines_do_not_depend_on_peer_progress() {
    let service = empty_service();
    let (mut connection, mut client, allowance) = pair(&service);
    allowance.store(0, Ordering::SeqCst);
    admit_two(&mut connection, &mut client, &service);
    let now = connection.sending_since.unwrap() + WRITE_TIMEOUT;
    assert!(!connection.step(&service, false, now).unwrap());
    assert!(connection.closing);
    assert!(
        connection
            .step(&service, false, now + CLOSE_TIMEOUT)
            .unwrap()
    );
}

struct EmptyContinuations {
    first: bool,
    reads: usize,
}
impl Read for EmptyContinuations {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let frame = [u8::from(self.first), 0x80, 0, 0, 0, 0];
        assert!(buf.len() >= frame.len());
        buf[..frame.len()].copy_from_slice(&frame);
        self.first = false;
        self.reads += 1;
        Ok(frame.len())
    }
}
impl Write for EmptyContinuations {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn empty_continuation_flood_yields_to_stop_check_with_bounded_io() {
    let mut socket = WebSocket::from_raw_socket(
        BoundedIo::new(EmptyContinuations {
            first: true,
            reads: 0,
        }),
        Role::Server,
        Some(config()),
    );
    assert!(blocked(&socket.read().unwrap_err()));
    assert_eq!(socket.get_ref().inner.reads, IO_CALLS_PER_TURN);
    socket.get_mut().turn();
    assert!(blocked(&socket.read().unwrap_err()));
    assert_eq!(socket.get_ref().inner.reads, IO_CALLS_PER_TURN * 2);
}
