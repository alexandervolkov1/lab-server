use super::*;

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
