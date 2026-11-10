//! Exercise the public message boundary without a socket, framing or hardware.
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant, UNIX_EPOCH};

use lab_server::connection::ClientSession;
use lab_server::fanout::ObservationFanout;
use lab_server::model::{Observation, ParameterDescription, Spec, Value};
use lab_server::outbound::{OutboundBuffer, OutputError};
use lab_server::runtime::DeviceCommand;
use lab_server::session::Session;
use serde_json::{Value as Json, json};

struct Context {
    commands: mpsc::SyncSender<DeviceCommand>,
    receiver: mpsc::Receiver<DeviceCommand>,
    catalog: HashMap<String, ParameterDescription>,
    latest: Mutex<HashMap<String, Observation>>,
}

impl Context {
    fn new() -> Self {
        let (commands, receiver) = mpsc::sync_channel(8);
        let catalog = [
            ("power", Spec::Integer { min: 0, max: 100 }, true, None),
            (
                "temperature",
                Spec::Float {
                    min: -100.0,
                    max: 500.0,
                },
                false,
                Some(Duration::from_secs(1)),
            ),
        ]
        .into_iter()
        .map(|(id, spec, writable, default_poll_period)| {
            (
                id.into(),
                ParameterDescription {
                    id: id.into(),
                    spec,
                    writable,
                    default_poll_period,
                    unit: "".into(),
                },
            )
        })
        .collect();
        Self {
            commands,
            receiver,
            catalog,
            latest: Mutex::new(HashMap::new()),
        }
    }

    fn send(&self, session: &mut Session, text: &str, now: Instant) {
        session
            .receive_message(text, &self.commands, &self.catalog, &self.latest, now)
            .unwrap();
    }
}

fn observation(value: f64) -> Observation {
    Observation {
        source: "temperature".into(),
        unit: "".into(),
        completed_at: UNIX_EPOCH,
        reading: Ok(Value::Float(value)),
    }
}

fn messages(session: &mut Session) -> Vec<Json> {
    let mut result = Vec::new();
    while let Some(text) = session.next_message().unwrap() {
        assert!(!text.contains('\n'));
        result.push(serde_json::from_str(&text).unwrap());
    }
    result
}

#[test]
fn complete_json_text_has_no_ndjson_dependency_and_uses_the_existing_parser() {
    let context = Context::new();
    let mut session = Session::new(64, 64 * 1024);
    let mut tcp = ClientSession::new(64, 64 * 1024);
    let now = Instant::now();
    for text in [
        r#"{"id":1,"op":"hello"}"#,
        r#"{"id":2,"op":"describe"}"#,
        "not-json",
        "[]",
        r#"{"id":0,"op":"ping"}"#,
        r#"{"id":3,"op":"get"}"#,
        r#"{"id":3,"op":"ping"}"#,
        r#"{"id":4,"id":5,"op":"ping"}"#,
        r#"{"id":4,"op":"ping"} {}"#,
        r#"{"id":4,"op":"get","key":"температура"}"#,
        r#"{"id":5,"op":"get","key":"temperature"}"#,
    ] {
        context.send(&mut session, text, now);
        for incoming in tcp.receive(format!("{text}\r\n").as_bytes()) {
            tcp.dispatch_incoming(
                incoming,
                &context.commands,
                &context.catalog,
                &context.latest,
                now,
            )
            .unwrap();
        }
        let direct = session.next_message().unwrap().unwrap();
        let mut wire = Vec::new();
        tcp.outbound.flush_to(&mut wire).unwrap();
        assert_eq!(wire, format!("{direct}\n").as_bytes());
        assert!(session.next_message().unwrap().is_none());
    }
    context.send(&mut session, "{\n \"id\":6,\n \"op\":\"ping\"\n}", now);
    assert_eq!(messages(&mut session), [json!({"id":6,"status":"ok"})]);
    assert!(context.receiver.try_recv().is_err());
}

#[test]
fn message_queue_keeps_utf8_boundaries_and_the_exact_64k_lf_budget() {
    let mut queue = OutboundBuffer::new(64 * 1024);
    let text = format!("\"{}\"", "я".repeat(32766)); // 65534 UTF-8 bytes
    assert_eq!(text.len(), 65534);
    queue.enqueue_message(&text).unwrap();
    assert_eq!(queue.queued_bytes(), 65535);
    assert_eq!(queue.enqueue_message("0"), Err(OutputError::Full));
    assert_eq!(queue.pop_message().unwrap(), Some(text));
    assert_eq!(queue.queued_bytes(), 0);
    queue.enqueue_message(&"x".repeat(65535)).unwrap();
    assert_eq!(queue.queued_bytes(), 65536);
    assert_eq!(queue.enqueue_message("0"), Err(OutputError::Full));
    assert_eq!(queue.pop_message().unwrap().unwrap().len(), 65535);
    assert!(queue.pop_message().unwrap().is_none());
    assert_eq!(
        queue.enqueue_message(&"x".repeat(65536)),
        Err(OutputError::Full)
    );
    assert!(queue.is_empty());
}

#[test]
fn tcp_compatibility_framing_keeps_valid_prefix_before_terminal_error() {
    let context = Context::new();
    let now = Instant::now();
    for (tail, code) in [
        (b"\n".to_vec(), "empty_frame"),
        (vec![0xff, b'\n'], "invalid_utf8"),
        (vec![b'x'; 16 * 1024 + 1], "frame_too_long"),
    ] {
        let prefix = "{\"id\":1,\"op\":\"get\",\"key\":\"я\"}\r\n{\"id\":2,\"op\":\"ping\"}\n";
        let mut input = prefix.as_bytes().to_vec();
        input.extend(tail);
        let mut outputs = Vec::new();
        for one_byte_reads in [false, true] {
            let mut session = ClientSession::new(64, 4096);
            let mut events = Vec::new();
            if one_byte_reads {
                for byte in &input {
                    events.extend(session.receive(&[*byte]));
                }
            } else {
                events.extend(session.receive(&input));
            }
            for event in events {
                session
                    .dispatch_incoming(
                        event,
                        &context.commands,
                        &context.catalog,
                        &context.latest,
                        now,
                    )
                    .unwrap();
            }
            assert!(session.is_terminal());
            assert!(session.receive(b"{\"id\":3,\"op\":\"ping\"}\n").is_empty());
            let mut wire = Vec::new();
            session.outbound.flush_to(&mut wire).unwrap();
            outputs.push(String::from_utf8(wire).unwrap());
        }
        assert_eq!(outputs[0], outputs[1]);
        let replies: Vec<Json> = outputs[0]
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(replies.len(), 3);
        assert_eq!(replies[0]["error"], "unknown_key");
        assert_eq!(replies[1], json!({"id":2,"status":"ok"}));
        assert_eq!(replies[2], json!({"status":"error","error":code}));
    }
}

struct BudgetWriter {
    budget: usize,
    bytes: Vec<u8>,
}

impl Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.budget == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = self.budget.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..n]);
        self.budget -= n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn tcp_partial_writes_release_only_written_bytes_and_cannot_replay_a_prefix() {
    let mut queue = OutboundBuffer::new(8);
    queue.enqueue_message("\"я\"").unwrap(); // 4 payload bytes + LF
    let mut writer = BudgetWriter {
        budget: 2,
        bytes: Vec::new(),
    };
    queue.flush_to(&mut writer).unwrap(); // cut inside UTF-8, not a message boundary
    assert_eq!(queue.queued_bytes(), 3);
    assert_eq!(
        queue.pop_message().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    queue.enqueue_message("null").unwrap(); // capacity reclaimed by partial write
    assert_eq!(queue.queued_bytes(), 8);
    assert_eq!(queue.enqueue_message("0"), Err(OutputError::Full));
    writer.budget = 2;
    queue.flush_to(&mut writer).unwrap(); // body complete, LF still pending
    assert_eq!(queue.queued_bytes(), 6);
    assert!(queue.pop_message().is_err());
    writer.budget = 1;
    queue.flush_to(&mut writer).unwrap();
    assert_eq!(writer.bytes, "\"я\"\n".as_bytes());
    assert_eq!(queue.queued_bytes(), 5);
    writer.budget = 5;
    queue.flush_to(&mut writer).unwrap();
    assert_eq!(writer.bytes, "\"я\"\nnull\n".as_bytes());
    assert!(queue.is_empty());
    assert_eq!(queue.queued_bytes(), 0);
    assert!(queue.pop_message().unwrap().is_none());
}

#[test]
fn output_overflow_preserves_queued_prefix_and_terminates_message_session() {
    let context = Context::new();
    let mut session = Session::new(64, 23); // exactly one ping reply + LF
    context.send(&mut session, r#"{"id":1,"op":"ping"}"#, Instant::now());
    let error = session
        .receive_message(
            r#"{"id":2,"op":"ping"}"#,
            &context.commands,
            &context.catalog,
            &context.latest,
            Instant::now(),
        )
        .unwrap_err();
    assert_eq!(
        error.get_ref().unwrap().downcast_ref::<OutputError>(),
        Some(&OutputError::Full)
    );
    assert!(session.is_terminal());
    assert!(!session.is_finished());
    context.send(&mut session, r#"{"id":3,"op":"ping"}"#, Instant::now());
    assert_eq!(messages(&mut session), [json!({"id":1,"status":"ok"})]);
    assert!(session.is_finished());
}

#[test]
fn concurrent_message_session_orders_subscription_ack_completion_and_measurement() {
    let context = Context::new();
    let fanout = ObservationFanout::new(8);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"set","key":"power","value":30}"#,
        now,
    );
    let DeviceCommand::Set(command) = context.receiver.recv().unwrap() else {
        panic!("expected set")
    };
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        assert!(command.execution.try_start());
        started_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        command
            .execution
            .complete(|| command.reply.send(Ok(())).unwrap());
    });
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    fanout.publish(&observation(-1.0)).unwrap(); // before activation, excluded
    context.send(
        &mut session,
        r#"{"id":2,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    context.send(&mut session, r#"{"id":3,"op":"ping"}"#, now);
    fanout.publish(&observation(25.0)).unwrap();
    release_tx.send(()).unwrap();
    worker.join().unwrap(); // completion is known to be ready, no timing race
    session.collect_ready(now).unwrap();
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 4);
    assert_eq!(
        output[0],
        json!({"id":2,"status":"ok","keys":["temperature"]})
    );
    assert_eq!(output[1], json!({"id":3,"status":"ok"}));
    assert_eq!(output[2], json!({"id":1,"status":"ok"}));
    assert_eq!(output[3]["event"], "measurement");
    assert_eq!(output[3]["value"], 25.0);
    session.collect_ready(now).unwrap();
    assert!(messages(&mut session).is_empty());
}

#[test]
fn unsubscribe_and_reactivation_exclude_queued_old_publications() {
    let context = Context::new();
    let fanout = ObservationFanout::new(8);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    fanout.publish(&observation(1.0)).unwrap();
    context.send(
        &mut session,
        r#"{"id":2,"op":"unsubscribe","keys":["temperature"]}"#,
        now,
    );
    session.collect_observations(&receiver).unwrap();
    fanout.publish(&observation(2.0)).unwrap();
    context.send(
        &mut session,
        r#"{"id":3,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    fanout.publish(&observation(3.0)).unwrap();
    // Idempotent subscribe must not move the retained key's boundary past 3.0.
    context.send(
        &mut session,
        r#"{"id":4,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 5);
    assert_eq!(output[4]["value"], 3.0);
    assert!(
        output[..4]
            .iter()
            .all(|reply| reply["status"] == "ok" && reply.get("event").is_none())
    );
}

#[test]
fn message_receiver_overflow_is_sticky_across_resubscription() {
    let context = Context::new();
    let fanout = ObservationFanout::new(1);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    fanout.publish(&observation(1.0)).unwrap();
    fanout.publish(&observation(2.0)).unwrap();
    context.send(
        &mut session,
        r#"{"id":2,"op":"unsubscribe","keys":["temperature"]}"#,
        now,
    );
    context.send(
        &mut session,
        r#"{"id":3,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 4);
    assert_eq!(
        output[3],
        json!({"status":"error","error":"observation_data_lost"})
    );
    assert!(session.is_finished());
    session.collect_observations(&receiver).unwrap();
    assert!(messages(&mut session).is_empty());
}

#[test]
fn orderly_observation_close_drains_prefix_and_differs_from_data_loss() {
    let context = Context::new();
    let fanout = ObservationFanout::new(8);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["temperature"]}"#,
        Instant::now(),
    );
    fanout.publish(&observation(25.0)).unwrap();
    drop(fanout);
    session.collect_observations(&receiver).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 3);
    assert_eq!(output[1]["value"], 25.0);
    assert_eq!(output[2]["error"], "observation_stream_closed");
    assert!(session.is_finished());
}

#[test]
fn observation_service_is_bounded_to_one_turn() {
    let context = Context::new();
    let fanout = ObservationFanout::new(64);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 64 * 1024);
    session.bind_observations(&receiver);
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["temperature"]}"#,
        Instant::now(),
    );
    messages(&mut session);
    for n in 0..40 {
        fanout.publish(&observation(f64::from(n))).unwrap();
    }
    session.collect_observations(&receiver).unwrap();
    let first = messages(&mut session);
    assert_eq!(first.len(), 32);
    session.collect_observations(&receiver).unwrap();
    let tail = messages(&mut session);
    assert_eq!(tail.len(), 8);
    assert_eq!(first[31]["value"], 31.0);
    assert_eq!(tail[0]["value"], 32.0);
}

#[test]
fn concurrent_message_eof_preserves_queued_cancellation_and_started_outcome_unknown() {
    let context = Context::new();
    let mut session = Session::new(8, 4096);
    let now = Instant::now();
    for id in 1..=2 {
        context.send(
            &mut session,
            &json!({"id":id,"op":"set","key":"power","value":30}).to_string(),
            now,
        );
    }
    let DeviceCommand::Set(queued) = context.receiver.recv().unwrap() else {
        panic!("set")
    };
    let DeviceCommand::Set(started) = context.receiver.recv().unwrap() else {
        panic!("set")
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        assert!(started.execution.try_start());
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        started
            .execution
            .complete(|| assert!(started.reply.send(Ok(())).is_err()));
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    session.close_input();
    assert!(!session.is_finished());
    context.send(&mut session, r#"{"id":3,"op":"ping"}"#, now);
    session
        .collect_ready(now + Duration::from_secs(11))
        .unwrap();
    assert!(!queued.execution.try_start());
    assert_eq!(
        messages(&mut session),
        [
            json!({"id":1,"status":"error","error":"runtime_timeout"}),
            json!({"id":2,"status":"error","error":"outcome_unknown"}),
        ]
    );
    assert!(session.is_finished());
    release_tx.send(()).unwrap();
    worker.join().unwrap();
    session
        .collect_ready(now + Duration::from_secs(12))
        .unwrap();
    assert!(messages(&mut session).is_empty());
}

#[test]
fn clean_message_eof_keeps_final_ack_and_stops_observation_admission() {
    let context = Context::new();
    let fanout = ObservationFanout::new(8);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    context.send(
        &mut session,
        r#"{"id":2,"op":"set","key":"power","value":30}"#,
        now,
    );
    let DeviceCommand::Set(command) = context.receiver.recv().unwrap() else {
        panic!("set")
    };
    assert!(command.execution.try_start());
    session.close_input();
    fanout.publish(&observation(25.0)).unwrap();
    session.collect_observations(&receiver).unwrap();
    command
        .execution
        .complete(|| command.reply.send(Ok(())).unwrap());
    session.collect_ready(now).unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 2);
    assert_eq!(output[1], json!({"id":2,"status":"ok"}));
    assert!(session.is_finished());
}

#[test]
fn terminal_loss_does_not_fabricate_pending_ack_and_drop_releases_reply_receiver() {
    let context = Context::new();
    let fanout = ObservationFanout::new(1);
    let receiver = fanout.register().unwrap();
    let mut session = Session::new(8, 4096);
    session.bind_observations(&receiver);
    let now = Instant::now();
    context.send(
        &mut session,
        r#"{"id":1,"op":"set","key":"power","value":30}"#,
        now,
    );
    let DeviceCommand::Set(command) = context.receiver.recv().unwrap() else {
        panic!("set")
    };
    assert!(command.execution.try_start());
    context.send(
        &mut session,
        r#"{"id":2,"op":"subscribe","keys":["temperature"]}"#,
        now,
    );
    fanout
        .loss_epoch()
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    session.collect_observations(&receiver).unwrap();
    session
        .collect_ready(now + Duration::from_secs(11))
        .unwrap();
    let output = messages(&mut session);
    assert_eq!(output.len(), 2);
    assert_eq!(output[1]["error"], "observation_data_lost");
    assert!(session.is_finished());
    drop(session);
    command
        .execution
        .complete(|| assert!(command.reply.send(Ok(())).is_err()));
}
