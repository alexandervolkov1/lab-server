use super::*;

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
    connection
        .session
        .dispatch_subscription(
            1,
            SubscriptionChange::Subscribe,
            vec!["sample_temperature".into()],
            &get_catalog(),
        )
        .unwrap();
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
        serde_json::from_str::<serde_json::Value>(text.lines().last().unwrap()).unwrap()["error"],
        "observation_data_lost"
    );
}

fn inject_observation_loss(fanout: &crate::fanout::ObservationFanout, upstream: bool) {
    if upstream {
        fanout
            .loss_epoch()
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    } else {
        fanout.publish(&observation()).unwrap();
        fanout.publish(&observation()).unwrap();
    }
}

#[test]
fn observation_loss_preserves_command_only_client_and_pending_set() {
    for upstream in [true, false] {
        let (mut connection, mut client) = loopback_pair();
        let fanout = crate::fanout::ObservationFanout::new(1);
        let observations = fanout.register().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let catalog = power_catalog();
        let latest = Mutex::new(HashMap::new());
        connection
            .session
            .dispatch_set(
                1,
                "power".into(),
                Value::Integer(20),
                &tx,
                &catalog,
                Instant::now(),
            )
            .unwrap();
        let crate::runtime::DeviceCommand::Set(command) = rx.try_recv().unwrap() else {
            panic!()
        };
        assert!(command.execution.try_start());
        inject_observation_loss(&fanout, upstream);
        assert!(
            !connection
                .drive_once_with_observations(&tx, &catalog, &latest, &observations)
                .unwrap()
        );
        assert!(!connection.session.is_terminal());
        assert_eq!(connection.session.pending.len(), 1);
        connection
            .session
            .dispatch_incoming(
                Incoming::Request(TcpRequest::Ping { id: 2 }),
                &tx,
                &catalog,
                &latest,
                Instant::now(),
            )
            .unwrap();
        command
            .execution
            .complete(|| command.reply.send(Ok(())).unwrap());
        for _ in 0..2 {
            assert!(
                !connection
                    .drive_once_with_observations(&tx, &catalog, &latest, &observations)
                    .unwrap()
            );
        }
        assert!(connection.session.pending.is_empty());
        drop(connection);
        let mut text = String::new();
        client.read_to_string(&mut text).unwrap();
        let messages: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], json!({"id":2,"status":"ok"}));
        assert_eq!(messages[1], json!({"id":1,"status":"ok"}));
    }
}

#[test]
fn loss_while_unsubscribed_does_not_poison_later_subscription() {
    for upstream in [true, false] {
        let (mut connection, mut client) = loopback_pair();
        connection.session.outbound = OutboundBuffer::new(4096);
        let fanout = crate::fanout::ObservationFanout::new(1);
        let observations = fanout.register().unwrap();
        let (tx, _rx) = mpsc::sync_channel(1);
        let catalog = get_catalog();
        let latest = Mutex::new(HashMap::new());
        connection
            .drive_once_with_observations(&tx, &catalog, &latest, &observations)
            .unwrap();
        let keys = vec!["sample_temperature".into()];
        connection
            .session
            .dispatch_subscription(1, SubscriptionChange::Subscribe, keys.clone(), &catalog)
            .unwrap();
        connection
            .session
            .dispatch_subscription(2, SubscriptionChange::Unsubscribe, keys.clone(), &catalog)
            .unwrap();
        inject_observation_loss(&fanout, upstream);
        // No connection turn between loss and activation: reset at subscribe itself.
        connection
            .session
            .dispatch_subscription(3, SubscriptionChange::Subscribe, keys, &catalog)
            .unwrap();
        assert!(
            !connection
                .drive_once_with_observations(&tx, &catalog, &latest, &observations)
                .unwrap()
        );
        fanout.publish(&observation()).unwrap();
        assert!(
            !connection
                .drive_once_with_observations(&tx, &catalog, &latest, &observations)
                .unwrap()
        );
        drop(connection);
        let mut text = String::new();
        client.read_to_string(&mut text).unwrap();
        let messages: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(messages.len(), 4);
        for (message, id) in messages.iter().zip(1..=3) {
            assert_eq!(message["id"], id);
            assert_eq!(message["status"], "ok");
        }
        assert_eq!(messages[3]["event"], "measurement");
    }
}

#[test]
fn subscribed_client_loss_does_not_fabricate_pending_set_result() {
    for upstream in [true, false] {
        let (mut connection, mut client) = loopback_pair();
        connection.session.outbound = OutboundBuffer::new(4096);
        let fanout = crate::fanout::ObservationFanout::new(1);
        let observations = fanout.register().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let mut catalog = get_catalog();
        catalog.extend(power_catalog());
        let latest = Mutex::new(HashMap::new());
        connection
            .drive_once_with_observations(&tx, &catalog, &latest, &observations)
            .unwrap();
        connection
            .session
            .dispatch_subscription(
                1,
                SubscriptionChange::Subscribe,
                vec!["sample_temperature".into()],
                &catalog,
            )
            .unwrap();
        connection
            .session
            .dispatch_set(
                2,
                "power".into(),
                Value::Integer(20),
                &tx,
                &catalog,
                Instant::now(),
            )
            .unwrap();
        let crate::runtime::DeviceCommand::Set(command) = rx.try_recv().unwrap() else {
            panic!()
        };
        assert!(command.execution.try_start());
        inject_observation_loss(&fanout, upstream);
        assert!(
            connection
                .drive_once_with_observations(&tx, &catalog, &latest, &observations)
                .unwrap()
        );
        command
            .execution
            .complete(|| command.reply.send(Ok(())).unwrap());
        connection.session.collect_ready(Instant::now()).unwrap();
        assert!(connection.session.outbound.is_empty());
        drop(connection);
        let mut text = String::new();
        client.read_to_string(&mut text).unwrap();
        let messages: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            messages.last().unwrap(),
            &json!({"status":"error","error":"observation_data_lost"})
        );
        assert!(!messages.iter().any(|message| message["id"] == 2));
    }
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
            "error": "observation_data_lost"
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
