use super::*;

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
