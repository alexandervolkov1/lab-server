use super::*;

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
