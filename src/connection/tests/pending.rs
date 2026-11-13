use super::backend::{TestBackend, TestCommand};
use super::*;
use crate::port_worker::DeviceObservation;

#[test]
fn handles_read_and_set_without_blocking() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);

    let mut pending = PendingRequests::new(2);

    let (backend, commands) = TestBackend::new(2);
    let mut description = get_catalog()["sample_temperature"].clone();
    description.id = "sensor/sample_temperature".into();
    assert_eq!(
        backend.enqueue_read(&mut pending, 1, deadline, description),
        Ok(())
    );
    assert_eq!(
        backend.enqueue_set(
            &mut pending,
            2,
            deadline,
            "power".into(),
            Value::Integer(20)
        ),
        Ok(())
    );
    let TestCommand::Read { reply, .. } = commands.try_recv().unwrap() else {
        panic!("expected Read command")
    };
    let TestCommand::Set(command) = commands.try_recv().unwrap() else {
        panic!("expected Set command")
    };

    assert_eq!(pending.len(), 2);
    assert!(pending.poll(now).is_empty());

    assert!(command.execution.try_start());
    command.complete(Ok(())).unwrap();

    let finished = pending.poll(now);

    assert_eq!(finished.len(), 1);
    assert!(matches!(
        &finished[0],
        FinishedRequest::RoutedMutation(Completion::Received {
            id: 2,
            value: Ok(())
        })
    ));
    assert_eq!(pending.len(), 1);

    reply
        .send(Ok(DeviceObservation {
            device_id: "sensor".into(),
            observation: observation(),
        }))
        .unwrap();

    let finished = pending.poll(now);

    assert_eq!(finished.len(), 1);

    match &finished[0] {
        FinishedRequest::RoutedRead {
            key,
            completion:
                Completion::Received {
                    id,
                    value: Ok(value),
                },
        } => {
            assert_eq!(*id, 1);
            assert_eq!(key, "sensor/sample_temperature");
            assert_eq!(value.device_id, "sensor");
            assert_eq!(value.observation.source, "sample_temperature");
        }
        _ => panic!("expected completed Read"),
    }

    assert!(pending.is_empty());
    assert!(
        pending.poll(deadline).is_empty(),
        "completed requests must not reply twice"
    );
    assert!(commands.try_recv().is_err());
}

#[test]
fn rejects_duplicate_and_full_before_queueing() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);

    let mut pending = PendingRequests::new(1);
    let (backend, commands) = TestBackend::new(1);
    assert_eq!(
        backend.enqueue_read(
            &mut pending,
            7,
            deadline,
            get_catalog()["configuration"].clone()
        ),
        Ok(())
    );
    let _read = commands.try_recv().unwrap();
    assert_eq!(backend.submit_attempts.get(), 1);

    assert_eq!(
        backend.enqueue_set(
            &mut pending,
            7,
            deadline,
            "power".into(),
            Value::Integer(20)
        ),
        Err(EnqueueError::DuplicateId)
    );

    assert_eq!(backend.submit_attempts.get(), 1);
    assert!(commands.try_recv().is_err());

    assert_eq!(
        backend.enqueue_set(
            &mut pending,
            8,
            deadline,
            "power".into(),
            Value::Integer(20)
        ),
        Err(EnqueueError::Full)
    );

    assert_eq!(backend.submit_attempts.get(), 1);
    assert!(commands.try_recv().is_err());
    assert_eq!(pending.len(), 1);
}

#[test]
fn failed_enqueue_does_not_consume_capacity() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);

    let mut pending = PendingRequests::new(1);
    let (closed, commands) = TestBackend::new(1);
    drop(commands);

    assert_eq!(
        closed.enqueue_set(
            &mut pending,
            1,
            deadline,
            "power".into(),
            Value::Integer(20)
        ),
        Err(EnqueueError::RuntimeUnavailable)
    );

    assert!(pending.is_empty());

    let (backend, commands) = TestBackend::new(1);
    // Fill the backend queue independently of this client's pending capacity.
    let mut other = PendingRequests::new(1);
    backend
        .enqueue_set(&mut other, 99, deadline, "power".into(), Value::Integer(10))
        .unwrap();
    assert_eq!(
        backend.enqueue_set(
            &mut pending,
            1,
            deadline,
            "power".into(),
            Value::Integer(20)
        ),
        Err(EnqueueError::RuntimeBusy)
    );
    assert!(pending.is_empty());
    let _other_command = commands.try_recv().unwrap();
    assert_eq!(
        backend.enqueue_set(
            &mut pending,
            1,
            deadline,
            "power".into(),
            Value::Integer(20)
        ),
        Ok(())
    );
    assert_eq!(pending.len(), 1);
    assert!(matches!(commands.try_recv(), Ok(TestCommand::Set(_))));
    assert!(commands.try_recv().is_err());
}

#[test]
fn timeout_and_disconnect_release_capacity() {
    for (started, disconnected) in [(false, false), (true, false), (false, true), (true, true)] {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(3600);
        let mut pending = PendingRequests::new(2);
        let (backend, commands) = TestBackend::new(2);
        backend
            .enqueue_read(
                &mut pending,
                1,
                deadline,
                get_catalog()["configuration"].clone(),
            )
            .unwrap();
        drop(commands.try_recv().unwrap());
        backend
            .enqueue_set(
                &mut pending,
                2,
                deadline,
                "power".into(),
                Value::Integer(20),
            )
            .unwrap();
        let TestCommand::Set(command) = commands.try_recv().unwrap() else {
            panic!("expected Set command")
        };
        let execution = command.execution.clone();
        if started {
            assert!(execution.try_start());
        }
        let command = if disconnected {
            drop(command);
            None
        } else {
            Some(command)
        };
        let finished = pending.poll(if disconnected { now } else { deadline });
        assert_eq!(finished.len(), 2);
        assert!(matches!(
            &finished[0],
            FinishedRequest::RoutedRead {
                completion: Completion::Disconnected { id: 1 },
                ..
            }
        ));
        match (&finished[1], started, disconnected) {
            (FinishedRequest::RoutedMutation(Completion::OutcomeUnknown { id: 2 }), true, _) => {}
            (FinishedRequest::RoutedMutation(Completion::Timeout { id: 2 }), false, false) => {}
            (FinishedRequest::RoutedMutation(Completion::Disconnected { id: 2 }), false, true) => {}
            _ => panic!(
                "unexpected mutation completion: started={started}, disconnected={disconnected}"
            ),
        }
        assert!(pending.is_empty());
        assert!(
            !execution.try_start(),
            "finished mutation cannot start or restart"
        );
        if started && let Some(command) = command {
            assert!(matches!(
                command.complete(Ok(())),
                Err(mpsc::TrySendError::Disconnected(_))
            ));
        }
        assert!(
            pending.poll(deadline).is_empty(),
            "late completion must not reply twice"
        );
        assert!(
            commands.try_recv().is_err(),
            "completion must not enqueue a retry"
        );
        // Both completed entries release their capacity, even for unknown outcomes.
        backend
            .enqueue_read(
                &mut pending,
                3,
                deadline,
                get_catalog()["configuration"].clone(),
            )
            .unwrap();
        backend
            .enqueue_set(
                &mut pending,
                4,
                deadline,
                "power".into(),
                Value::Integer(30),
            )
            .unwrap();
        assert_eq!(pending.len(), 2);
    }
}
