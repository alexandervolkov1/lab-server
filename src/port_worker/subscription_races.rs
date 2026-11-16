use super::*;
use crate::device_registry::DeviceRegistry;
use crate::fanout::ObservationFanout;
use crate::model::Spec;
use std::io::{BufReader, Cursor};
use std::thread;

const WAIT: Duration = Duration::from_secs(10);

#[test]
fn loss_before_old_prefix_never_restarts_history_and_other_port_is_independent() {
    use crate::continuity::Continuity;
    use crate::observation_aggregator::{AggregatorEvent, ObservationAggregator};
    let make = || {
        PortWorker::new(
            BufReader::new(Cursor::new(vec![])),
            vec![DeviceInstance::new("a", MockDriver).unwrap()],
            2,
            2,
        )
        .unwrap()
    };
    let (mut worker, channels) = make();
    let (mut other, other_channels) = make();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("p", channels.commands.clone())
        .unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let mut continuity = Continuity::new(channels.commands.source_instance());
    let mut independent = Continuity::new(other_channels.commands.source_instance());
    // Real worker stamps x1, x2; x3 cannot enter the capacity-two queue.
    for _ in 0..3 {
        worker.poll_one();
    }
    let mut aggregator =
        ObservationAggregator::new(&registry, [("p".into(), channels.observations)]).unwrap();
    let batch = aggregator.poll(1);
    let [
        AggregatorEvent::DataLost { loss_epoch, .. },
        AggregatorEvent::Measurement {
            observation: x1, ..
        },
    ] = batch.as_slice()
    else {
        panic!("{batch:?}")
    };
    assert_eq!(*loss_epoch, 1);
    continuity.lost(*loss_epoch);
    assert_eq!(x1.metadata.as_ref().unwrap().sequence, 1);
    assert!(!continuity.accept(x1.metadata.as_ref().unwrap()));
    let batch = aggregator.poll(1);
    let [
        AggregatorEvent::Measurement {
            observation: x2, ..
        },
    ] = batch.as_slice()
    else {
        panic!()
    };
    assert_eq!(x2.metadata.as_ref().unwrap().sequence, 2);
    assert!(!continuity.accept(x2.metadata.as_ref().unwrap()));
    assert!(aggregator.poll(1).is_empty()); // silence does not recover anything
    other.poll_one();
    let sample = other_channels.observations.try_recv().unwrap();
    assert!(independent.accept(sample.observation.metadata.as_ref().unwrap()));
    worker.poll_one(); // x4, first trustworthy sample of generation 1
    let batch = aggregator.poll(1);
    let [
        AggregatorEvent::Measurement {
            observation: x4, ..
        },
    ] = batch.as_slice()
    else {
        panic!()
    };
    let stamp = x4.metadata.as_ref().unwrap();
    assert_eq!((stamp.sequence, stamp.generation), (4, 1));
    assert!(continuity.accept(stamp));
    assert!(!continuity.accept(stamp)); // replay
    for _ in 0..4 {
        worker.poll_one();
    } // x5/x6 queued; x7/x8 lost
    let batch = aggregator.poll(1);
    let [
        AggregatorEvent::DataLost { loss_epoch, .. },
        AggregatorEvent::Measurement { observation, .. },
    ] = batch.as_slice()
    else {
        panic!()
    };
    assert_eq!(*loss_epoch, 3);
    continuity.lost(*loss_epoch);
    assert!(!continuity.accept(observation.metadata.as_ref().unwrap()));
    continuity.lost(u64::MAX);
    assert!(!continuity.accept(stamp));
}

struct MockDriver;
impl Driver for MockDriver {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![ParameterDescription {
            id: "value".into(),
            spec: Spec::Boolean,
            unit: "".into(),
            writable: false,
            default_poll_period: Some(Duration::from_nanos(1)),
        }]
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        Ok(Value::Boolean(true))
    }
    fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
        unreachable!()
    }
}

#[test]
fn concurrent_subscription_boundary_cannot_split_full_from_loss_commit() {
    for initially_active in [true, false] {
        let device = DeviceInstance::new("a", MockDriver).unwrap();
        let (mut worker, channels) =
            PortWorker::new(BufReader::new(Cursor::new(vec![])), vec![device], 2, 1).unwrap();
        let mut registry = DeviceRegistry::new();
        registry
            .register_port("p", channels.commands.clone())
            .unwrap();
        registry.register_device("p", &worker.devices()[0]).unwrap();
        let fanout = ObservationFanout::routed(2, &registry);
        let receiver = fanout.register().unwrap();
        let keys = vec!["a/value".into()];
        if initially_active {
            receiver.interest.set_keys(&keys);
        }
        let (entered, full) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        worker.before_loss_commit = Some(Box::new(move || {
            entered.send(()).unwrap();
            resume.recv_timeout(WAIT).unwrap();
        }));
        let join = thread::spawn(move || worker.run());
        full.recv_timeout(WAIT).unwrap();
        assert_eq!(channels.commands.loss_epoch(), 0);
        assert!(matches!(
            channels.commands.observation_boundary.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        let interest = receiver.interest.clone();
        let (started, attempting) = mpsc::channel();
        let (done, completed) = mpsc::channel();
        let transition_keys = if initially_active {
            vec![]
        } else {
            keys.clone()
        };
        let transition = thread::spawn(move || {
            started.send(()).unwrap();
            interest.set_keys(&transition_keys);
            done.send(()).unwrap();
        });
        attempting.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            completed.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        // Control/shutdown does not take the observation gate, even in this
        // deliberately suspended publication. No further polls after commit.
        channels.commands.shutdown();
        release.send(()).unwrap();
        completed.recv_timeout(WAIT).unwrap();
        transition.join().unwrap();
        assert_eq!(join.join().unwrap(), StopReason::Shutdown);
        assert_eq!(channels.commands.loss_epoch(), 1);
        if initially_active {
            assert!(receiver.terminal_when_inactive());
            receiver.interest.set_keys(&keys);
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            ));
            assert_eq!(receiver.closed_error(), "observation_data_lost");
        } else {
            // Full happened before first subscription; the new baseline includes
            // that commit, even though activation raced with its recording.
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
        }
    }
}

#[test]
fn saturation_remains_fatal_for_new_and_repeated_subscriptions() {
    let device = DeviceInstance::new("a", MockDriver).unwrap();
    let (worker, channels) =
        PortWorker::new(BufReader::new(Cursor::new(vec![])), vec![device], 1, 1).unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("p", channels.commands.clone())
        .unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let fanout = ObservationFanout::routed(1, &registry);
    worker.loss_epoch.store(u64::MAX, Ordering::SeqCst);
    for _ in 0..2 {
        let receiver = fanout.register().unwrap();
        for keys in [vec!["a/value".into()], vec![], vec!["a/value".into()]] {
            receiver.interest.set_keys(&keys);
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            ));
            assert_eq!(receiver.closed_error(), "observation_data_lost");
        }
    }
}

#[test]
fn concurrent_loss_after_unsubscribe_does_not_poison_resubscribe_or_hold_io() {
    struct BlockingRead {
        entered: mpsc::Sender<()>,
        resume: mpsc::Receiver<()>,
    }
    impl Driver for BlockingRead {
        fn parameters(&self) -> Vec<ParameterDescription> {
            MockDriver.parameters()
        }
        fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
            self.entered.send(()).unwrap();
            self.resume.recv_timeout(WAIT).unwrap();
            Ok(Value::Boolean(true))
        }
        fn write(&mut self, _: &str, _: &Value, _: &mut dyn DeviceIo) -> Result<(), DriverError> {
            unreachable!()
        }
    }
    let (entered, in_io) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let device = DeviceInstance::new("a", BlockingRead { entered, resume }).unwrap();
    let (worker, channels) =
        PortWorker::new(BufReader::new(Cursor::new(vec![])), vec![device], 1, 1).unwrap();
    // Pre-fill the bounded queue so the suspended transaction will hit Full.
    worker
        .output
        .try_send(DeviceObservation {
            device_id: "a".into(),
            observation: Observation {
                source: "value".into(),
                unit: "".into(),
                metadata: None,
                completed_at: SystemTime::UNIX_EPOCH,
                reading: Ok(Value::Boolean(false)),
            },
        })
        .unwrap();
    let mut registry = DeviceRegistry::new();
    registry
        .register_port("p", channels.commands.clone())
        .unwrap();
    registry.register_device("p", &worker.devices()[0]).unwrap();
    let fanout = ObservationFanout::routed(2, &registry);
    let receiver = fanout.register().unwrap();
    let keys = vec!["a/value".into()];
    receiver.interest.set_keys(&keys);
    let join = thread::spawn(move || worker.run());
    in_io.recv_timeout(WAIT).unwrap();
    assert!(channels.commands.observation_boundary.try_lock().is_ok());
    receiver.interest.set_keys(&[]); // completes without releasing physical I/O
    channels.commands.shutdown();
    release.send(()).unwrap();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    assert_eq!(channels.commands.loss_epoch(), 1);
    receiver.interest.set_keys(&keys);
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
}
