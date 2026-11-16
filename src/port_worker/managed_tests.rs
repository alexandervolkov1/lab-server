use super::*;
use crate::{
    command_router::CommandRouter,
    device_registry::DeviceRegistry,
    output::Authority,
    pending::{Completion, Poll},
};
use std::io::{self, BufRead, Cursor, Read};
use std::sync::{Barrier, atomic::AtomicUsize};

const WAIT: Duration = Duration::from_secs(10);
struct MemoryIo {
    input: Cursor<Vec<u8>>,
    bytes: Arc<AtomicUsize>,
}
impl Read for MemoryIo {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.input.read(b)
    }
}
impl BufRead for MemoryIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.input.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.input.consume(n)
    }
}
impl DeviceIo for MemoryIo {
    fn send(&mut self, b: &[u8]) -> io::Result<()> {
        self.bytes.fetch_add(b.len(), Ordering::SeqCst);
        Ok(())
    }
}
enum Progress {
    Started,
    Done,
}
struct DriverGate {
    progress: mpsc::Sender<Progress>,
    resume: mpsc::Receiver<()>,
}
struct Actuator(Option<DriverGate>);
impl Driver for Actuator {
    fn parameters(&self) -> Vec<ParameterDescription> {
        ["power", "backup"]
            .into_iter()
            .map(|id| ParameterDescription {
                id: id.into(),
                unit: String::new(),
                spec: crate::model::Spec::Integer { min: 0, max: 100 },
                writable: true,
                default_poll_period: None,
            })
            .collect()
    }
    fn actions(&self) -> Vec<ActionDescription> {
        vec![ActionDescription {
            name: "reset".into(),
            description: String::new(),
        }]
    }
    fn read(&mut self, _: &str, _: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        Ok(Value::Integer(0))
    }
    fn write(&mut self, _: &str, _: &Value, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        io.send(b"W")?;
        if let Some(g) = &self.0 {
            g.progress.send(Progress::Started).unwrap();
            g.resume.recv_timeout(WAIT).unwrap();
        }
        Ok(())
    }
    fn invoke_action(&mut self, _: &str, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        io.send(b"A")?;
        Ok(())
    }
}
struct Fixture {
    worker: PortWorker<MemoryIo>,
    channels: PortChannels,
    router: CommandRouter,
    authority: Arc<Authority>,
    bytes: Arc<AtomicUsize>,
}
impl Fixture {
    fn new(managed: bool, gate: Option<DriverGate>) -> Self {
        let authority = Arc::new(Authority::new(
            ["a/power", "a/backup", "b/power"]
                .into_iter()
                .map(str::to_owned),
        ));
        let bytes = Arc::new(AtomicUsize::new(0));
        let (worker, channels) = PortWorker::with_mode(
            MemoryIo {
                input: Cursor::new(vec![]),
                bytes: bytes.clone(),
            },
            vec![DeviceInstance::new("a", Actuator(gate)).unwrap()],
            8,
            8,
            if managed {
                PortMode::Managed(authority.clone())
            } else {
                PortMode::Standalone
            },
        )
        .unwrap();
        let mut registry = DeviceRegistry::new();
        registry
            .register_port("p", channels.commands.clone())
            .unwrap();
        registry.register_device("p", &worker.devices()[0]).unwrap();
        let router = CommandRouter::with_authority(registry, authority.clone());
        Self {
            worker,
            channels,
            router,
            authority,
            bytes,
        }
    }
    fn execute(&mut self, p: Pending<Result<(), CommandError>>) -> Result<(), CommandError> {
        let command = self.worker.commands.try_recv().unwrap();
        self.worker.execute(command);
        match p.poll(Instant::now()) {
            Poll::Finished(Completion::Received { value, .. }) => value,
            _ => panic!("expected completed command"),
        }
    }
}

#[test]
fn managed_rejects_direct_mutations_without_io_and_standalone_preserves_them() {
    for managed in [false, true] {
        let mut f = Fixture::new(managed, None);
        let deadline = Instant::now() + WAIT;
        let p = f
            .channels
            .commands
            .set(1, "a", "power", Value::Integer(3), deadline)
            .unwrap();
        assert_eq!(
            f.execute(p),
            if managed {
                Err(CommandError::Unauthorized)
            } else {
                Ok(())
            }
        );
        let p = f
            .channels
            .commands
            .invoke_action(2, "a", "reset", deadline)
            .unwrap();
        assert_eq!(
            f.execute(p),
            if managed {
                Err(CommandError::Unauthorized)
            } else {
                Ok(())
            }
        );
        assert_eq!(f.bytes.load(Ordering::SeqCst), if managed { 0 } else { 2 });
        assert_eq!(f.channels.commands.stop_reason(), None);
        let before = f.bytes.load(Ordering::SeqCst);
        let p = f
            .router
            .set(3, "a/power", Value::Integer(5), deadline)
            .unwrap();
        assert_eq!(f.execute(p), Ok(()));
        let p = f.router.invoke_action(4, "a/reset", deadline).unwrap();
        assert_eq!(f.execute(p), Ok(()));
        assert_eq!(f.bytes.load(Ordering::SeqCst), before + 2);
        let owner = ("controller".into(), 1);
        f.authority.acquire("a/power", owner.clone()).unwrap();
        assert!(
            f.authority
                .acquire("a/power", ("second".into(), 2))
                .is_err()
        );
        assert!(
            f.router
                .set(5, "a/power", Value::Integer(5), deadline)
                .is_err()
        );
        assert!(f.router.invoke_action(6, "a/reset", deadline).is_err());
        if managed {
            let p = f
                .channels
                .commands
                .set(7, "a", "power", Value::Integer(3), deadline)
                .unwrap();
            assert_eq!(f.execute(p), Err(CommandError::Unauthorized));
            let p = f
                .channels
                .commands
                .invoke_action(8, "a", "reset", deadline)
                .unwrap();
            assert_eq!(f.execute(p), Err(CommandError::Unauthorized));
            assert_eq!(f.bytes.load(Ordering::SeqCst), before + 2);
        }
        f.authority.release(&owner);
    }
}

#[test]
fn managed_checks_permit_issuer_and_exact_output_scope_at_worker_boundary() {
    let mut f = Fixture::new(true, None);
    let foreign = Authority::new(["a/power".into(), "a/backup".into()].into_iter());
    for permit in [
        f.authority.permit(&["a/backup".into()], None).unwrap(),
        f.authority.permit(&["b/power".into()], None).unwrap(),
        foreign.permit(&["a/power".into()], None).unwrap(),
        f.authority
            .permit(&["a/power".into(), "a/backup".into()], None)
            .unwrap(),
    ] {
        let p = f
            .channels
            .commands
            .set_guarded(
                1,
                "a",
                "power",
                Value::Integer(5),
                (Instant::now() + WAIT, Some(permit)),
            )
            .unwrap();
        assert_eq!(f.execute(p), Err(CommandError::Unauthorized));
    }
    for permit in [
        f.authority.permit(&["a/power".into()], None).unwrap(),
        foreign
            .permit(&["a/power".into(), "a/backup".into()], None)
            .unwrap(),
        f.authority.permit(&[], None).unwrap(),
    ] {
        let p = f
            .channels
            .commands
            .action_guarded(2, "a", "reset", Instant::now() + WAIT, Some(permit))
            .unwrap();
        assert_eq!(f.execute(p), Err(CommandError::Unauthorized));
    }
    assert_eq!(f.bytes.load(Ordering::SeqCst), 0);
    assert_eq!(f.channels.commands.stop_reason(), None);
    let owner = ("controller".into(), 1);
    f.authority.acquire("a/power", owner.clone()).unwrap();
    let p = f
        .router
        .set_owned(
            3,
            "a/power",
            Value::Integer(5),
            Instant::now() + WAIT,
            Some(&owner),
        )
        .unwrap();
    f.authority.release(&owner);
    assert_eq!(f.execute(p), Err(CommandError::RuntimeTimeout));
    assert_eq!(f.bytes.load(Ordering::SeqCst), 0);
}

#[test]
fn concurrent_managed_revoke_and_driver_start_keep_exclusive_ownership() {
    for _ in 0..32 {
        let (progress_tx, progress_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let mut f = Fixture::new(
            true,
            Some(DriverGate {
                progress: progress_tx.clone(),
                resume: resume_rx,
            }),
        );
        let owner = ("controller".into(), 1);
        f.authority.acquire("a/power", owner.clone()).unwrap();
        let p = f
            .router
            .set_owned(
                1,
                "a/power",
                Value::Integer(3),
                Instant::now() + WAIT,
                Some(&owner),
            )
            .unwrap();
        let command = f.worker.commands.try_recv().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let other = barrier.clone();
        let join = std::thread::spawn(move || {
            other.wait();
            f.worker.execute(command);
            progress_tx.send(Progress::Done).unwrap();
            f.worker
        });
        barrier.wait();
        f.authority.release(&owner);
        let started = match progress_rx.recv_timeout(WAIT).unwrap() {
            Progress::Started => {
                assert!(f.authority.acquire("a/power", ("next".into(), 2)).is_err());
                resume_tx.send(()).unwrap();
                assert!(matches!(
                    progress_rx.recv_timeout(WAIT).unwrap(),
                    Progress::Done
                ));
                true
            }
            Progress::Done => false,
        };
        let _worker = join.join().unwrap();
        assert_eq!(f.bytes.load(Ordering::SeqCst), usize::from(started));
        f.authority.acquire("a/power", ("next".into(), 2)).unwrap();
        match p.poll(Instant::now()) {
            Poll::Finished(Completion::Received { value, .. }) => assert_eq!(
                value,
                if started {
                    Ok(())
                } else {
                    Err(CommandError::RuntimeTimeout)
                }
            ),
            _ => panic!("missing completion"),
        }
    }
}

#[test]
fn managed_started_timeout_latches_unknown_after_late_completion_without_retry() {
    let (progress_tx, progress_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let mut f = Fixture::new(
        true,
        Some(DriverGate {
            progress: progress_tx,
            resume: resume_rx,
        }),
    );
    let owner = ("controller".into(), 1);
    f.authority.acquire("a/power", owner.clone()).unwrap();
    let deadline = Instant::now() + WAIT;
    let p = f
        .router
        .set_owned(1, "a/power", Value::Integer(3), deadline, Some(&owner))
        .unwrap();
    let command = f.worker.commands.try_recv().unwrap();
    let join = std::thread::spawn(move || {
        f.worker.execute(command);
        f.worker
    });
    assert!(matches!(
        progress_rx.recv_timeout(WAIT).unwrap(),
        Progress::Started
    ));
    assert!(matches!(
        p.poll(deadline),
        Poll::Finished(Completion::OutcomeUnknown { .. })
    ));
    f.authority.release(&owner);
    assert!(f.authority.acquire("a/power", ("next".into(), 2)).is_err());
    resume_tx.send(()).unwrap();
    let _worker = join.join().unwrap();
    assert!(f.authority.acquire("a/power", ("next".into(), 2)).is_err());
    assert_eq!(f.bytes.load(Ordering::SeqCst), 1);
    let snapshot = f.authority.snapshot();
    let output = snapshot
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["key"] == "a/power")
        .unwrap();
    assert_eq!(output["outcome_unknown"], true);
    assert_eq!(output["in_flight"], 0);
}
