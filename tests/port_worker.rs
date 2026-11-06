use std::collections::VecDeque;
use std::io::{self, BufRead, Read};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use lab_server::driver::{DeviceIo, Driver, DriverError, PortState};
use lab_server::model::{ParameterDescription, Spec, Value};
use lab_server::pending::{Completion, Pending, Poll};
use lab_server::port_worker::{
    CommandError, DeviceInstance, DeviceObservation, PortWorker, StopReason, SubmitError,
};

const WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct Call {
    address: u8,
    write: bool,
    sequence: u8,
    io_address: usize,
}

#[derive(Default)]
struct Trace {
    sends: Vec<Vec<u8>>,
    completed: usize,
    calls: Vec<Call>,
}

struct FakeIo {
    trace: Arc<Mutex<Trace>>,
    active: Option<Vec<u8>>,
    offset: usize,
    read_error: Option<io::ErrorKind>,
}

impl FakeIo {
    fn new(trace: Arc<Mutex<Trace>>) -> Self {
        Self {
            trace,
            active: None,
            offset: 3,
            read_error: None,
        }
    }
}

impl Read for FakeIo {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let data = self.fill_buf()?;
        let size = out.len().min(data.len());
        out[..size].copy_from_slice(&data[..size]);
        self.consume(size);
        Ok(size)
    }
}

impl BufRead for FakeIo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if let Some(kind) = self.read_error.take() {
            return Err(io::Error::from(kind));
        }
        Ok(&b"ok\n"[self.offset..])
    }
    fn consume(&mut self, size: usize) {
        self.offset += size;
        if self.offset == 3 && self.active.take().is_some() {
            self.trace.lock().unwrap().completed += 1;
        }
    }
}

impl DeviceIo for FakeIo {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        assert_eq!(request.len(), 4);
        if request[3] == 0 {
            assert!(
                self.active.is_none(),
                "transactions interleaved or stale reply consumed by next driver"
            );
            self.active = Some(request[..3].to_vec());
        } else {
            assert_eq!(self.active.as_deref(), Some(&request[..3]));
            self.offset = 0;
        }
        self.trace.lock().unwrap().sends.push(request.to_vec());
        Ok(())
    }
}

struct Gate {
    entered: mpsc::Sender<Call>,
    resume: Mutex<mpsc::Receiver<()>>,
}

fn gate() -> (Arc<Gate>, mpsc::Receiver<Call>, mpsc::Sender<()>) {
    let (entered, calls) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    (
        Arc::new(Gate {
            entered,
            resume: Mutex::new(resume),
        }),
        calls,
        release,
    )
}

// Distinct const instantiations implement two byte-level protocols. Each logical
// device has its own request counter and value even when its driver type matches.
struct FakeDriver<const PROTOCOL: u8> {
    address: u8,
    sequence: u8,
    enabled: bool,
    trace: Arc<Mutex<Trace>>,
    gate: Option<Arc<Gate>>,
    failures: VecDeque<DriverError>,
    panic: bool,
}

impl<const PROTOCOL: u8> FakeDriver<PROTOCOL> {
    fn new(address: u8, trace: Arc<Mutex<Trace>>) -> Self {
        Self {
            address,
            sequence: 0,
            enabled: false,
            trace,
            gate: None,
            failures: VecDeque::new(),
            panic: false,
        }
    }

    fn transaction(&mut self, write: bool, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        self.sequence += 1;
        let call = Call {
            address: self.address,
            write,
            sequence: self.sequence,
            io_address: (io as *mut dyn DeviceIo as *mut ()) as usize,
        };
        self.trace.lock().unwrap().calls.push(call.clone());
        io.send(&[PROTOCOL, self.address, self.sequence, 0])?;
        if let Some(gate) = &self.gate {
            gate.entered.send(call).unwrap();
            gate.resume.lock().unwrap().recv_timeout(WAIT).unwrap();
        }
        assert!(!self.panic, "simulated driver panic");
        io.send(&[PROTOCOL, self.address, self.sequence, 1])?;
        let failure = self.failures.pop_front();
        if let Some(error) = &failure
            && error.port_state != PortState::Synchronized
        {
            return Err(error.clone());
        }
        let mut response = String::new();
        io.read_line(&mut response)?;
        assert_eq!(response, "ok\n");
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl<const PROTOCOL: u8> Driver for FakeDriver<PROTOCOL> {
    fn parameters(&self) -> Vec<ParameterDescription> {
        vec![
            ParameterDescription {
                id: "value".into(),
                spec: Spec::Integer { min: 0, max: 255 },
                unit: "test".into(),
                writable: false,
                default_poll_period: None,
            },
            ParameterDescription {
                id: "enabled".into(),
                spec: Spec::Boolean,
                unit: "".into(),
                writable: true,
                default_poll_period: None,
            },
        ]
    }
    fn read(&mut self, parameter: &str, io: &mut dyn DeviceIo) -> Result<Value, DriverError> {
        self.transaction(false, io)?;
        Ok(if parameter == "enabled" {
            Value::Boolean(self.enabled)
        } else {
            Value::Integer(self.sequence.into())
        })
    }
    fn write(&mut self, _: &str, value: &Value, io: &mut dyn DeviceIo) -> Result<(), DriverError> {
        let Value::Boolean(value) = value else {
            panic!("wrong type reached driver")
        };
        self.enabled = *value;
        self.transaction(true, io)
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

fn received<T>(mut pending: Pending<T>) -> T {
    let limit = Instant::now() + WAIT;
    loop {
        match pending.poll(Instant::now()) {
            Poll::Waiting(next) => pending = next,
            Poll::Finished(Completion::Received { value, .. }) => return value,
            _ => panic!("expected explicit command reply"),
        }
        assert!(Instant::now() < limit, "reply timed out");
        thread::sleep(Duration::from_millis(1));
    }
}

fn value(pending: Pending<Result<DeviceObservation, CommandError>>) -> Value {
    received(pending).unwrap().observation.reading.unwrap()
}

#[test]
fn different_drivers_share_one_io_with_independent_state_and_local_names() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let devices = vec![
        DeviceInstance::new("gauge1", FakeDriver::<1>::new(1, trace.clone())).unwrap(),
        DeviceInstance::new("controller1", FakeDriver::<2>::new(2, trace.clone())).unwrap(),
        DeviceInstance::new("gauge2", FakeDriver::<1>::new(3, trace.clone())).unwrap(),
    ];
    let (worker, channels) = PortWorker::new(FakeIo::new(trace.clone()), devices, 16, 16).unwrap();
    let handle = &channels.commands;
    let join = thread::spawn(move || worker.run());
    for round in 1..=2 {
        for device in ["gauge1", "controller1", "gauge2"] {
            let observation =
                received(handle.read(1, device, "value", deadline()).unwrap()).unwrap();
            assert_eq!(observation.device_id, device);
            assert_eq!(observation.observation.source, "value");
            assert_eq!(observation.observation.reading, Ok(Value::Integer(round)));
        }
    }
    assert_eq!(
        received(
            handle
                .set(2, "gauge1", "enabled", Value::Boolean(true), deadline())
                .unwrap()
        ),
        Ok(())
    );
    assert_eq!(
        value(handle.read(3, "gauge1", "enabled", deadline()).unwrap()),
        Value::Boolean(true)
    );
    assert_eq!(
        value(handle.read(4, "gauge2", "enabled", deadline()).unwrap()),
        Value::Boolean(false)
    );
    handle.shutdown();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    let trace = trace.lock().unwrap();
    assert_eq!(trace.completed, trace.calls.len());
    assert_eq!(trace.sends.len(), trace.calls.len() * 2);
    assert!(
        trace
            .calls
            .iter()
            .all(|call| call.io_address == trace.calls[0].io_address)
    );
    for pair in trace.sends.chunks_exact(2) {
        assert_eq!(&pair[0][..3], &pair[1][..3]);
    }
}

#[test]
fn concurrent_senders_get_one_reply_each_and_transactions_never_interleave() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let devices = vec![DeviceInstance::new("one", FakeDriver::<1>::new(1, trace.clone())).unwrap()];
    let (worker, channels) = PortWorker::new(FakeIo::new(trace.clone()), devices, 32, 8).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let senders: Vec<_> = (0..8)
        .map(|sender| {
            let handle = channels.commands.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                (0..4)
                    .map(|request| {
                        handle
                            .read(sender * 4 + request, "one", "value", deadline())
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let pending: Vec<_> = senders
        .into_iter()
        .flat_map(|sender| sender.join().unwrap())
        .collect();
    let join = thread::spawn(move || worker.run());
    let mut values: Vec<_> = pending
        .into_iter()
        .map(|pending| match value(pending) {
            Value::Integer(value) => value,
            _ => panic!(),
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, (1..=32).collect::<Vec<_>>());
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(trace.lock().unwrap().completed, 32);
}

#[test]
fn bounded_queue_rejects_overflow_and_drop_rejects_accepted_commands() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (worker, channels) = PortWorker::new(FakeIo::new(trace.clone()), vec![], 1, 1).unwrap();
    let pending = channels
        .commands
        .read(1, "one", "value", deadline())
        .unwrap();
    assert!(matches!(
        channels
            .commands
            .set(2, "one", "enabled", Value::Boolean(true), deadline()),
        Err(SubmitError::Full)
    ));
    drop(worker);
    assert!(matches!(
        received(pending),
        Err(CommandError::PortStopped(StopReason::Shutdown))
    ));
    assert!(matches!(
        channels.commands.read(3, "one", "value", deadline()),
        Err(SubmitError::Stopped(StopReason::Shutdown))
    ));
    assert!(trace.lock().unwrap().sends.is_empty());
}

#[test]
fn expired_and_cancelled_sets_never_reach_driver() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let devices = vec![DeviceInstance::new("one", FakeDriver::<1>::new(1, trace.clone())).unwrap()];
    let (worker, channels) = PortWorker::new(FakeIo::new(trace.clone()), devices, 8, 8).unwrap();
    let expired = channels
        .commands
        .set(1, "one", "enabled", Value::Boolean(true), Instant::now())
        .unwrap();
    let future = deadline();
    let cancelled = channels
        .commands
        .set(2, "one", "enabled", Value::Boolean(true), future)
        .unwrap();
    assert!(matches!(
        cancelled.poll(future),
        Poll::Finished(Completion::Timeout { id: 2 })
    ));
    let barrier = channels
        .commands
        .read(3, "one", "enabled", deadline())
        .unwrap();
    let join = thread::spawn(move || worker.run());
    assert_eq!(value(barrier), Value::Boolean(false));
    assert_eq!(received(expired), Err(CommandError::RuntimeTimeout));
    channels.commands.shutdown();
    join.join().unwrap();
    assert_eq!(trace.lock().unwrap().calls.len(), 1);
}

#[test]
fn started_set_timeout_keeps_port_owned_until_late_reply_finishes() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut driver = FakeDriver::<1>::new(1, trace.clone());
    driver.gate = Some(gate);
    let (worker, channels) = PortWorker::new(
        FakeIo::new(trace.clone()),
        vec![DeviceInstance::new("one", driver).unwrap()],
        8,
        8,
    )
    .unwrap();
    let future = deadline();
    let pending = channels
        .commands
        .set(1, "one", "enabled", Value::Boolean(true), future)
        .unwrap();
    let join = thread::spawn(move || worker.run());
    assert!(calls.recv_timeout(WAIT).unwrap().write);
    assert!(matches!(
        pending.poll(future),
        Poll::Finished(Completion::OutcomeUnknown { id: 1 })
    ));
    let read = channels
        .commands
        .read(2, "one", "enabled", deadline())
        .unwrap();
    assert!(calls.try_recv().is_err());
    assert_eq!(trace.lock().unwrap().sends.len(), 1);
    release.send(()).unwrap();
    let next = calls.recv_timeout(WAIT).unwrap();
    assert!(!next.write);
    assert_eq!(next.sequence, 2);
    channels.commands.shutdown();
    release.send(()).unwrap();
    assert_eq!(value(read), Value::Boolean(true));
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    assert_eq!(trace.lock().unwrap().completed, 2);
}

#[test]
fn every_started_set_driver_error_is_outcome_unknown() {
    for state in [
        PortState::Synchronized,
        PortState::Desynchronized,
        PortState::Disconnected,
    ] {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let mut driver = FakeDriver::<1>::new(1, trace.clone());
        driver
            .failures
            .push_back(DriverError::new("write failed", state));
        let (worker, channels) = PortWorker::new(
            FakeIo::new(trace),
            vec![DeviceInstance::new("one", driver).unwrap()],
            8,
            8,
        )
        .unwrap();
        let write = channels
            .commands
            .set(1, "one", "enabled", Value::Boolean(true), deadline())
            .unwrap();
        let queued = channels
            .commands
            .read(2, "one", "enabled", deadline())
            .unwrap();
        let join = thread::spawn(move || worker.run());
        assert_eq!(received(write), Err(CommandError::OutcomeUnknown));
        if state == PortState::Synchronized {
            assert_eq!(value(queued), Value::Boolean(true));
            assert_eq!(channels.commands.port_state(), state);
            assert!(channels.commands.stop_reason().is_none());
            channels.commands.shutdown();
        } else {
            assert!(
                matches!(received(queued), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
            );
        }
        join.join().unwrap();
        assert_eq!(channels.commands.port_state(), state);
    }
}

#[test]
fn read_error_state_controls_other_devices_and_queued_commands() {
    for state in [
        PortState::Synchronized,
        PortState::Desynchronized,
        PortState::Disconnected,
    ] {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let mut bad = FakeDriver::<1>::new(1, trace.clone());
        bad.failures
            .push_back(DriverError::new("read failed", state));
        let devices = vec![
            DeviceInstance::new("one", bad).unwrap(),
            DeviceInstance::new("two", FakeDriver::<2>::new(2, trace.clone())).unwrap(),
        ];
        let (worker, channels) =
            PortWorker::new(FakeIo::new(trace.clone()), devices, 8, 8).unwrap();
        assert_eq!(channels.commands.port_state(), PortState::Synchronized);
        let read = channels
            .commands
            .read(1, "one", "value", deadline())
            .unwrap();
        let queued_read = channels
            .commands
            .read(2, "two", "value", deadline())
            .unwrap();
        let queued_set = channels
            .commands
            .set(3, "two", "enabled", Value::Boolean(true), deadline())
            .unwrap();
        let join = thread::spawn(move || worker.run());
        let observation = received(read).unwrap();
        assert_eq!(observation.device_id, "one");
        assert_eq!(observation.observation.reading, Err("read failed".into()));
        if state == PortState::Synchronized {
            assert_eq!(value(queued_read), Value::Integer(1));
            assert_eq!(received(queued_set), Ok(()));
            channels.commands.shutdown();
        } else {
            assert!(
                matches!(received(queued_read), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
            );
            assert!(
                matches!(received(queued_set), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
            );
        }
        join.join().unwrap();
        assert_eq!(channels.commands.port_state(), state);
        assert_eq!(
            trace.lock().unwrap().calls.len(),
            if state == PortState::Synchronized {
                3
            } else {
                1
            }
        );
        assert!(matches!(
            channels.commands.read(4, "two", "value", deadline()),
            Err(SubmitError::Stopped(_))
        ));
    }
}

#[test]
fn late_bytes_after_io_timeout_are_not_read_or_cleared_by_another_driver() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let mut io = FakeIo::new(trace.clone());
    io.read_error = Some(io::ErrorKind::TimedOut);
    let devices = vec![
        DeviceInstance::new("one", FakeDriver::<1>::new(1, trace.clone())).unwrap(),
        DeviceInstance::new("two", FakeDriver::<2>::new(2, trace.clone())).unwrap(),
    ];
    let (worker, channels) = PortWorker::new(io, devices, 8, 8).unwrap();
    let read = channels
        .commands
        .read(1, "one", "value", deadline())
        .unwrap();
    let next = channels
        .commands
        .read(2, "two", "value", deadline())
        .unwrap();
    assert!(
        matches!(worker.run(), StopReason::Driver(error) if error.port_state == PortState::Desynchronized)
    );
    assert!(received(read).unwrap().observation.reading.is_err());
    assert!(matches!(received(next), Err(CommandError::PortStopped(_))));
    let trace = trace.lock().unwrap();
    assert_eq!(trace.completed, 0);
    assert_eq!(trace.sends.len(), 2);
    assert_eq!(trace.calls.len(), 1);
}

#[test]
fn commands_and_overdue_polls_alternate_under_sustained_load() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut first = FakeDriver::<1>::new(1, trace.clone());
    first.gate = Some(gate.clone());
    let mut second = FakeDriver::<2>::new(2, trace.clone());
    second.gate = Some(gate);
    let mut devices = vec![
        DeviceInstance::new("one", first).unwrap(),
        DeviceInstance::new("two", second).unwrap(),
    ];
    for device in &mut devices {
        device
            .set_poll_period("value", Some(Duration::from_nanos(1)))
            .unwrap();
    }
    let (worker, channels) = PortWorker::new(FakeIo::new(trace.clone()), devices, 8, 32).unwrap();
    let mut pending = Vec::new();
    // Keep the command queue nonempty throughout 16 consecutive transactions.
    for id in 0..8 {
        pending.push(
            channels
                .commands
                .set(id, "one", "enabled", Value::Boolean(true), deadline())
                .unwrap(),
        );
    }
    let join = thread::spawn(move || worker.run());
    let mut polled = [0; 2];
    for turn in 0..16 {
        let call = calls.recv_timeout(WAIT).unwrap();
        assert_eq!(call.write, turn % 2 == 0);
        if !call.write {
            polled[usize::from(call.address - 1)] += 1;
        }
        if turn == 15 {
            channels.commands.shutdown();
        }
        release.send(()).unwrap();
    }
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    for pending in pending {
        assert_eq!(received(pending), Ok(()));
    }
    assert_eq!(polled, [4, 4]);
    assert_eq!(channels.observations.try_iter().count(), 8);
}

#[test]
fn synchronized_poll_failure_does_not_stop_other_devices() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut first = FakeDriver::<1>::new(1, trace.clone());
    first.gate = Some(gate.clone());
    first
        .failures
        .push_back(DriverError::new("bad measurement", PortState::Synchronized));
    let mut second = FakeDriver::<2>::new(2, trace.clone());
    second.gate = Some(gate);
    let mut devices = vec![
        DeviceInstance::new("one", first).unwrap(),
        DeviceInstance::new("two", second).unwrap(),
    ];
    for device in &mut devices {
        device
            .set_poll_period("value", Some(Duration::from_secs(60)))
            .unwrap();
    }
    let (worker, channels) = PortWorker::new(FakeIo::new(trace), devices, 8, 8).unwrap();
    let join = thread::spawn(move || worker.run());
    assert_eq!(calls.recv_timeout(WAIT).unwrap().address, 1);
    release.send(()).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap().address, 2);
    channels.commands.shutdown();
    release.send(()).unwrap();
    join.join().unwrap();
    let events: Vec<_> = channels.observations.try_iter().collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].observation.reading, Err("bad measurement".into()));
    assert_eq!(events[1].observation.reading, Ok(Value::Integer(1)));
}

#[test]
fn shutdown_with_active_poll_and_full_queue_completes_all_waiters() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut driver = FakeDriver::<1>::new(1, trace.clone());
    driver.gate = Some(gate);
    let mut device = DeviceInstance::new("one", driver).unwrap();
    device
        .set_poll_period("value", Some(Duration::from_secs(60)))
        .unwrap();
    let (worker, channels) =
        PortWorker::new(FakeIo::new(trace.clone()), vec![device], 2, 8).unwrap();
    let join = thread::spawn(move || worker.run());
    calls.recv_timeout(WAIT).unwrap();
    let read = channels
        .commands
        .read(1, "one", "value", deadline())
        .unwrap();
    let write = channels
        .commands
        .set(2, "one", "enabled", Value::Boolean(true), deadline())
        .unwrap();
    channels.commands.shutdown();
    release.send(()).unwrap();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    assert!(matches!(
        received(read),
        Err(CommandError::PortStopped(StopReason::Shutdown))
    ));
    assert_eq!(
        received(write),
        Err(CommandError::PortStopped(StopReason::Shutdown))
    );
    assert_eq!(trace.lock().unwrap().calls.len(), 1);
}

#[test]
fn idle_worker_stops_on_shutdown_or_last_sender_drop() {
    for shutdown in [false, true] {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let (worker, channels) = PortWorker::new(FakeIo::new(trace), vec![], 1, 1).unwrap();
        let (done, finished) = mpsc::channel();
        let join = thread::spawn(move || done.send(worker.run()).unwrap());
        if shutdown {
            channels.commands.shutdown();
        } else {
            drop(channels.commands);
        }
        assert_eq!(
            finished.recv_timeout(WAIT).unwrap(),
            if shutdown {
                StopReason::Shutdown
            } else {
                StopReason::NoCommandSenders
            }
        );
        join.join().unwrap();
    }
}

#[test]
fn observation_overflow_is_explicit_and_never_blocks_worker() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut driver = FakeDriver::<1>::new(1, trace.clone());
    driver.gate = Some(gate);
    let mut device = DeviceInstance::new("one", driver).unwrap();
    device
        .set_poll_period("value", Some(Duration::from_nanos(1)))
        .unwrap();
    let (worker, channels) =
        PortWorker::new(FakeIo::new(trace.clone()), vec![device], 2, 1).unwrap();
    let observer = channels.commands.clone();
    let join = thread::spawn(move || worker.run());
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 1);
    assert_eq!(observer.loss_epoch(), 0);
    release.send(()).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 2);
    assert_eq!(observer.loss_epoch(), 0); // First observation fits in the queue.

    // Each gate is entered after the preceding publication attempt. No timing
    // assumptions: hold the next transaction while inspecting the loss counter.
    for losses in 1..=2 {
        release.send(()).unwrap();
        calls.recv_timeout(WAIT).unwrap();
        assert_eq!(observer.loss_epoch(), losses);
        assert!(observer.stop_reason().is_none());
        assert_eq!(observer.port_state(), PortState::Synchronized);
    }

    let get = channels
        .commands
        .read(1, "one", "enabled", deadline())
        .unwrap();
    release.send(()).unwrap(); // Poll 4 is lost; get 5 starts.
    let call = calls.recv_timeout(WAIT).unwrap();
    assert_eq!(call.sequence, 5);
    assert!(!call.write);
    assert_eq!(observer.loss_epoch(), 3);
    release.send(()).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 6);
    assert_eq!(value(get), Value::Boolean(false));

    let set = channels
        .commands
        .set(2, "one", "enabled", Value::Boolean(true), deadline())
        .unwrap();
    release.send(()).unwrap(); // Poll 6 is lost; set 7 starts.
    let call = calls.recv_timeout(WAIT).unwrap();
    assert_eq!(call.sequence, 7);
    assert!(call.write);
    assert_eq!(observer.loss_epoch(), 4);
    release.send(()).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 8);
    assert_eq!(received(set), Ok(()));

    let get = channels
        .commands
        .read(3, "one", "enabled", deadline())
        .unwrap();
    release.send(()).unwrap(); // Poll 8 is lost; get 9 verifies the write.
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 9);
    release.send(()).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 10);
    assert_eq!(value(get), Value::Boolean(true));
    assert_eq!(observer.loss_epoch(), 5);

    let first = channels.observations.try_recv().unwrap();
    assert_eq!(first.observation.reading, Ok(Value::Integer(1)));
    assert!(matches!(
        channels.observations.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(observer.loss_epoch(), 5); // Draining never acknowledges/reset loss.
    release.send(()).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap().sequence, 11);
    let resumed = channels.observations.try_recv().unwrap();
    assert_eq!(resumed.observation.reading, Ok(Value::Integer(10)));
    assert_eq!(channels.commands.loss_epoch(), 5);
    assert!(observer.stop_reason().is_none());

    channels.commands.shutdown();
    release.send(()).unwrap();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    assert_eq!(
        channels
            .observations
            .try_recv()
            .unwrap()
            .observation
            .reading,
        Ok(Value::Integer(11))
    );
    assert!(matches!(
        channels.observations.try_recv(),
        Err(mpsc::TryRecvError::Disconnected)
    ));
    assert_eq!(observer.loss_epoch(), 5);
    assert_eq!(trace.lock().unwrap().completed, 11);
}

#[test]
fn observation_overflow_is_local_to_one_worker() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut driver = FakeDriver::<1>::new(1, trace.clone());
    driver.gate = Some(gate);
    let mut device = DeviceInstance::new("one", driver).unwrap();
    device
        .set_poll_period("value", Some(Duration::from_nanos(1)))
        .unwrap();
    let (worker, channels) = PortWorker::new(FakeIo::new(trace), vec![device], 2, 1).unwrap();
    let join = thread::spawn(move || worker.run());
    calls.recv_timeout(WAIT).unwrap();
    for _ in 0..2 {
        release.send(()).unwrap();
        calls.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(channels.commands.loss_epoch(), 1);
    assert!(channels.commands.stop_reason().is_none());

    let trace = Arc::new(Mutex::new(Trace::default()));
    let device = DeviceInstance::new("one", FakeDriver::<2>::new(1, trace.clone())).unwrap();
    let (other, independent) = PortWorker::new(FakeIo::new(trace), vec![device], 2, 1).unwrap();
    let other_join = thread::spawn(move || other.run());
    assert_eq!(
        value(
            independent
                .commands
                .read(1, "one", "enabled", deadline())
                .unwrap()
        ),
        Value::Boolean(false)
    );
    assert_eq!(
        received(
            independent
                .commands
                .set(2, "one", "enabled", Value::Boolean(true), deadline())
                .unwrap()
        ),
        Ok(())
    );
    assert_eq!(independent.commands.loss_epoch(), 0);
    assert_eq!(independent.commands.port_state(), PortState::Synchronized);

    channels.observations.try_recv().unwrap();
    channels.commands.shutdown();
    release.send(()).unwrap();
    assert_eq!(join.join().unwrap(), StopReason::Shutdown);
    assert_eq!(channels.commands.loss_epoch(), 1);
    assert_eq!(
        value(
            independent
                .commands
                .read(3, "one", "enabled", deadline())
                .unwrap()
        ),
        Value::Boolean(true)
    );
    assert!(independent.commands.stop_reason().is_none());
    assert_eq!(independent.commands.loss_epoch(), 0);
    independent.commands.shutdown();
    assert_eq!(other_join.join().unwrap(), StopReason::Shutdown);
}

#[test]
fn observation_receiver_dropped_after_overflow_still_stops_worker() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut driver = FakeDriver::<1>::new(1, trace.clone());
    driver.gate = Some(gate);
    let mut device = DeviceInstance::new("one", driver).unwrap();
    device
        .set_poll_period("value", Some(Duration::from_nanos(1)))
        .unwrap();
    let (worker, channels) =
        PortWorker::new(FakeIo::new(trace.clone()), vec![device], 2, 1).unwrap();
    let join = thread::spawn(move || worker.run());
    calls.recv_timeout(WAIT).unwrap();
    for _ in 0..2 {
        release.send(()).unwrap();
        calls.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(channels.commands.loss_epoch(), 1);
    drop(channels.observations);
    let read = channels
        .commands
        .read(1, "one", "value", deadline())
        .unwrap();
    let write = channels
        .commands
        .set(2, "one", "enabled", Value::Boolean(true), deadline())
        .unwrap();
    release.send(()).unwrap();
    assert_eq!(join.join().unwrap(), StopReason::ObservationReceiverDropped);
    assert!(matches!(
        received(read),
        Err(CommandError::PortStopped(
            StopReason::ObservationReceiverDropped
        ))
    ));
    assert_eq!(
        received(write),
        Err(CommandError::PortStopped(
            StopReason::ObservationReceiverDropped
        ))
    );
    assert_eq!(
        channels.commands.stop_reason(),
        Some(StopReason::ObservationReceiverDropped)
    );
    assert_eq!(channels.commands.port_state(), PortState::Synchronized);
    assert_eq!(channels.commands.loss_epoch(), 1);
    assert_eq!(trace.lock().unwrap().calls.len(), 3);
}

#[test]
fn observation_overflow_does_not_mask_fatal_driver_errors() {
    for state in [PortState::Desynchronized, PortState::Disconnected] {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let (gate, calls, release) = gate();
        let mut driver = FakeDriver::<1>::new(1, trace.clone());
        driver.gate = Some(gate);
        driver.failures = [
            DriverError::new("measurement failed", PortState::Synchronized),
            DriverError::new("measurement failed", PortState::Synchronized),
            DriverError::new("physical failure", state),
        ]
        .into();
        let mut device = DeviceInstance::new("one", driver).unwrap();
        device
            .set_poll_period("value", Some(Duration::from_nanos(1)))
            .unwrap();
        let (worker, channels) =
            PortWorker::new(FakeIo::new(trace.clone()), vec![device], 2, 1).unwrap();
        let join = thread::spawn(move || worker.run());
        calls.recv_timeout(WAIT).unwrap();
        for _ in 0..2 {
            release.send(()).unwrap();
            calls.recv_timeout(WAIT).unwrap();
        }
        assert_eq!(channels.commands.loss_epoch(), 1);
        assert!(channels.commands.stop_reason().is_none());
        let read = channels
            .commands
            .read(1, "one", "value", deadline())
            .unwrap();
        let write = channels
            .commands
            .set(2, "one", "enabled", Value::Boolean(true), deadline())
            .unwrap();
        release.send(()).unwrap();
        assert!(
            matches!(join.join().unwrap(), StopReason::Driver(error) if error.port_state == state)
        );
        assert!(
            matches!(received(read), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
        );
        assert!(
            matches!(received(write), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
        );
        assert_eq!(channels.commands.port_state(), state);
        // The fatal measurement also did not fit; the physical failure remains
        // independently visible even though that observation cannot be delivered.
        assert_eq!(channels.observations.try_iter().count(), 1);
        assert_eq!(channels.commands.loss_epoch(), 2);
        assert_eq!(trace.lock().unwrap().calls.len(), 3);
    }
}

#[test]
fn panicking_port_does_not_stop_an_independent_worker() {
    let bad_trace = Arc::new(Mutex::new(Trace::default()));
    let mut driver = FakeDriver::<1>::new(1, bad_trace.clone());
    driver.panic = true;
    let (bad, bad_channels) = PortWorker::new(
        FakeIo::new(bad_trace),
        vec![DeviceInstance::new("one", driver).unwrap()],
        4,
        4,
    )
    .unwrap();
    let good_trace = Arc::new(Mutex::new(Trace::default()));
    let (good, good_channels) = PortWorker::new(
        FakeIo::new(good_trace.clone()),
        vec![DeviceInstance::new("one", FakeDriver::<2>::new(1, good_trace)).unwrap()],
        4,
        4,
    )
    .unwrap();
    let failed = bad_channels
        .commands
        .set(1, "one", "enabled", Value::Boolean(true), deadline())
        .unwrap();
    let queued = bad_channels
        .commands
        .read(2, "one", "value", deadline())
        .unwrap();
    let bad_join = thread::spawn(move || bad.run());
    let good_join = thread::spawn(move || good.run());
    assert_eq!(received(failed), Err(CommandError::OutcomeUnknown));
    assert!(matches!(
        received(queued),
        Err(CommandError::PortStopped(_))
    ));
    assert!(
        matches!(bad_join.join().unwrap(), StopReason::Driver(error) if error.port_state == PortState::Desynchronized)
    );
    for expected in 1..=3 {
        assert_eq!(
            value(
                good_channels
                    .commands
                    .read(expected, "one", "value", deadline())
                    .unwrap()
            ),
            Value::Integer(expected as i64)
        );
    }
    good_channels.commands.shutdown();
    assert_eq!(good_join.join().unwrap(), StopReason::Shutdown);
}

#[test]
fn invalid_commands_are_rejected_without_io() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let devices = vec![DeviceInstance::new("one", FakeDriver::<1>::new(1, trace.clone())).unwrap()];
    let (worker, channels) = PortWorker::new(FakeIo::new(trace.clone()), devices, 4, 4).unwrap();
    let join = thread::spawn(move || worker.run());
    assert!(matches!(
        received(
            channels
                .commands
                .read(1, "absent", "value", deadline())
                .unwrap()
        ),
        Err(CommandError::UnknownDevice)
    ));
    assert!(matches!(
        received(
            channels
                .commands
                .read(2, "one", "absent", deadline())
                .unwrap()
        ),
        Err(CommandError::UnknownParameter)
    ));
    assert_eq!(
        received(
            channels
                .commands
                .set(3, "one", "value", Value::Integer(1), deadline())
                .unwrap()
        ),
        Err(CommandError::ReadOnly)
    );
    assert_eq!(
        received(
            channels
                .commands
                .set(4, "one", "enabled", Value::Integer(1), deadline())
                .unwrap()
        ),
        Err(CommandError::InvalidValue)
    );
    channels.commands.shutdown();
    join.join().unwrap();
    assert!(trace.lock().unwrap().calls.is_empty());
}

#[test]
fn concurrent_submitters_racing_fatal_stop_have_no_hanging_requests() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (gate, calls, release) = gate();
    let mut driver = FakeDriver::<1>::new(1, trace.clone());
    driver.gate = Some(gate);
    driver.failures.push_back(DriverError::new(
        "lost synchronization",
        PortState::Desynchronized,
    ));
    let (worker, channels) = PortWorker::new(
        FakeIo::new(trace.clone()),
        vec![DeviceInstance::new("one", driver).unwrap()],
        16,
        8,
    )
    .unwrap();
    let started = channels
        .commands
        .set(1, "one", "enabled", Value::Boolean(true), deadline())
        .unwrap();
    let join = thread::spawn(move || worker.run());
    calls.recv_timeout(WAIT).unwrap();
    let queued = channels
        .commands
        .set(2, "one", "enabled", Value::Boolean(false), deadline())
        .unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let senders: Vec<_> = (0..8)
        .map(|sender| {
            let handle = channels.commands.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                (0..4)
                    .map(|index| {
                        handle.set(
                            3 + sender * 4 + index,
                            "one",
                            "enabled",
                            Value::Boolean(true),
                            deadline(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    barrier.wait();
    release.send(()).unwrap();
    assert!(matches!(join.join().unwrap(), StopReason::Driver(_)));
    assert_eq!(received(started), Err(CommandError::OutcomeUnknown));
    assert!(matches!(
        received(queued),
        Err(CommandError::PortStopped(_))
    ));
    for result in senders
        .into_iter()
        .flat_map(|sender| sender.join().unwrap())
    {
        match result {
            Ok(pending) => assert!(matches!(
                received(pending),
                Err(CommandError::PortStopped(_))
            )),
            Err(SubmitError::Full | SubmitError::Stopped(_)) => {}
        }
    }
    assert_eq!(trace.lock().unwrap().calls.len(), 1);
}

#[test]
fn fatal_poll_error_overrides_shutdown_and_rejects_queued_work() {
    for state in [PortState::Desynchronized, PortState::Disconnected] {
        let trace = Arc::new(Mutex::new(Trace::default()));
        let (gate, calls, release) = gate();
        let mut driver = FakeDriver::<1>::new(1, trace.clone());
        driver.gate = Some(gate);
        driver
            .failures
            .push_back(DriverError::new("fatal read", state));
        let mut device = DeviceInstance::new("one", driver).unwrap();
        device
            .set_poll_period("value", Some(Duration::from_secs(60)))
            .unwrap();
        let (worker, channels) =
            PortWorker::new(FakeIo::new(trace.clone()), vec![device], 2, 8).unwrap();
        let join = thread::spawn(move || worker.run());
        calls.recv_timeout(WAIT).unwrap();
        let read = channels
            .commands
            .read(1, "one", "value", deadline())
            .unwrap();
        let write = channels
            .commands
            .set(2, "one", "enabled", Value::Boolean(true), deadline())
            .unwrap();
        channels.commands.shutdown();
        release.send(()).unwrap();
        assert!(
            matches!(join.join().unwrap(), StopReason::Driver(error) if error.port_state == state)
        );
        assert!(
            matches!(received(read), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
        );
        assert!(
            matches!(received(write), Err(CommandError::PortStopped(StopReason::Driver(error))) if error.port_state == state)
        );
        let event = channels.observations.recv_timeout(WAIT).unwrap();
        assert_eq!(event.observation.reading, Err("fatal read".into()));
        assert_eq!(channels.commands.port_state(), state);
        assert_eq!(trace.lock().unwrap().calls.len(), 1);
    }
}

#[test]
fn invalid_catalogs_and_limits_are_rejected_before_io() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    assert!(DeviceInstance::new(" ", FakeDriver::<1>::new(1, trace.clone())).is_err());
    let mut device = DeviceInstance::new("one", FakeDriver::<1>::new(1, trace.clone())).unwrap();
    assert!(
        device
            .set_poll_period("value", Some(Duration::ZERO))
            .is_err()
    );
    assert!(
        device
            .set_poll_period("absent", Some(Duration::from_secs(1)))
            .is_err()
    );
    let duplicate = DeviceInstance::new("one", FakeDriver::<2>::new(2, trace.clone())).unwrap();
    assert!(PortWorker::new(FakeIo::new(trace.clone()), vec![device, duplicate], 1, 1).is_err());
    assert!(PortWorker::new(FakeIo::new(trace.clone()), vec![], 0, 1).is_err());
    assert!(PortWorker::new(FakeIo::new(trace.clone()), vec![], 1, 0).is_err());
    assert!(trace.lock().unwrap().calls.is_empty());
}
