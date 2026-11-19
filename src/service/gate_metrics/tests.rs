use super::*;
use crate::{command_router::CommandRouter, device_registry::DeviceRegistry};
use std::sync::{Barrier, TryLockError, mpsc};
use std::thread;

const WAIT: Duration = Duration::from_secs(10);

fn service() -> DeviceService {
    DeviceService::new(CommandRouter::new(DeviceRegistry::new()), 4)
}

fn metric(service: &DeviceService, operation: GateOperation) -> GateMetric {
    service.gate_metrics()[operation as usize]
}

#[test]
fn records_contention_only_for_the_waiting_acquisition_and_posts_on_unlock() {
    let service = service();
    let owner = service.lock_service(GateOperation::ApplyEvent);
    let (contended, wait_contended) = mpsc::channel();
    let (acquired, wait_acquired) = mpsc::channel();
    let (release, wait_release) = mpsc::channel();
    let rendezvous = Barrier::new(2);
    let minimum_wait = thread::scope(|scope| {
        let peer_service = &service;
        let rendezvous = &rendezvous;
        scope.spawn(move || {
            let _peer = peer_service
                .gate_metrics
                .measure(GateOperation::TcpDrive, || {
                    assert!(matches!(
                        peer_service.gate.try_lock(),
                        Err(TryLockError::WouldBlock)
                    ));
                    contended.send(Instant::now()).unwrap();
                    rendezvous.wait();
                    peer_service.gate.lock().unwrap()
                });
            acquired.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
        });

        let blocked_at = wait_contended.recv_timeout(WAIT).unwrap();
        rendezvous.wait();
        assert_eq!(metric(&service, GateOperation::ApplyEvent).count, 0);
        assert_eq!(metric(&service, GateOperation::TcpDrive).count, 0);
        assert!(matches!(
            wait_acquired.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let minimum_wait = blocked_at.elapsed();
        drop(owner);
        wait_acquired.recv_timeout(WAIT).unwrap();
        assert_eq!(metric(&service, GateOperation::ApplyEvent).count, 1);
        assert_eq!(metric(&service, GateOperation::TcpDrive).count, 0);
        release.send(()).unwrap();
        minimum_wait
    });
    let owner = metric(&service, GateOperation::ApplyEvent);
    let peer = metric(&service, GateOperation::TcpDrive);
    assert!(service.gate.try_lock().is_ok());
    assert_eq!(peer.count, 1);
    assert!(u128::from(peer.wait.total_ns) >= minimum_wait.as_nanos());
    assert!(u128::from(owner.hold.total_ns) >= minimum_wait.as_nanos());
    assert_eq!(peer.wait.total_ns, peer.wait.max_ns);
    assert_eq!(owner.hold.total_ns, owner.hold.max_ns);
}

#[test]
fn real_service_paths_record_an_early_error_and_normal_returns() {
    use crate::processing::{Management, State};

    let mut service = service();
    assert_eq!(
        service.manage(Management::State {
            name: "missing".into(),
            state: State::Shadow,
            expected_revision: 0,
        }),
        Err("unknown_instance".into())
    );
    service.tick_recorded(|_| {}).unwrap();
    let mut session = crate::session::Session::new(1, 1024);
    service
        .receive_message(&mut session, r#"{"id":1,"op":"ping"}"#, Instant::now())
        .unwrap();
    assert!(session.next_message().unwrap().is_some());
    service.stop_controls();
    for operation in [
        GateOperation::Manage,
        GateOperation::Tick,
        GateOperation::ReceiveMessage,
        GateOperation::StopControls,
    ] {
        let sample = metric(&service, operation);
        assert_eq!(sample.count, 1);
        assert_eq!(sample.hold.total_ns, sample.hold.max_ns);
        assert_eq!(sample.wait.total_ns, sample.wait.max_ns);
    }
    assert!(service.gate.try_lock().is_ok());
    service.reset_gate_metrics();
    assert!(
        service
            .gate_metrics()
            .iter()
            .all(|sample| sample.count == 0)
    );
}

#[test]
fn concurrent_acquisitions_count_each_sample_in_both_histograms() {
    const THREADS: usize = 6;
    const ACQUISITIONS: u64 = 257;
    let service = service();
    let start = Barrier::new(THREADS);
    thread::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|| {
                start.wait();
                for _ in 0..ACQUISITIONS {
                    let _gate = service.lock_service(GateOperation::ReceiveMessage);
                    std::hint::black_box(());
                }
            });
        }
    });
    let row = metric(&service, GateOperation::ReceiveMessage);
    let counters = &service.gate_metrics.operations[GateOperation::ReceiveMessage as usize];
    assert_eq!(row.count, THREADS as u64 * ACQUISITIONS);
    for histogram in [&counters.wait, &counters.hold] {
        assert_eq!(
            histogram
                .buckets
                .iter()
                .map(|n| n.load(Relaxed))
                .sum::<u64>(),
            row.count
        );
    }
    for timing in [row.wait, row.hold] {
        assert!(timing.p50_ns <= timing.p95_ns);
        assert!(timing.p95_ns <= timing.p99_ns);
        assert!(timing.p99_ns <= timing.max_ns);
        assert!(timing.max_ns <= timing.total_ns);
    }
}

#[test]
fn percentiles_use_bounded_buckets_and_keep_exact_maximum() {
    let histogram = Histogram::default();
    for (ns, count) in [(0, 50), (33, 45), (1024, 4), (10_000, 1)] {
        for _ in 0..count {
            histogram.record(Duration::from_nanos(ns));
        }
    }
    let timing = histogram.snapshot(100);
    assert_eq!(timing.p50_ns, 0);
    assert_eq!(timing.p95_ns, 35);
    assert_eq!(timing.p99_ns, 1151);
    assert_eq!(timing.max_ns, 10_000);
    assert_eq!(timing.total_ns, 33 * 45 + 1024 * 4 + 10_000);
    assert_eq!(upper_bound(bucket(u64::MAX)), u64::MAX);
}
