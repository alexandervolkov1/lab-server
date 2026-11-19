//! Bounded, opt-in timing of the shared service mutex.

use std::sync::MutexGuard;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use super::{DeviceService, GateOperation};

const SUB_BUCKETS: usize = 8;
const BUCKETS: usize = 1 + u64::BITS as usize * SUB_BUCKETS;

impl GateOperation {
    const ALL: [Self; 6] = [
        Self::Manage,
        Self::ApplyEvent,
        Self::Tick,
        Self::ReceiveMessage,
        Self::StopControls,
        Self::TcpDrive,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Manage => "manage",
            Self::ApplyEvent => "apply_event",
            Self::Tick => "tick",
            Self::ReceiveMessage => "receive_message",
            Self::StopControls => "stop_controls",
            Self::TcpDrive => "tcp_drive",
        }
    }
}

type ServiceGuard<'a> = MutexGuard<'a, ()>;

pub(crate) struct GateGuard<'a> {
    guard: Option<ServiceGuard<'a>>,
    metrics: &'a OperationMetrics,
    wait: Duration,
    acquired: Instant,
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        let hold = self.acquired.elapsed();
        drop(self.guard.take());
        self.metrics.wait.record(self.wait);
        self.metrics.hold.record(hold);
        self.metrics.count.fetch_add(1, Relaxed);
    }
}

#[derive(Default)]
pub(super) struct GateMetrics {
    operations: [OperationMetrics; GateOperation::ALL.len()],
}

impl GateMetrics {
    pub(super) fn measure<'a>(
        &'a self,
        operation: GateOperation,
        acquire: impl FnOnce() -> ServiceGuard<'a>,
    ) -> GateGuard<'a> {
        let start = Instant::now();
        let guard = acquire();
        let acquired = Instant::now();
        GateGuard {
            guard: Some(guard),
            metrics: &self.operations[operation as usize],
            wait: acquired.duration_since(start),
            acquired,
        }
    }

    fn snapshot(&self) -> [GateMetric; GateOperation::ALL.len()] {
        GateOperation::ALL.map(|operation| {
            let metrics = &self.operations[operation as usize];
            let count = metrics.count.load(Relaxed);
            GateMetric {
                operation: operation.label(),
                count,
                wait: metrics.wait.snapshot(count),
                hold: metrics.hold.snapshot(count),
            }
        })
    }
}

/// Measurements for one kind of service lock acquisition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GateMetric {
    pub operation: &'static str,
    pub count: u64,
    pub wait: GateTiming,
    pub hold: GateTiming,
}

/// Durations in nanoseconds. Percentiles are histogram upper bounds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GateTiming {
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub total_ns: u64,
}

impl DeviceService {
    /// Snapshot without acquiring the service lock. Read after joining producers
    /// and clients for consistent counts, percentiles and totals.
    pub fn gate_metrics(&self) -> [GateMetric; GateOperation::ALL.len()] {
        self.gate_metrics.snapshot()
    }

    /// Start a fresh measurement series on an exclusively owned service.
    pub fn reset_gate_metrics(&mut self) {
        self.gate_metrics = GateMetrics::default();
    }
}

#[derive(Default)]
struct OperationMetrics {
    count: AtomicU64,
    wait: Histogram,
    hold: Histogram,
}

struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    max: AtomicU64,
    total: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKETS],
            max: AtomicU64::new(0),
            total: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    fn record(&self, duration: Duration) {
        let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.buckets[bucket(ns)].fetch_add(1, Relaxed);
        self.max.fetch_max(ns, Relaxed);
        self.total.fetch_add(ns, Relaxed);
    }

    fn snapshot(&self, count: u64) -> GateTiming {
        if count == 0 {
            return GateTiming::default();
        }
        let max = self.max.load(Relaxed);
        let ranks = [50, 95, 99].map(|p| (u128::from(count) * p).div_ceil(100) as u64);
        let mut percentiles = [max; 3];
        let mut seen = 0;
        let mut next = 0;
        for (bucket, entries) in self.buckets.iter().enumerate() {
            seen += entries.load(Relaxed);
            while next < ranks.len() && seen >= ranks[next] {
                percentiles[next] = upper_bound(bucket).min(max);
                next += 1;
            }
            if next == ranks.len() {
                break;
            }
        }
        let [p50_ns, p95_ns, p99_ns] = percentiles;
        GateTiming {
            p50_ns,
            p95_ns,
            p99_ns,
            max_ns: max,
            total_ns: self.total.load(Relaxed),
        }
    }
}

fn bucket(ns: u64) -> usize {
    if ns == 0 {
        return 0;
    }
    let exponent = ns.ilog2() as usize;
    let base = 1_u64 << exponent;
    let width = (base / SUB_BUCKETS as u64).max(1);
    1 + exponent * SUB_BUCKETS + ((ns - base) / width) as usize
}

fn upper_bound(bucket: usize) -> u64 {
    if bucket == 0 {
        return 0;
    }
    let offset = bucket - 1;
    let base = 1_u64 << (offset / SUB_BUCKETS);
    let width = (base / SUB_BUCKETS as u64).max(1);
    base + ((offset % SUB_BUCKETS + 1) as u64 * width - 1)
}
