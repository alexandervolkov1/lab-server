//! Scoped, opt-in timing for Session request phases.

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use crate::service::{GateTiming, Histogram};

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Parse,
    Dispatch,
    Catalog,
    SnapshotBuild,
    Get,
    Availability,
    GetValue,
    Actions,
    Describe,
    DescribeSort,
    DescribeSerialize,
    Response,
}

impl Phase {
    const ALL: [(Self, &'static str); 12] = [
        (Self::Parse, "parse"),
        (Self::Dispatch, "dispatch"),
        (Self::Catalog, "catalog"),
        (Self::SnapshotBuild, "snapshot_build"),
        (Self::Get, "get"),
        (Self::Availability, "availability"),
        (Self::GetValue, "get_value"),
        (Self::Actions, "actions"),
        (Self::Describe, "describe"),
        (Self::DescribeSort, "describe_sort"),
        (Self::DescribeSerialize, "describe_serialize"),
        (Self::Response, "response"),
    ];
}

#[derive(Default)]
struct Counters {
    count: AtomicU64,
    duration: Histogram,
}

type Measurements = [Counters; Phase::ALL.len()];

thread_local! {
    static ACTIVE: RefCell<Option<Arc<Measurements>>> = const { RefCell::new(None) };
}

/// Optional phase collector. Threads opt in independently with [Self::record_on_thread].
#[derive(Clone, Default)]
pub struct RequestMetrics(Arc<Measurements>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestPhaseMetric {
    pub phase: &'static str,
    pub count: u64,
    pub timing: GateTiming,
}

impl RequestMetrics {
    /// Collect phases on this thread until the closure returns or unwinds.
    pub fn record_on_thread<R>(&self, run: impl FnOnce() -> R) -> R {
        let _restore = Restore(ACTIVE.replace(Some(self.0.clone())));
        run()
    }

    /// Take a consistent snapshot after the measured threads finish.
    pub fn snapshot(&self) -> [RequestPhaseMetric; Phase::ALL.len()] {
        Phase::ALL.map(|(phase, label)| {
            let counters = &self.0[phase as usize];
            let count = counters.count.load(Relaxed);
            RequestPhaseMetric {
                phase: label,
                count,
                timing: counters.duration.snapshot(count),
            }
        })
    }
}

struct Restore(Option<Arc<Measurements>>);

impl Drop for Restore {
    fn drop(&mut self) {
        ACTIVE.replace(self.0.take());
    }
}

pub(crate) struct Span(Phase, Instant);

pub(crate) fn span(phase: Phase) -> Option<Span> {
    ACTIVE.with_borrow(|active| active.as_ref().map(|_| Span(phase, Instant::now())))
}

impl Drop for Span {
    fn drop(&mut self) {
        let duration = self.1.elapsed();
        ACTIVE.with_borrow(|active| {
            if let Some(metrics) = active {
                let counters = &metrics[self.0 as usize];
                counters.duration.record(duration);
                counters.count.fetch_add(1, Relaxed);
            }
        });
    }
}
