//! Independent, bounded SQLite recorder. No hardware, TCP or subscription access.
//!
//! Admission never waits for SQLite or queue space. A short control mutex orders
//! try_send, loss accounting and shutdown; it is never held during database I/O.
//! Accepted means queued, not committed. Only successful `shutdown` confirms all
//! accepted records and the final loss snapshot were committed and SQLite closed.

use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use crate::model::{Observation, Value};
use crate::observation_aggregator::AggregatorEvent;

mod storage;
use storage::Storage;

pub const SCHEMA_VERSION: i32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataError {
    /// Signed Unix nanoseconds cannot represent this SystemTime (no wrapping).
    TimestampOutOfRange,
    /// SQLite cannot faithfully store NaN as a REAL value.
    NonFiniteFloat,
}

impl fmt::Display for DataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TimestampOutOfRange => "timestamp exceeds signed 64-bit Unix nanoseconds",
            Self::NonFiniteFloat => "measurement float must be finite",
        })
    }
}
impl std::error::Error for DataError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderError {
    InvalidOptions(&'static str),
    Sqlite {
        operation: &'static str,
        extended_code: Option<i32>,
        message: String,
    },
    UnsupportedSchema {
        application_id: i32,
        version: i32,
    },
    Data(DataError),
    ThreadSpawn(String),
    WriterPanicked,
    WriterDisconnected,
}

impl RecorderError {
    fn sqlite(operation: &'static str, error: rusqlite::Error) -> Self {
        Self::Sqlite {
            operation,
            extended_code: error.sqlite_error().map(|error| error.extended_code),
            message: error.to_string(),
        }
    }
}
impl fmt::Display for RecorderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions(message) => write!(f, "invalid recorder options: {message}"),
            Self::Sqlite {
                operation, message, ..
            } => write!(f, "recorder SQLite {operation}: {message}"),
            Self::UnsupportedSchema {
                application_id,
                version,
            } => write!(
                f,
                "unsupported recorder database: application_id={application_id}, version={version}"
            ),
            Self::Data(error) => error.fmt(f),
            Self::ThreadSpawn(message) => write!(f, "cannot start recorder writer: {message}"),
            Self::WriterPanicked => f.write_str("recorder writer panicked"),
            Self::WriterDisconnected => f.write_str("recorder writer disconnected unexpectedly"),
        }
    }
}
impl std::error::Error for RecorderError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderState {
    Running,
    Closing,
    Stopped,
    Failed(RecorderError),
}

/// Lifetime cumulative counts, never reset by a read or successful commit.
/// MAX means at least MAX; continuity remains unknown permanently.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LossInfo {
    pub dropped_events: u64,
    /// Audit facts not durably recorded: full/closed queue, invalid data, or
    /// uncommitted entries at writer failure. Includes subsequent failed attempts.
    pub dropped_audit_events: u64,
    /// Subset of dropped_events: aggregator notifications of PortWorker loss
    /// that could not themselves enter the recorder queue. Port details may be
    /// lost, but the fact that worker continuity was broken cannot disappear.
    pub dropped_data_lost_events: u64,
    /// Events rejected before enqueueing because their data cannot be stored.
    /// Separate from queue overflow, but also makes the session incomplete.
    pub rejected_invalid_data: u64,
}

impl LossInfo {
    pub fn is_incomplete(self) -> bool {
        self.dropped_events != 0
            || self.rejected_invalid_data != 0
            || self.dropped_audit_events != 0
    }

    fn record(&mut self, event: &AggregatorEvent) {
        self.dropped_events = self.dropped_events.saturating_add(1);
        if matches!(event, AggregatorEvent::DataLost { .. }) {
            self.dropped_data_lost_events = self.dropped_data_lost_events.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitError {
    Full,
    Closed,
    Failed(RecorderError),
    InvalidData(DataError),
}
impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("recorder queue is full; loss recorded"),
            Self::Closed => f.write_str("recorder is closing or stopped"),
            Self::Failed(error) => error.fmt(f),
            Self::InvalidData(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SubmitError {}

#[derive(Debug, Clone)]
pub struct RecorderOptions {
    pub queue_capacity: usize,
    pub audit_queue_capacity: usize,
    pub audit_enabled: bool,
    pub measurements_enabled: bool,
    pub batch_size: usize,
    /// Upper bound on accumulation time, not on filesystem latency.
    pub flush_interval: Duration,
    /// Finite SQLite lock wait on the writer thread only. Exhaustion is fatal.
    pub busy_timeout: Duration,
}
impl Default for RecorderOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            audit_queue_capacity: 1024,
            audit_enabled: false,
            measurements_enabled: true,
            batch_size: 128,
            flush_interval: Duration::from_millis(100),
            busy_timeout: Duration::from_millis(250),
        }
    }
}
impl RecorderOptions {
    fn validate(&self) -> Result<(), RecorderError> {
        if self.queue_capacity == 0 || self.audit_queue_capacity == 0 || self.batch_size == 0 {
            return Err(RecorderError::InvalidOptions(
                "queue_capacity, audit_queue_capacity and batch_size must be positive",
            ));
        }
        if self.flush_interval.is_zero()
            || Instant::now().checked_add(self.flush_interval).is_none()
        {
            return Err(RecorderError::InvalidOptions(
                "flush_interval must be positive and representable",
            ));
        }
        if self.busy_timeout.as_millis() > i32::MAX as u128 {
            return Err(RecorderError::InvalidOptions(
                "busy_timeout exceeds SQLite's millisecond range",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingSummary {
    pub session_id: i64,
    pub accepted_records: u64,
    pub audit_records: u64,
    pub losses: LossInfo,
}

struct Control {
    state: RecorderState,
    audit_pending: u64,
    losses: LossInfo,
}
type Shared = Arc<Mutex<Control>>;

fn control(shared: &Shared) -> MutexGuard<'_, Control> {
    // No user callbacks or I/O run under this lock. Preserve diagnostics even
    // if unwinding interrupted a control operation.
    shared.lock().unwrap_or_else(|poison| poison.into_inner())
}

struct Entry {
    event: AggregatorEvent,
    registered_at_ns: i64,
}
#[derive(Clone)]
pub struct RecorderHandle {
    sender: mpsc::SyncSender<Entry>,
    audit_sender: mpsc::SyncSender<crate::audit::Event>,
    wake_sender: mpsc::SyncSender<()>,
    operations: Arc<std::sync::atomic::AtomicU64>,
    pub audit_enabled: bool,
    pub measurements_enabled: bool,
    shared: Shared,
    session_id: i64,
}

impl RecorderHandle {
    fn wake_writer(&self) {
        // A full channel already holds a wakeup. A disconnected receiver
        // means the writer has exited its loop and handles the outcome itself.
        let _ = self.wake_sender.try_send(());
    }

    pub(crate) fn next_operation(&self) -> u64 {
        self.operations
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |n| n.checked_add(1),
            )
            .expect("operation id exhausted")
    }
    pub(crate) fn audit_lost(&self) {
        if self.audit_enabled {
            let mut c = control(&self.shared);
            c.losses.dropped_audit_events = c.losses.dropped_audit_events.saturating_add(1);
        }
    }
    pub(crate) fn audit_event(&self, event: crate::audit::Event) {
        if !self.audit_enabled {
            return;
        }
        let mut c = control(&self.shared);
        if event.context.len().saturating_add(event.details.len()) > 128 * 1024
            || c.state != RecorderState::Running
            || self.audit_sender.try_send(event).is_err()
        {
            c.losses.dropped_audit_events = c.losses.dropped_audit_events.saturating_add(1);
        } else {
            c.audit_pending = c.audit_pending.saturating_add(1);
            self.wake_writer();
        }
    }

    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    /// Consumes an existing aggregator event; clone it first to also feed TCP.
    /// Full means NOT accepted and increments the out-of-queue loss counters.
    /// InvalidData increments rejected_invalid_data, independently of overflow.
    /// Closed/Failed mean NOT accepted and do not change the loss counters.
    pub fn try_record(&self, event: AggregatorEvent) -> Result<(), SubmitError> {
        if !self.measurements_enabled {
            return Ok(());
        }
        let mut shared = control(&self.shared);
        match &shared.state {
            RecorderState::Running => {}
            RecorderState::Failed(error) => return Err(SubmitError::Failed(error.clone())),
            _ => return Err(SubmitError::Closed),
        }
        let validate = || {
            if let AggregatorEvent::Measurement { observation, .. } = &event {
                unix_ns(observation.completed_at)?;
                if matches!(observation.reading, Ok(Value::Float(value)) if !value.is_finite()) {
                    return Err(DataError::NonFiniteFloat);
                }
            }
            unix_ns(SystemTime::now())
        };
        let registered_at_ns = validate().map_err(|error| {
            shared.losses.rejected_invalid_data =
                shared.losses.rejected_invalid_data.saturating_add(1);
            SubmitError::InvalidData(error)
        })?;
        match self.sender.try_send(Entry {
            event,
            registered_at_ns,
        }) {
            Ok(()) => {
                self.wake_writer();
                Ok(())
            }
            Err(mpsc::TrySendError::Full(entry)) => {
                shared.losses.record(&entry.event);
                Err(SubmitError::Full)
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                let error = RecorderError::WriterDisconnected;
                shared.state = RecorderState::Failed(error.clone());
                Err(SubmitError::Failed(error))
            }
        }
    }

    pub fn try_observation(
        &self,
        port_id: impl Into<String>,
        global_key: impl Into<String>,
        observation: Observation,
    ) -> Result<(), SubmitError> {
        self.try_record(AggregatorEvent::Measurement {
            port_id: port_id.into(),
            global_key: global_key.into(),
            observation,
        })
    }

    pub fn state(&self) -> RecorderState {
        control(&self.shared).state.clone()
    }

    pub fn losses(&self) -> LossInfo {
        control(&self.shared).losses
    }
}

pub struct Recorder {
    handle: RecorderHandle,
    writer: Option<JoinHandle<Result<RecordingSummary, RecorderError>>>,
}

impl Recorder {
    /// Waits for schema validation/creation and session creation on the writer.
    pub fn open(path: impl AsRef<Path>, queue_capacity: usize) -> Result<Self, RecorderError> {
        Self::with_options(
            path,
            RecorderOptions {
                queue_capacity,
                ..RecorderOptions::default()
            },
        )
    }

    pub fn with_options(
        path: impl AsRef<Path>,
        options: RecorderOptions,
    ) -> Result<Self, RecorderError> {
        Self::start(path.as_ref(), options, Hooks::default())
    }

    pub(crate) fn start(
        path: &Path,
        options: RecorderOptions,
        mut hooks: Hooks,
    ) -> Result<Self, RecorderError> {
        options.validate()?;
        let path = path.to_owned();
        let (sender, receiver) = mpsc::sync_channel(options.queue_capacity);
        let (audit_sender, audit_receiver) = mpsc::sync_channel(options.audit_queue_capacity);
        let (wake_sender, wake_receiver) = mpsc::sync_channel(1);
        let audit_enabled = options.audit_enabled;
        let measurements_enabled = options.measurements_enabled;
        let shared = Arc::new(Mutex::new(Control {
            state: RecorderState::Running,
            audit_pending: 0,
            losses: LossInfo::default(),
        }));
        let writer_shared = shared.clone();
        let (ready, started) = mpsc::sync_channel(1);
        let writer = thread::Builder::new()
            .name("sqlite-recorder".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let storage = Storage::open(
                        &path,
                        options.busy_timeout,
                        audit_enabled,
                        measurements_enabled,
                    )?;
                    let _ = ready.send(Ok(storage.session_id));
                    write_loop(
                        storage,
                        receiver,
                        audit_receiver,
                        wake_receiver,
                        &writer_shared,
                        options,
                        &mut hooks,
                    )
                }))
                .unwrap_or(Err(RecorderError::WriterPanicked));
                let mut c = control(&writer_shared);
                if result.is_err() {
                    c.losses.dropped_audit_events = c
                        .losses
                        .dropped_audit_events
                        .saturating_add(c.audit_pending);
                    c.audit_pending = 0;
                }
                c.state = match &result {
                    Ok(_) => RecorderState::Stopped,
                    Err(error) => RecorderState::Failed(error.clone()),
                };
                drop(c);
                #[cfg(test)]
                if let Some(stopped) = hooks.stopped.take() {
                    let _ = stopped.send(());
                }
                if let Err(error) = &result {
                    let _ = ready.try_send(Err(error.clone()));
                }
                result
            })
            .map_err(|error| RecorderError::ThreadSpawn(error.to_string()))?;
        match started.recv() {
            Ok(Ok(session_id)) => Ok(Self {
                handle: RecorderHandle {
                    sender,
                    audit_sender,
                    wake_sender,
                    operations: Arc::new(std::sync::atomic::AtomicU64::new(1)),
                    audit_enabled,
                    measurements_enabled,
                    shared,
                    session_id,
                },
                writer: Some(writer),
            }),
            result => {
                let _ = writer.join();
                Err(result
                    .ok()
                    .and_then(Result::err)
                    .unwrap_or(RecorderError::WriterDisconnected))
            }
        }
    }

    pub fn handle(&self) -> RecorderHandle {
        self.handle.clone()
    }

    /// Stops admission atomically, drains accepted entries, commits the final
    /// loss snapshot/end marker, closes SQLite and joins the writer. Handles
    /// may outlive the owner and continue to expose final state and losses.
    pub fn shutdown(mut self) -> Result<RecordingSummary, RecorderError> {
        self.finish()
    }

    fn finish(&mut self) -> Result<RecordingSummary, RecorderError> {
        {
            let mut shared = control(&self.handle.shared);
            if shared.state == RecorderState::Running {
                shared.state = RecorderState::Closing;
            }
            self.handle.wake_writer();
        }
        self.writer
            .take()
            .expect("writer joined once")
            .join()
            .unwrap_or(Err(RecorderError::WriterPanicked))
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        if self.writer.is_some() {
            // Best effort only: explicit shutdown is needed to receive errors.
            let _ = self.finish();
        }
    }
}

pub(crate) fn unix_ns(time: SystemTime) -> Result<i64, DataError> {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration_ns(duration, false),
        Err(error) => duration_ns(error.duration(), true),
    }
}

fn duration_ns(duration: Duration, before_epoch: bool) -> Result<i64, DataError> {
    let signed = i128::try_from(duration.as_nanos()).ok().and_then(|value| {
        if before_epoch {
            value.checked_neg()
        } else {
            Some(value)
        }
    });
    signed
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(DataError::TimestampOutOfRange)
}

// Test-only synchronization at the wait and transaction boundaries.
#[derive(Default)]
pub(crate) struct Hooks {
    #[cfg(test)]
    pub(crate) before_wait: Option<Box<dyn FnMut() + Send>>,
    #[cfg(test)]
    pub(crate) before_flush: Option<Box<dyn FnMut() + Send>>,
    #[cfg(test)]
    pub(crate) after_flush: Option<Box<dyn FnMut() + Send>>,
    #[cfg(test)]
    pub(crate) stopped: Option<mpsc::Sender<()>>,
}
impl Hooks {
    fn before_wait(&mut self) {
        #[cfg(test)]
        if let Some(hook) = &mut self.before_wait {
            hook();
        }
    }

    fn before_flush(&mut self) {
        #[cfg(test)]
        if let Some(hook) = &mut self.before_flush {
            hook();
        }
    }
    fn after_flush(&mut self) {
        #[cfg(test)]
        if let Some(hook) = &mut self.after_flush {
            hook();
        }
    }
}

fn write_loop(
    mut storage: Storage,
    receiver: mpsc::Receiver<Entry>,
    audit_receiver: mpsc::Receiver<crate::audit::Event>,
    wake_receiver: mpsc::Receiver<()>,
    shared: &Shared,
    options: RecorderOptions,
    hooks: &mut Hooks,
) -> Result<RecordingSummary, RecorderError> {
    let mut batch = Vec::with_capacity(options.batch_size);
    let mut audit_batch = Vec::with_capacity(options.batch_size);
    let mut audit_written = 0_u64;
    let mut deadline = Instant::now() + options.flush_interval;
    let mut written = 0_u64;
    let mut persisted_loss = LossInfo::default();
    loop {
        let closing = control(shared).state == RecorderState::Closing;
        // One from each queue per turn: neither traffic class can starve the
        // other. After Closing, no producer can enqueue behind either check.
        let audit_next = audit_receiver.try_recv().ok();
        let next = receiver.try_recv().ok();
        let idle = audit_next.is_none() && next.is_none();
        let drained = closing && idle;
        if let Some(event) = audit_next {
            audit_batch.push(event);
        }
        if let Some(entry) = next {
            batch.push(entry);
        }
        if batch.len() == options.batch_size
            || audit_batch.len() == options.batch_size
            || drained
            || Instant::now() >= deadline
        {
            let losses = control(shared).losses;
            if !batch.is_empty() || !audit_batch.is_empty() || losses != persisted_loss {
                hooks.before_flush();
                storage.write_batch(&batch, &audit_batch, losses, losses != persisted_loss)?;
                audit_written = audit_written.saturating_add(audit_batch.len() as u64);
                control(shared).audit_pending -= audit_batch.len() as u64;
                audit_batch.clear();
                written = written.saturating_add(batch.len() as u64);
                batch.clear();
                persisted_loss = losses;
                hooks.after_flush();
            }
            deadline = Instant::now() + options.flush_interval;
        }
        if drained {
            let summary = RecordingSummary {
                session_id: storage.session_id,
                accepted_records: written,
                audit_records: audit_written,
                losses: persisted_loss,
            };
            storage.finish(persisted_loss.dropped_audit_events == 0)?;
            return Ok(summary);
        }
        if idle {
            // Enqueues signal after publishing data. A signal racing with the
            // empty checks stays pending until this wait, and every wake or
            // timeout leads back to both queues and the flush deadline.
            hooks.before_wait();
            if wake_receiver.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                == Err(mpsc::RecvTimeoutError::Disconnected)
            {
                return Err(RecorderError::WriterDisconnected);
            }
        }
    }
}

#[cfg(test)]
mod tests;
