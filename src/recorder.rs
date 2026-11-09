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

pub const SCHEMA_VERSION: i32 = 1;

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
    /// Subset of dropped_events: aggregator notifications of PortWorker loss
    /// that could not themselves enter the recorder queue. Port details may be
    /// lost, but the fact that worker continuity was broken cannot disappear.
    pub dropped_data_lost_events: u64,
}

impl LossInfo {
    pub fn is_incomplete(self) -> bool {
        self.dropped_events != 0
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
            batch_size: 128,
            flush_interval: Duration::from_millis(100),
            busy_timeout: Duration::from_millis(250),
        }
    }
}
impl RecorderOptions {
    fn validate(&self) -> Result<(), RecorderError> {
        if self.queue_capacity == 0 || self.batch_size == 0 {
            return Err(RecorderError::InvalidOptions(
                "queue_capacity and batch_size must be positive",
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
    pub losses: LossInfo,
}

struct Control {
    state: RecorderState,
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
enum Message {
    Event(Entry),
    Wake,
}

#[derive(Clone)]
pub struct RecorderHandle {
    sender: mpsc::SyncSender<Message>,
    shared: Shared,
    session_id: i64,
}

impl RecorderHandle {
    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    /// Consumes an existing aggregator event; clone it first to also feed TCP.
    /// Full means NOT accepted and increments the out-of-queue loss counters.
    /// InvalidData/Closed/Failed mean NOT accepted and do not count as overflow.
    pub fn try_record(&self, event: AggregatorEvent) -> Result<(), SubmitError> {
        let mut shared = control(&self.shared);
        match &shared.state {
            RecorderState::Running => {}
            RecorderState::Failed(error) => return Err(SubmitError::Failed(error.clone())),
            _ => return Err(SubmitError::Closed),
        }
        if let AggregatorEvent::Measurement { observation, .. } = &event {
            unix_ns(observation.completed_at).map_err(SubmitError::InvalidData)?;
            if matches!(observation.reading, Ok(Value::Float(value)) if !value.is_finite()) {
                return Err(SubmitError::InvalidData(DataError::NonFiniteFloat));
            }
        }
        let registered_at_ns = unix_ns(SystemTime::now()).map_err(SubmitError::InvalidData)?;
        match self.sender.try_send(Message::Event(Entry {
            event,
            registered_at_ns,
        })) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(Message::Event(entry))) => {
                shared.losses.record(&entry.event);
                Err(SubmitError::Full)
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                let error = RecorderError::WriterDisconnected;
                shared.state = RecorderState::Failed(error.clone());
                Err(SubmitError::Failed(error))
            }
            Err(mpsc::TrySendError::Full(Message::Wake)) => unreachable!(),
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

    fn start(
        path: &Path,
        options: RecorderOptions,
        mut hooks: Hooks,
    ) -> Result<Self, RecorderError> {
        options.validate()?;
        let path = path.to_owned();
        let (sender, receiver) = mpsc::sync_channel(options.queue_capacity);
        let shared = Arc::new(Mutex::new(Control {
            state: RecorderState::Running,
            losses: LossInfo::default(),
        }));
        let writer_shared = shared.clone();
        let (ready, started) = mpsc::sync_channel(1);
        let writer = thread::Builder::new()
            .name("sqlite-recorder".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let storage = Storage::open(&path, options.busy_timeout)?;
                    let _ = ready.send(Ok(storage.session_id));
                    write_loop(storage, receiver, &writer_shared, options, &mut hooks)
                }))
                .unwrap_or(Err(RecorderError::WriterPanicked));
                control(&writer_shared).state = match &result {
                    Ok(_) => RecorderState::Stopped,
                    Err(error) => RecorderState::Failed(error.clone()),
                };
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
            // Wake an idle writer; a full queue already gives it work to drain.
            let _ = self.handle.sender.try_send(Message::Wake);
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

fn unix_ns(time: SystemTime) -> Result<i64, DataError> {
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

// Test-only synchronization at the actual transaction boundary; no sleeps or
// simulated substitute database on the production write path.
#[derive(Default)]
struct Hooks {
    #[cfg(test)]
    before_flush: Option<Box<dyn FnMut() + Send>>,
    #[cfg(test)]
    after_flush: Option<Box<dyn FnMut() + Send>>,
    #[cfg(test)]
    stopped: Option<mpsc::Sender<()>>,
}
impl Hooks {
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
    receiver: mpsc::Receiver<Message>,
    shared: &Shared,
    options: RecorderOptions,
    hooks: &mut Hooks,
) -> Result<RecordingSummary, RecorderError> {
    let mut batch = Vec::with_capacity(options.batch_size);
    let mut deadline = Instant::now() + options.flush_interval;
    let mut written = 0_u64;
    let mut persisted_loss = LossInfo::default();
    loop {
        let closing = control(shared).state == RecorderState::Closing;
        let mut drained = false;
        let next = if closing {
            match receiver.try_recv() {
                Ok(message) => Some(message),
                Err(_) => {
                    drained = true;
                    None
                }
            }
        } else {
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(message) => Some(message),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    drained = true;
                    None
                }
            }
        };
        if let Some(Message::Event(entry)) = next {
            batch.push(entry);
        }
        if batch.len() == options.batch_size || drained || Instant::now() >= deadline {
            let losses = control(shared).losses;
            if !batch.is_empty() || losses != persisted_loss {
                hooks.before_flush();
                storage.write_batch(&batch, losses, losses != persisted_loss)?;
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
                losses: persisted_loss,
            };
            storage.finish()?;
            return Ok(summary);
        }
    }
}

#[cfg(test)]
mod tests;
