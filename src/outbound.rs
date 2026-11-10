//! Bounded FIFO of complete serialized messages, without wire delimiters.
use std::collections::VecDeque;
use std::io;

#[derive(Debug, PartialEq, Eq)]
pub enum OutputError {
    InvalidLine,
    Full,
}

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to queue TCP response: {self:?}")
    }
}

impl std::error::Error for OutputError {}

pub(crate) fn is_output_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<OutputError>())
}

/// Each message costs its UTF-8 byte length plus one reserved framing byte.
/// This keeps the existing TCP limit (64 KiB including LF). Message transports
/// release that whole charge on pop; TCP releases only bytes actually written.
/// The front text is retained during a partial write, so retained payload is
/// bounded by the queue budget plus at most one partially written message.
pub struct OutboundBuffer {
    max_bytes: usize,
    pub(crate) messages: VecDeque<String>,
    pub(crate) remaining_bytes: usize,
    pub(crate) front_offset: usize,
}

impl OutboundBuffer {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            messages: VecDeque::new(),
            remaining_bytes: 0,
            front_offset: 0,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.queued_bytes()
    }

    pub fn queued_bytes(&self) -> usize {
        self.remaining_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// The session supplies compact JSON. Retain the legacy nonempty, LF-free
    /// text validation for callers of OutboundBuffer::enqueue_line as well.
    pub fn enqueue_message(&mut self, text: &str) -> Result<(), OutputError> {
        if text.is_empty() || text.as_bytes().contains(&b'\n') {
            return Err(OutputError::InvalidLine);
        }
        let needed = text.len().checked_add(1).ok_or(OutputError::Full)?;
        if needed > self.max_bytes.saturating_sub(self.remaining_bytes) {
            return Err(OutputError::Full);
        }
        self.messages.push_back(text.to_owned());
        self.remaining_bytes += needed;
        Ok(())
    }

    /// Take a complete message exactly once. Switching transports in the middle
    /// of an NDJSON write is rejected instead of duplicating its sent prefix.
    pub fn pop_message(&mut self) -> io::Result<Option<String>> {
        if self.front_offset != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot take a message after a partial stream write",
            ));
        }
        let message = self.messages.pop_front();
        if let Some(text) = &message {
            self.remaining_bytes -= text.len() + 1;
        }
        Ok(message)
    }
}
