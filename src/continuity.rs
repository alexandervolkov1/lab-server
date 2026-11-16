//! Algorithm-independent acceptance boundary for a physical source incarnation.
use crate::model::SourceMetadata;

#[derive(Debug)]
pub struct Continuity {
    instance: u64,
    minimum_generation: u64,
    last_sequence: u64,
    stopped: bool,
}
impl Continuity {
    pub fn new(instance: u64) -> Self {
        Self {
            instance,
            minimum_generation: 0,
            last_sequence: 0,
            stopped: false,
        }
    }
    pub fn lost(&mut self, generation: u64) {
        self.minimum_generation = self.minimum_generation.max(generation);
    }
    pub fn stop(&mut self) {
        self.stopped = true;
    }
    /// Old queued samples remain recordable but cannot reinitialize processing.
    pub fn accept(&mut self, sample: &SourceMetadata) -> bool {
        if self.stopped
            || sample.source_instance != self.instance
            || sample.generation < self.minimum_generation
            || sample.generation == u64::MAX
            || sample.sequence <= self.last_sequence
        {
            return false;
        }
        self.minimum_generation = sample.generation;
        self.last_sequence = sample.sequence;
        true
    }
}
