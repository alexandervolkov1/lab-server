use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const MAX_CLIENTS: usize = 32;

#[derive(Clone)]
pub struct ClientSlots(Arc<AtomicUsize>);

pub struct ClientSlot(Arc<AtomicUsize>);

impl ClientSlots {
    pub fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    pub fn try_acquire(&self) -> Option<ClientSlot> {
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < MAX_CLIENTS).then_some(count + 1)
            })
            .ok()?;
        Some(ClientSlot(self.0.clone()))
    }
}

impl Drop for ClientSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_clients_and_reuses_released_slot() {
        let slots = ClientSlots::new();
        let mut clients: Vec<_> = (0..MAX_CLIENTS)
            .map(|_| slots.try_acquire().unwrap())
            .collect();
        assert!(slots.try_acquire().is_none());
        clients.pop();
        let replacement = slots.try_acquire().unwrap();
        assert!(slots.try_acquire().is_none());
        drop(replacement);
        drop(clients);
        assert!(slots.try_acquire().is_some());
    }

    #[test]
    fn errors_and_panics_release_client_slot() {
        let slots = ClientSlots::new();
        let fail = || -> std::io::Result<()> {
            let _slot = slots.try_acquire().unwrap();
            Err(std::io::Error::other("connection setup failed"))
        };
        assert!(fail().is_err());
        let worker_slots = slots.clone();
        assert!(
            std::thread::spawn(move || {
                let _slot = worker_slots.try_acquire().unwrap();
                panic!("connection failed");
            })
            .join()
            .is_err()
        );
        let clients: Vec<_> = (0..MAX_CLIENTS)
            .map(|_| slots.try_acquire().unwrap())
            .collect();
        assert_eq!(clients.len(), MAX_CLIENTS);
    }
}
