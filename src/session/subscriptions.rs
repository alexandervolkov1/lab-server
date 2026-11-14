//! Atomic validation of the per-session subscription set.
use crate::model::ParameterDescription;
use std::collections::HashMap;

const MAX_SUBSCRIPTIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionChange {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    UnknownKey(String),
    NotSubscribable(String),
    Full,
}

#[derive(Default)]
pub struct SubscriptionSet {
    keys: std::collections::BTreeSet<String>,
}

impl SubscriptionSet {
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    pub fn keys(&self) -> Vec<String> {
        self.keys.iter().cloned().collect()
    }

    pub fn apply(
        &mut self,
        change: SubscriptionChange,
        keys: &[String],
        catalog: &HashMap<String, ParameterDescription>,
    ) -> Result<Vec<String>, SubscriptionError> {
        let mut next = self.keys.clone();

        for key in keys {
            let Some(description) = catalog.get(key) else {
                return Err(SubscriptionError::UnknownKey(key.clone()));
            };

            if description.default_poll_period.is_none() {
                return Err(SubscriptionError::NotSubscribable(key.clone()));
            }

            match change {
                SubscriptionChange::Subscribe => {
                    next.insert(key.clone());
                }

                SubscriptionChange::Unsubscribe => {
                    next.remove(key);
                }
            }
        }

        if next.len() > MAX_SUBSCRIPTIONS {
            return Err(SubscriptionError::Full);
        }

        self.keys = next;
        Ok(self.keys())
    }
}
