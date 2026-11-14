//! TCP contract (NDJSON on 127.0.0.1:7240, protocol version 1).
//!
//! Request IDs are positive u64 values, strictly increasing within a connection.
//! A valid ID is consumed even when the operation or its contents are rejected.
//! Repeated or smaller IDs produce `duplicate_id` WITHOUT an `id` field; the
//! original pending request remains eligible for its single response. Invalid
//! JSON, duplicate JSON fields and invalid IDs do not consume an ID. Clients
//! must reconnect before exhausting u64; independent connections have independent IDs.
//!
//! Set and invoke_action are validated against their catalogs before queueing.
//! Their ten-second deadlines include queue time. Starting a driver mutation and
//! cancelling the queued operation are mutually exclusive: `runtime_timeout`
//! means it did not start. Once started, a timeout, lost reply channel, or driver
//! failure produces `outcome_unknown`; clients must not assume the operation
//! failed or blindly retry.
//! An available definitive reply wins over timeout; late replies never produce
//! another response. Driver validation remains a second line of defense.
//! invoke_action accepts a global key and no arguments; describe adds a separate
//! actions array of {key, description}, preserving parameters and version 1.
//!
//! Subscription activation is linearized against the fanout publication counter,
//! independently for each newly subscribed key. Publications at or before that
//! boundary are excluded. The subscribe ACK precedes all events for new keys;
//! retained keys keep their boundary on an idempotent subscribe. Accepted events
//! preserve publication order. Unsubscribe ACK follows already queued events and
//! prevents further events for removed keys. A cached get is independent of this
//! stream and may return an earlier measurement.
//!
//! At most 32 clients have workers; excess connections are closed. Device commands
//! and upstream observations each have capacity 128. Command admission never
//! waits: a full queue returns `runtime_busy`, a closed queue `runtime_unavailable`.
//! Upstream observation overflow continues device polling but invalidates clients
//! with active subscriptions with terminal `observation_data_lost` (without request
//! ID). Per-client fanout overflow reports the same explicit loss of data after
//! the queued prefix. Reconnection creates a new stream. Clients without subscriptions
//! keep their TCP connection and pending commands; loss during that inactive period
//! does not invalidate a later subscription. Fanout skips inactive clients.
//! Loss during an active subscription remains pending across unsubscribe/subscribe;
//! neither changing subscriptions nor reading an internal error acknowledges delivery
//! of the loss notification. Such a stream still terminates with the loss error.
//! Closing a subscribed connection does not determine the outcome of a pending Set.
//! `observation_stream_closed` denotes closure without a detected overflow.
//! An exhausted output buffer or transport failure may only permit connection
//! closure; EOF is never proof of complete measurement delivery or Set failure.
//! There is no replay across reconnects. The per-client pending, subscription and
//! output limits remain 64, 64 and 64 KiB respectively.

use crate::model::{Observation, Value};
use serde_json::json;
use std::io;

pub const MAX_FRAME_BYTES: usize = 16 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    EmptyFrame,
    FrameTooLong,
    InvalidUtf8,
    IncompleteFrame,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FrameDecodeFailure {
    pub frames: Vec<String>,
    pub error: FrameError,
}

#[derive(Default)]
pub struct FrameDecoder {
    pending: Vec<u8>,
}

impl FrameDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, FrameDecodeFailure> {
        let mut frames = Vec::new();

        for chunk in bytes.split_inclusive(|&byte| byte == b'\n') {
            let has_lf = chunk.last() == Some(&b'\n');

            let data = if has_lf {
                &chunk[..chunk.len() - 1]
            } else {
                chunk
            };

            if data.len() > MAX_FRAME_BYTES - self.pending.len() {
                return Err(FrameDecodeFailure {
                    frames,
                    error: FrameError::FrameTooLong,
                });
            }

            self.pending.extend_from_slice(data);

            if has_lf {
                if self.pending.is_empty() {
                    return Err(FrameDecodeFailure {
                        frames,
                        error: FrameError::EmptyFrame,
                    });
                }

                let line = match std::str::from_utf8(&self.pending) {
                    Ok(line) => line.to_owned(),
                    Err(_) => {
                        return Err(FrameDecodeFailure {
                            frames,
                            error: FrameError::InvalidUtf8,
                        });
                    }
                };

                frames.push(line);
                self.pending.clear();
            }
        }

        Ok(frames)
    }

    pub fn finish(&self) -> Result<(), FrameError> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(FrameError::IncompleteFrame)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum JsonError {
    InvalidJson,
    ExpectedObject,
    DuplicateKey,
}

struct UniqueJsonObject(serde_json::Map<String, serde_json::Value>);

impl<'de> serde::Deserialize<'de> for UniqueJsonObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UniqueObjectVisitor;

        impl<'de> serde::de::Visitor<'de> for UniqueObjectVisitor {
            type Value = UniqueJsonObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut fields = serde_json::Map::new();

                while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                    if fields.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }

                    fields.insert(key, value);
                }

                Ok(UniqueJsonObject(fields))
            }
        }

        deserializer.deserialize_map(UniqueObjectVisitor)
    }
}

pub fn parse_json_object(
    frame: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, JsonError> {
    match serde_json::from_str::<UniqueJsonObject>(frame) {
        Ok(UniqueJsonObject(fields)) => Ok(fields),
        Err(_) => match serde_json::from_str::<serde_json::Value>(frame) {
            Ok(serde_json::Value::Object(_)) => Err(JsonError::DuplicateKey),
            Ok(_) => Err(JsonError::ExpectedObject),
            Err(_) => Err(JsonError::InvalidJson),
        },
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RequestOp {
    Hello,
    Describe,
    Get,
    Set,
    InvokeAction,
    Subscribe,
    Unsubscribe,
    Ping,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RequestHeader {
    pub id: u64,
    pub op: RequestOp,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RequestError {
    InvalidId,
    UnsupportedOp,
    InvalidKey,
    InvalidValue,
    InvalidKeys,
}

pub fn parse_request_header(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<RequestHeader, RequestError> {
    let id = object
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .filter(|id| *id > 0)
        .ok_or(RequestError::InvalidId)?;

    let op = match object.get("op").and_then(serde_json::Value::as_str) {
        Some("hello") => RequestOp::Hello,
        Some("describe") => RequestOp::Describe,
        Some("get") => RequestOp::Get,
        Some("set") => RequestOp::Set,
        Some("invoke_action") => RequestOp::InvokeAction,
        Some("subscribe") => RequestOp::Subscribe,
        Some("unsubscribe") => RequestOp::Unsubscribe,
        Some("ping") => RequestOp::Ping,
        _ => return Err(RequestError::UnsupportedOp),
    };

    Ok(RequestHeader { id, op })
}

pub fn parse_parameter_key(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, RequestError> {
    object
        .get("key")
        .and_then(serde_json::Value::as_str)
        .filter(|key| !key.is_empty())
        .map(|key| key.to_owned())
        .ok_or(RequestError::InvalidKey)
}

pub fn parse_set_value(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<crate::model::Value, RequestError> {
    if let Some(value) = object.get("value").and_then(serde_json::Value::as_bool) {
        return Ok(crate::model::Value::Boolean(value));
    }

    let number = object
        .get("value")
        .and_then(serde_json::Value::as_number)
        .ok_or(RequestError::InvalidValue)?;

    let raw = number.as_str();

    if raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
        raw.parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(crate::model::Value::Float)
            .ok_or(RequestError::InvalidValue)
    } else {
        raw.parse::<i64>()
            .map(crate::model::Value::Integer)
            .map_err(|_| RequestError::InvalidValue)
    }
}

pub fn parse_subscription_keys(
    object: &serde_json::Map<String, serde_json::Value>,
    op: &RequestOp,
) -> Result<Vec<String>, RequestError> {
    if !matches!(op, &RequestOp::Subscribe | &RequestOp::Unsubscribe) {
        return Err(RequestError::InvalidKeys);
    }

    let mut keys = match (object.get("key"), object.get("keys")) {
        (None, Some(value)) => {
            let items = value.as_array().ok_or(RequestError::InvalidKeys)?;
            let mut keys = Vec::with_capacity(items.len());

            for value in items {
                let key = value
                    .as_str()
                    .filter(|key| !key.is_empty())
                    .ok_or(RequestError::InvalidKeys)?;

                keys.push(key.to_owned());
            }

            keys
        }

        (Some(value), None) if matches!(op, &RequestOp::Subscribe) => {
            let key = value
                .as_str()
                .filter(|key| !key.is_empty())
                .ok_or(RequestError::InvalidKeys)?;

            vec![key.to_owned()]
        }

        _ => return Err(RequestError::InvalidKeys),
    };

    keys.sort_unstable();
    keys.dedup();

    Ok(keys)
}

#[derive(Debug, PartialEq)]
pub enum Request {
    Hello {
        id: u64,
    },
    Describe {
        id: u64,
    },
    Ping {
        id: u64,
    },
    Get {
        id: u64,
        key: String,
    },
    Set {
        id: u64,
        key: String,
        value: crate::model::Value,
    },
    InvokeAction {
        id: u64,
        key: String,
    },
    Subscribe {
        id: u64,
        keys: Vec<String>,
    },
    Unsubscribe {
        id: u64,
        keys: Vec<String>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    Json(JsonError),
    Request(RequestError),
}

pub fn parse_request(frame: &str) -> Result<Request, ParseError> {
    let object = parse_json_object(frame).map_err(ParseError::Json)?;

    let RequestHeader { id, op } = parse_request_header(&object).map_err(ParseError::Request)?;

    match op {
        RequestOp::Hello => Ok(Request::Hello { id }),
        RequestOp::Describe => Ok(Request::Describe { id }),
        RequestOp::Ping => Ok(Request::Ping { id }),

        RequestOp::Get => {
            let key = parse_parameter_key(&object).map_err(ParseError::Request)?;
            Ok(Request::Get { id, key })
        }

        RequestOp::InvokeAction => {
            let key = parse_parameter_key(&object).map_err(ParseError::Request)?;
            Ok(Request::InvokeAction { id, key })
        }

        RequestOp::Set => {
            let key = parse_parameter_key(&object).map_err(ParseError::Request)?;
            let value = parse_set_value(&object).map_err(ParseError::Request)?;

            Ok(Request::Set { id, key, value })
        }

        RequestOp::Subscribe => {
            let keys = parse_subscription_keys(&object, &RequestOp::Subscribe)
                .map_err(ParseError::Request)?;

            Ok(Request::Subscribe { id, keys })
        }

        RequestOp::Unsubscribe => {
            let keys = parse_subscription_keys(&object, &RequestOp::Unsubscribe)
                .map_err(ParseError::Request)?;

            Ok(Request::Unsubscribe { id, keys })
        }
    }
}

/// Compatibility names for protocol-v1 TCP callers.
pub use Request as TcpRequest;
pub use parse_request as parse_tcp_request;

pub fn observation_json(id: Option<u64>, observation: &Observation) -> io::Result<String> {
    let duration = observation
        .completed_at
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?;

    let timestamp_ms = u64::try_from(duration.as_millis()).map_err(io::Error::other)?;

    let mut message = match &observation.reading {
        Ok(value) => {
            let (json_value, unit) = match value {
                Value::Float(value) => (json!(value), observation.unit.as_str()),
                Value::Integer(value) => (json!(value), observation.unit.as_str()),
                Value::Boolean(value) => (json!(value), ""),
            };
            json!({
                "status": "ok",
                "key": &observation.source,
                "value": json_value,
                "unit": unit,
                "completed_at_unix_ms": timestamp_ms
            })
        }

        Err(error) => json!({
            "status": "error",
            "key": &observation.source,
            "error": error,
            "completed_at_unix_ms": timestamp_ms
        }),
    };

    match id {
        Some(id) => message["id"] = json!(id),
        None => message["event"] = json!("measurement"),
    }

    Ok(message.to_string())
}

#[cfg(test)]
mod tests;
