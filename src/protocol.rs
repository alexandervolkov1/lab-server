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

    pub fn finish(self) -> Result<(), FrameError> {
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
pub enum TcpRequest {
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

pub fn parse_tcp_request(frame: &str) -> Result<TcpRequest, ParseError> {
    let object = parse_json_object(frame).map_err(ParseError::Json)?;

    let RequestHeader { id, op } = parse_request_header(&object).map_err(ParseError::Request)?;

    match op {
        RequestOp::Hello => Ok(TcpRequest::Hello { id }),
        RequestOp::Describe => Ok(TcpRequest::Describe { id }),
        RequestOp::Ping => Ok(TcpRequest::Ping { id }),

        RequestOp::Get => {
            let key = parse_parameter_key(&object).map_err(ParseError::Request)?;
            Ok(TcpRequest::Get { id, key })
        }

        RequestOp::Set => {
            let key = parse_parameter_key(&object).map_err(ParseError::Request)?;
            let value = parse_set_value(&object).map_err(ParseError::Request)?;

            Ok(TcpRequest::Set { id, key, value })
        }

        RequestOp::Subscribe => {
            let keys = parse_subscription_keys(&object, &RequestOp::Subscribe)
                .map_err(ParseError::Request)?;

            Ok(TcpRequest::Subscribe { id, keys })
        }

        RequestOp::Unsubscribe => {
            let keys = parse_subscription_keys(&object, &RequestOp::Unsubscribe)
                .map_err(ParseError::Request)?;

            Ok(TcpRequest::Unsubscribe { id, keys })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_frame_split_across_reads() {
        let mut decoder = FrameDecoder::default();

        assert!(decoder.push(b"{\"id\":").unwrap().is_empty());

        assert_eq!(
            decoder.push(b"1}\n").unwrap(),
            vec![r#"{"id":1}"#.to_owned()]
        );

        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn accepts_two_frames_in_one_read() {
        let mut decoder = FrameDecoder::default();

        assert_eq!(
            decoder.push(b"{\"id\":1}\n{\"id\":2}\n").unwrap(),
            vec![r#"{"id":1}"#.to_owned(), r#"{"id":2}"#.to_owned()]
        );
    }

    #[test]
    fn preserves_tail_after_complete_frame() {
        let mut decoder = FrameDecoder::default();

        assert_eq!(
            decoder.push(b"{\"id\":1}\n{\"id\":").unwrap(),
            vec![r#"{"id":1}"#.to_owned()]
        );

        assert_eq!(
            decoder.push(b"2}\n").unwrap(),
            vec![r#"{"id":2}"#.to_owned()]
        );
    }

    #[test]
    fn accepts_utf8_split_across_reads() {
        let mut decoder = FrameDecoder::default();
        let bytes = "{\"key\":\"температура\"}\n".as_bytes();
        let split = bytes.iter().position(|&byte| byte >= 0x80).unwrap() + 1;

        assert!(decoder.push(&bytes[..split]).unwrap().is_empty());

        assert_eq!(
            decoder.push(&bytes[split..]).unwrap(),
            vec![r#"{"key":"температура"}"#.to_owned()]
        );
    }

    #[test]
    fn rejects_empty_frame() {
        let mut decoder = FrameDecoder::default();

        let failure = decoder.push(b"\n").unwrap_err();
        assert!(failure.frames.is_empty());
        assert_eq!(failure.error, FrameError::EmptyFrame);
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut decoder = FrameDecoder::default();

        assert!(decoder.push(&[0xff]).unwrap().is_empty());

        let failure = decoder.push(b"\n").unwrap_err();
        assert!(failure.frames.is_empty());
        assert_eq!(failure.error, FrameError::InvalidUtf8);
    }

    #[test]
    fn enforces_max_frame_bytes() {
        let mut decoder = FrameDecoder::default();

        assert!(
            decoder
                .push(&vec![b'x'; MAX_FRAME_BYTES])
                .unwrap()
                .is_empty()
        );

        assert_eq!(
            decoder.push(b"\n").unwrap(),
            vec!["x".repeat(MAX_FRAME_BYTES)]
        );

        let mut decoder = FrameDecoder::default();

        let failure = decoder.push(&vec![b'x'; MAX_FRAME_BYTES + 1]).unwrap_err();

        assert!(failure.frames.is_empty());
        assert_eq!(failure.error, FrameError::FrameTooLong);
    }

    #[test]
    fn enforces_limit_across_reads() {
        let mut decoder = FrameDecoder::default();

        decoder.push(&vec![b'x'; MAX_FRAME_BYTES]).unwrap();

        let failure = decoder.push(b"x").unwrap_err();
        assert!(failure.frames.is_empty());
        assert_eq!(failure.error, FrameError::FrameTooLong);
    }

    #[test]
    fn preserves_completed_frames_before_later_error() {
        let cases = [
            (b"\n".to_vec(), FrameError::EmptyFrame),
            (vec![0xff, b'\n'], FrameError::InvalidUtf8),
            (vec![b'x'; MAX_FRAME_BYTES + 1], FrameError::FrameTooLong),
        ];

        for (tail, expected_error) in cases {
            let mut combined_input = b"{}\n".to_vec();
            combined_input.extend_from_slice(&tail);

            let mut combined = FrameDecoder::default();
            let combined_failure = combined.push(&combined_input).unwrap_err();

            let mut split = FrameDecoder::default();
            let split_frames = split.push(b"{}\n").unwrap();
            let split_failure = split.push(&tail).unwrap_err();

            assert_eq!(combined_failure.frames, split_frames);
            assert_eq!(combined_failure.error, expected_error);
            assert!(split_failure.frames.is_empty());
            assert_eq!(split_failure.error, expected_error);
        }
    }

    #[test]
    fn rejects_eof_without_lf() {
        let mut decoder = FrameDecoder::default();

        decoder.push(b"{\"id\":1}").unwrap();

        assert_eq!(decoder.finish(), Err(FrameError::IncompleteFrame));

        let mut decoder = FrameDecoder::default();
        decoder.push(b"{}\n").unwrap();

        assert_eq!(decoder.finish(), Ok(()));
    }

    #[test]
    fn accepts_json_object() {
        let object = parse_json_object(r#"{"id":7,"op":"get","key":"power"}"#).unwrap();

        assert_eq!(
            object.get("id").and_then(serde_json::Value::as_u64),
            Some(7)
        );

        assert_eq!(
            object.get("op").and_then(serde_json::Value::as_str),
            Some("get")
        );

        assert!(parse_json_object("{}").is_ok());
    }

    #[test]
    fn rejects_malformed_json() {
        for frame in ["", "{", r#"{"id":1"#, r#"{"id":1} trailing"#] {
            assert_eq!(parse_json_object(frame), Err(JsonError::InvalidJson));
        }
    }

    #[test]
    fn rejects_non_object_json() {
        for frame in ["null", "false", "42", r#""hello""#, "[]"] {
            assert_eq!(parse_json_object(frame), Err(JsonError::ExpectedObject));
        }
    }

    #[test]
    fn rejects_duplicate_request_fields() {
        for frame in [
            r#"{"id":1,"id":2,"op":"ping"}"#,
            r#"{"id":1,"i\u0064":2,"op":"ping"}"#,
            r#"{"id":1,"op":"get","op":"set","key":"power"}"#,
            r#"{"id":1,"op":"set","key":"power","value":1,"value":2}"#,
            r#"{"id":1,"op":"subscribe","keys":[],"keys":["power"]}"#,
        ] {
            assert_eq!(parse_json_object(frame), Err(JsonError::DuplicateKey));
            assert_eq!(
                parse_tcp_request(frame),
                Err(ParseError::Json(JsonError::DuplicateKey))
            );
        }
    }

    #[test]
    fn accepts_crlf_frame() {
        let mut decoder = FrameDecoder::default();

        let frames = decoder.push(b"{\"id\":1}\r\n").unwrap();

        assert_eq!(frames.len(), 1);

        let object = parse_json_object(&frames[0]).unwrap();

        assert_eq!(
            object.get("id").and_then(serde_json::Value::as_u64),
            Some(1)
        );
    }

    #[test]
    fn parses_all_request_operations() {
        let cases = [
            ("hello", RequestOp::Hello),
            ("describe", RequestOp::Describe),
            ("get", RequestOp::Get),
            ("set", RequestOp::Set),
            ("subscribe", RequestOp::Subscribe),
            ("unsubscribe", RequestOp::Unsubscribe),
            ("ping", RequestOp::Ping),
        ];

        for (op, expected) in cases {
            let frame = format!(r#"{{"id":7,"op":"{op}"}}"#);
            let object = parse_json_object(&frame).unwrap();

            assert_eq!(
                parse_request_header(&object),
                Ok(RequestHeader {
                    id: 7,
                    op: expected,
                })
            );
        }
    }

    #[test]
    fn rejects_invalid_request_ids() {
        let cases = [
            r#"{"op":"hello"}"#,
            r#"{"id":0,"op":"hello"}"#,
            r#"{"id":-1,"op":"hello"}"#,
            r#"{"id":1.5,"op":"hello"}"#,
            r#"{"id":"1","op":"hello"}"#,
            r#"{"id":null,"op":"hello"}"#,
        ];

        for frame in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(parse_request_header(&object), Err(RequestError::InvalidId));
        }
    }

    #[test]
    fn rejects_unknown_request_operations() {
        let cases = [
            r#"{"id":1}"#,
            r#"{"id":1,"op":"unknown"}"#,
            r#"{"id":1,"op":42}"#,
        ];

        for frame in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(
                parse_request_header(&object),
                Err(RequestError::UnsupportedOp)
            );
        }
    }

    #[test]
    fn accepts_opaque_parameter_keys() {
        for key in ["power", "sample_temperature", "device.future.parameter"] {
            let frame = format!(r#"{{"id":4,"op":"get","key":"{key}"}}"#);
            let object = parse_json_object(&frame).unwrap();

            assert_eq!(parse_parameter_key(&object), Ok(key.to_owned()));
        }
    }

    #[test]
    fn rejects_missing_or_invalid_parameter_keys() {
        let cases = [
            r#"{"id":4,"op":"get"}"#,
            r#"{"id":4,"op":"get","key":""}"#,
            r#"{"id":4,"op":"get","key":null}"#,
            r#"{"id":4,"op":"get","key":42}"#,
            r#"{"id":4,"op":"get","key":["power"]}"#,
            r#"{"id":4,"op":"get","keys":["power"]}"#,
        ];

        for frame in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(parse_parameter_key(&object), Err(RequestError::InvalidKey));
        }
    }

    #[test]
    fn parses_integer_and_float_literals() {
        use crate::model::Value;

        let cases = [
            (r#"{"value":20}"#, Value::Integer(20)),
            (r#"{"value":-20}"#, Value::Integer(-20)),
            (r#"{"value":-0}"#, Value::Integer(0)),
            (r#"{"value":9223372036854775807}"#, Value::Integer(i64::MAX)),
            (
                r#"{"value":-9223372036854775808}"#,
                Value::Integer(i64::MIN),
            ),
            (r#"{"value":20.0}"#, Value::Float(20.0)),
            (r#"{"value":2e1}"#, Value::Float(20.0)),
            (r#"{"value":-0.0}"#, Value::Float(-0.0)),
        ];

        for (frame, expected) in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(parse_set_value(&object), Ok(expected));
        }
    }

    #[test]
    fn rejects_integers_outside_i64() {
        let cases = [
            r#"{"value":9223372036854775808}"#,
            r#"{"value":-9223372036854775809}"#,
            r#"{"value":18446744073709551616}"#,
            r#"{"value":-18446744073709551617}"#,
        ];

        for frame in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(parse_set_value(&object), Err(RequestError::InvalidValue));
        }
    }

    #[test]
    fn rejects_missing_or_non_numeric_values() {
        let cases = [
            r#"{"id":1}"#,
            r#"{"value":null}"#,
            r#"{"value":true}"#,
            r#"{"value":"20"}"#,
            r#"{"value":[]}"#,
            r#"{"value":{}}"#,
        ];

        for frame in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(parse_set_value(&object), Err(RequestError::InvalidValue));
        }
    }

    #[test]
    fn rejects_non_finite_float_conversion() {
        for frame in [r#"{"value":1e400}"#, r#"{"value":-1e400}"#] {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(parse_set_value(&object), Err(RequestError::InvalidValue));
        }
    }

    #[test]
    fn canonicalizes_subscription_keys() {
        let object = parse_json_object(
            r#"{"id":1,"op":"subscribe","keys":["z.future","sample_temperature","a","z.future"]}"#,
        )
        .unwrap();

        assert_eq!(
            parse_subscription_keys(&object, &RequestOp::Subscribe),
            Ok(vec![
                "a".to_owned(),
                "sample_temperature".to_owned(),
                "z.future".to_owned(),
            ])
        );
    }

    #[test]
    fn accepts_empty_key_arrays() {
        let object = parse_json_object(r#"{"keys":[]}"#).unwrap();

        for op in [RequestOp::Subscribe, RequestOp::Unsubscribe] {
            assert_eq!(
                parse_subscription_keys(&object, &op),
                Ok(Vec::<String>::new())
            );
        }
    }

    #[test]
    fn supports_legacy_single_key_only_for_subscribe() {
        let object =
            parse_json_object(r#"{"id":2,"op":"subscribe","key":"sample_temperature"}"#).unwrap();

        assert_eq!(
            parse_subscription_keys(&object, &RequestOp::Subscribe),
            Ok(vec!["sample_temperature".to_owned()])
        );

        assert_eq!(
            parse_subscription_keys(&object, &RequestOp::Unsubscribe),
            Err(RequestError::InvalidKeys)
        );
    }

    #[test]
    fn rejects_malformed_key_arrays() {
        let cases = [
            r#"{}"#,
            r#"{"keys":"sample_temperature"}"#,
            r#"{"keys":["sample_temperature",42]}"#,
            r#"{"keys":[""]}"#,
            r#"{"keys":[null]}"#,
            r#"{"key":"sample_temperature","keys":["sample_temperature"]}"#,
        ];

        for frame in cases {
            let object = parse_json_object(frame).unwrap();

            assert_eq!(
                parse_subscription_keys(&object, &RequestOp::Subscribe),
                Err(RequestError::InvalidKeys)
            );
        }
    }

    #[test]
    fn rejects_keys_for_unrelated_operation() {
        let object = parse_json_object(r#"{"keys":["power"]}"#).unwrap();

        assert_eq!(
            parse_subscription_keys(&object, &RequestOp::Get),
            Err(RequestError::InvalidKeys)
        );
    }

    #[test]
    fn parses_header_only_requests() {
        assert_eq!(
            parse_tcp_request(r#"{"id":1,"op":"hello"}"#),
            Ok(TcpRequest::Hello { id: 1 })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":2,"op":"describe"}"#),
            Ok(TcpRequest::Describe { id: 2 })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":3,"op":"ping"}"#),
            Ok(TcpRequest::Ping { id: 3 })
        );
    }

    #[test]
    fn parses_get_and_set_requests() {
        assert_eq!(
            parse_tcp_request(r#"{"id":4,"op":"get","key":"sample_temperature"}"#),
            Ok(TcpRequest::Get {
                id: 4,
                key: "sample_temperature".to_owned(),
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":5,"op":"set","key":"power","value":20}"#),
            Ok(TcpRequest::Set {
                id: 5,
                key: "power".to_owned(),
                value: crate::model::Value::Integer(20),
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":6,"op":"set","key":"power","value":20.0}"#),
            Ok(TcpRequest::Set {
                id: 6,
                key: "power".to_owned(),
                value: crate::model::Value::Float(20.0),
            })
        );
    }

    #[test]
    fn parses_subscription_requests() {
        assert_eq!(
            parse_tcp_request(
                r#"{"id":7,"op":"subscribe","keys":["sample_temperature","heater_temperature","sample_temperature"]}"#
            ),
            Ok(TcpRequest::Subscribe {
                id: 7,
                keys: vec![
                    "heater_temperature".to_owned(),
                    "sample_temperature".to_owned(),
                ],
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":8,"op":"unsubscribe","keys":["heater_temperature"]}"#),
            Ok(TcpRequest::Unsubscribe {
                id: 8,
                keys: vec!["heater_temperature".to_owned()],
            })
        );

        assert_eq!(
            parse_tcp_request(r#"{"id":9,"op":"subscribe","key":"power"}"#),
            Ok(TcpRequest::Subscribe {
                id: 9,
                keys: vec!["power".to_owned()],
            })
        );
    }

    #[test]
    fn propagates_request_parse_errors() {
        let cases = [
            ("not-json", ParseError::Json(JsonError::InvalidJson)),
            ("[]", ParseError::Json(JsonError::ExpectedObject)),
            (
                r#"{"op":"hello"}"#,
                ParseError::Request(RequestError::InvalidId),
            ),
            (
                r#"{"id":1,"op":"unknown"}"#,
                ParseError::Request(RequestError::UnsupportedOp),
            ),
            (
                r#"{"id":1,"op":"get"}"#,
                ParseError::Request(RequestError::InvalidKey),
            ),
            (
                r#"{"id":1,"op":"set","key":"power"}"#,
                ParseError::Request(RequestError::InvalidValue),
            ),
            (
                r#"{"id":1,"op":"unsubscribe","key":"power"}"#,
                ParseError::Request(RequestError::InvalidKeys),
            ),
        ];

        for (frame, expected) in cases {
            assert_eq!(parse_tcp_request(frame), Err(expected));
        }
    }
}
