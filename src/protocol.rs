pub const MAX_FRAME_BYTES: usize = 16 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum FrameError {
    EmptyFrame,
    FrameTooLong,
    InvalidUtf8,
    IncompleteFrame,
}

#[derive(Default)]
pub struct FrameDecoder {
    pending: Vec<u8>,
}

impl FrameDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, FrameError> {
        let mut frames = Vec::new();

        for chunk in bytes.split_inclusive(|&byte| byte == b'\n') {
            let has_lf = chunk.last() == Some(&b'\n');

            let data = if has_lf {
                &chunk[..chunk.len() - 1]
            } else {
                chunk
            };

            if data.len() > MAX_FRAME_BYTES - self.pending.len() {
                return Err(FrameError::FrameTooLong);
            }

            self.pending.extend_from_slice(data);

            if has_lf {
                if self.pending.is_empty() {
                    return Err(FrameError::EmptyFrame);
                }

                let line = std::str::from_utf8(&self.pending)
                    .map_err(|_| FrameError::InvalidUtf8)?
                    .to_owned();

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
}

pub fn parse_json_object(
    frame: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, JsonError> {
    let value: serde_json::Value =
        serde_json::from_str(frame).map_err(|_| JsonError::InvalidJson)?;

    match value {
        serde_json::Value::Object(fields) => Ok(fields),
        _ => Err(JsonError::ExpectedObject),
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

        assert_eq!(decoder.push(b"\n"), Err(FrameError::EmptyFrame));
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut decoder = FrameDecoder::default();

        assert!(decoder.push(&[0xff]).unwrap().is_empty());

        assert_eq!(decoder.push(b"\n"), Err(FrameError::InvalidUtf8));
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

        assert_eq!(
            decoder.push(&vec![b'x'; MAX_FRAME_BYTES + 1]),
            Err(FrameError::FrameTooLong)
        );
    }

    #[test]
    fn enforces_limit_across_reads() {
        let mut decoder = FrameDecoder::default();

        decoder.push(&vec![b'x'; MAX_FRAME_BYTES]).unwrap();

        assert_eq!(decoder.push(b"x"), Err(FrameError::FrameTooLong));
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
}
