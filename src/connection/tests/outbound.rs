use super::*;

enum WriteAction {
    Take(usize),
    WouldBlock,
    Interrupted,
}

struct ScriptedWriter {
    actions: VecDeque<WriteAction>,
    written: Vec<u8>,
}

impl Write for ScriptedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.actions.pop_front() {
            Some(WriteAction::Take(limit)) => {
                let count = limit.min(bytes.len());
                self.written.extend_from_slice(&bytes[..count]);
                Ok(count)
            }

            Some(WriteAction::WouldBlock) => Err(io::ErrorKind::WouldBlock.into()),

            Some(WriteAction::Interrupted) => Err(io::ErrorKind::Interrupted.into()),

            None => {
                self.written.extend_from_slice(bytes);
                Ok(bytes.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn preserves_partial_writes_and_message_order() {
    let mut buffer = OutboundBuffer::new(128);

    buffer.enqueue_line(r#"{"id":1,"status":"ok"}"#).unwrap();
    buffer.enqueue_line(r#"{"event":"measurement"}"#).unwrap();

    let mut writer = ScriptedWriter {
        actions: VecDeque::from(vec![
            WriteAction::Take(5),
            WriteAction::WouldBlock,
            WriteAction::Interrupted,
        ]),
        written: Vec::new(),
    };

    buffer.flush_to(&mut writer).unwrap();

    assert!(!buffer.is_empty());
    assert_eq!(writer.written, b"{\"id\"");

    buffer.flush_to(&mut writer).unwrap();

    assert!(buffer.is_empty());

    assert_eq!(
        String::from_utf8(writer.written).unwrap(),
        "{\"id\":1,\"status\":\"ok\"}\n{\"event\":\"measurement\"}\n"
    );
}

#[test]
fn enforces_output_capacity_and_line_framing() {
    let mut buffer = OutboundBuffer::new(6);

    assert_eq!(buffer.enqueue_line(""), Err(OutputError::InvalidLine));
    assert_eq!(buffer.enqueue_line("a\nb"), Err(OutputError::InvalidLine));

    assert_eq!(buffer.enqueue_line("abcde"), Ok(()));
    assert_eq!(buffer.len(), 6);

    assert_eq!(buffer.enqueue_line("x"), Err(OutputError::Full));
    assert_eq!(buffer.len(), 6);

    let mut writer = Vec::new();
    buffer.flush_to(&mut writer).unwrap();

    assert_eq!(writer, b"abcde\n");
    assert!(buffer.is_empty());

    assert_eq!(buffer.enqueue_line("x"), Ok(()));
}

#[test]
fn reports_zero_byte_write_without_losing_buffer() {
    let mut buffer = OutboundBuffer::new(16);
    buffer.enqueue_line("{}").unwrap();

    let mut writer = ScriptedWriter {
        actions: VecDeque::from(vec![WriteAction::Take(0)]),
        written: Vec::new(),
    };

    let error = buffer.flush_to(&mut writer).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::WriteZero);
    assert_eq!(buffer.len(), 3);
}
