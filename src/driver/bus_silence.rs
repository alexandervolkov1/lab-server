//! Host-side pacing for every exchange on one physical port.

use std::io::{self, BufRead, Read};
use std::thread;
use std::time::{Duration, Instant};

use super::DeviceIo;

pub(crate) struct BusSilence<I> {
    io: I,
    minimum: Duration,
    last_activity: Option<Instant>,
}

impl<I: DeviceIo> BusSilence<I> {
    pub(crate) fn new(io: I, minimum: Duration) -> Self {
        Self {
            io,
            minimum,
            last_activity: None,
        }
    }
}

impl<I: DeviceIo> Read for BusSilence<I> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.io.read(buffer)?;
        if count != 0 {
            self.last_activity = Some(Instant::now());
        }
        Ok(count)
    }
}

impl<I: DeviceIo> BufRead for BusSilence<I> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.io.fill_buf()
    }

    fn consume(&mut self, count: usize) {
        self.io.consume(count);
        if count != 0 {
            self.last_activity = Some(Instant::now());
        }
    }
}

impl<I: DeviceIo> DeviceIo for BusSilence<I> {
    fn send(&mut self, request: &[u8]) -> io::Result<()> {
        if let Some(last) = self.last_activity {
            let remaining = self.minimum.saturating_sub(last.elapsed());
            if !remaining.is_zero() {
                thread::sleep(remaining);
            }
        }
        self.io.send(request)?;
        // Also pace exchanges without a received frame. The timestamp is the
        // return of send on the host, whose time can differ from wire TX end.
        self.last_activity = Some(Instant::now());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::*;

    #[test]
    fn only_delivered_bytes_move_the_receive_timestamp() {
        let bytes = BufReader::new(Cursor::new(b"abc".to_vec()));
        let mut port = BusSilence::new(bytes, Duration::from_millis(20));
        assert_eq!(port.last_activity, None);
        assert_eq!(port.fill_buf().unwrap(), b"abc");
        port.consume(0);
        assert_eq!(port.last_activity, None);

        port.consume(1);
        let first = port.last_activity.unwrap();
        port.last_activity = None;
        let mut rest = [0; 2];
        port.read_exact(&mut rest).unwrap();
        assert_eq!(&rest, b"bc");
        let last = port.last_activity.unwrap();
        assert!(last >= first);
        assert_eq!(port.read(&mut rest).unwrap(), 0);
        assert_eq!(port.last_activity, Some(last));
    }

    #[test]
    fn second_send_without_a_reply_is_paced_from_the_first_send() {
        let minimum = Duration::from_millis(20);
        let mut port = BusSilence::new(BufReader::new(Cursor::new(Vec::new())), minimum);
        port.send(b"a").unwrap();
        let first_send = port.last_activity.unwrap();
        port.send(b"b").unwrap();
        assert!(port.last_activity.unwrap().duration_since(first_send) >= minimum);
        assert_eq!(port.io.into_inner().into_inner(), b"ab");
    }
}
