use bytes::{Buf, BytesMut};

use super::{DtcMessageType, RawDtcMessage};

/// Streaming parser that accumulates TCP bytes and extracts complete DTC frames.
#[derive(Default)]
pub struct DtcFrameParser {
    buffer: BytesMut,
}

impl DtcFrameParser {
    /// Push bytes from TCP reads into parser buffer.
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Read the next complete frame if available.
    pub fn next_message(&mut self) -> Option<RawDtcMessage> {
        if self.buffer.len() < 4 {
            return None;
        }
        let size = u16::from_le_bytes([self.buffer[0], self.buffer[1]]) as usize;
        if size < 4 || self.buffer.len() < size {
            return None;
        }
        let msg_type_raw = u16::from_le_bytes([self.buffer[2], self.buffer[3]]);
        let payload = self.buffer[4..size].to_vec();
        self.buffer.advance(size);
        Some(RawDtcMessage {
            message_type: DtcMessageType::from(msg_type_raw),
            payload,
        })
    }
}
