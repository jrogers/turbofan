//! Framing and log records: the bytes on the wire and on disk.
//!
//! A frame is a little-endian `u32` length and one SBE message from [`crate::wire`]. A log
//! record is `len: u32 · checksum: u64 · term: u64 · index: u64 · payload`, the same bytes in
//! the log file and in `AppendEntries`, so a leader ships what it stored without re-encoding it
//! and a follower checks the checksum before it appends. The payload is a log-entry message
//! (`SessionOpen`, `SessionMessage`, ...); Raft reads only the term and index.

use std::fmt;

use turbofan_sbe::{Encode, SbeError};

use crate::wire;

/// The most a frame can hold after its length: an `AppendEntries` of records, or a snapshot chunk.
pub const MAX_FRAME_BYTES: usize = 1 << 20;
/// The most a service payload can be, in a client message or a log entry (`payloadData`).
pub const MAX_PAYLOAD_BYTES: usize = 65536;
/// The most records one `AppendEntries` carries, which bounds a follower's work per message.
pub const MAX_ENTRIES_PER_APPEND: usize = 1024;
/// `Hello.magic`.
pub const MAGIC: u32 = u32::from_le_bytes(*b"CLST");

/// A record's length field, checksum, term and index.
pub const RECORD_HEADER: usize = 4 + 8 + 8 + 8;
/// The most a record can be: a log-entry message carrying the largest payload.
pub const MAX_RECORD_BYTES: usize = RECORD_HEADER + MAX_PAYLOAD_BYTES + 64;

const _: () = assert!(MAX_RECORD_BYTES * 8 < MAX_FRAME_BYTES, "an AppendEntries must fit several whole records");
const _: () = assert!(MAX_PAYLOAD_BYTES <= MAX_FRAME_BYTES / 2);

/// Why bytes off the wire or out of the log were refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// A frame or record says it's longer than its limit.
    TooLong {
        /// The length it gave.
        len: usize,
        /// The limit.
        max: usize,
    },
    /// The message in a frame didn't decode.
    Sbe(SbeError),
    /// A frame's message ends before the frame does.
    Trailing,
    /// A record's checksum doesn't match its bytes.
    Checksum,
    /// A record is shorter than its header.
    Short,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { len, max } => write!(f, "length {len} is over the limit of {max}"),
            Self::Sbe(error) => write!(f, "{error}"),
            Self::Trailing => f.write_str("bytes left over after the frame's message"),
            Self::Checksum => f.write_str("record checksum mismatch"),
            Self::Short => f.write_str("record shorter than its header"),
        }
    }
}

impl std::error::Error for CodecError {}

impl CodecError {
    /// The frame holds a message of a type this node doesn't know, from a newer version: it
    /// can be skipped, by its length, and the next one read. See `WIRE.md`.
    #[must_use]
    pub fn is_unknown_message(&self) -> bool {
        matches!(self, Self::Sbe(SbeError::UnknownTemplate(_)))
    }
}

impl From<SbeError> for CodecError {
    fn from(error: SbeError) -> Self {
        Self::Sbe(error)
    }
}

/// Appends `message` to `out` as one frame.
///
/// # Errors
///
/// If a data field is longer than its encoding allows, or the frame would be over
/// [`MAX_FRAME_BYTES`]. `out` is then as it was.
pub fn encode_frame(message: &impl Encode, out: &mut Vec<u8>) -> Result<(), CodecError> {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    if let Err(error) = message.encode_into(out) {
        out.truncate(start);
        return Err(error.into());
    }
    let len = out.len() - start - 4;
    if len > MAX_FRAME_BYTES {
        out.truncate(start);
        return Err(CodecError::TooLong { len, max: MAX_FRAME_BYTES });
    }
    let len = u32::try_from(len).expect("under MAX_FRAME_BYTES");
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// The first frame in `bytes`, and how many bytes it took, or `None` if it hasn't all arrived.
///
/// # Errors
///
/// If the frame is over [`MAX_FRAME_BYTES`] or its message is malformed: the connection is then
/// unusable, as there's no way to find the next frame. Or if its message is of a type this node
/// doesn't know ([`CodecError::is_unknown_message`]): skip it.
///
/// A message from a newer version may have more after what this version knows of it (groups
/// or data added at the end): that's read past, not refused.
pub fn decode_frame(bytes: &[u8]) -> Result<Option<(wire::Decoded<'_>, usize)>, CodecError> {
    let Some(len) = bytes.get(..4) else { return Ok(None) };
    let len = usize::try_from(u32::from_le_bytes(len.try_into().expect("four bytes"))).expect("u32 fits usize");
    if len > MAX_FRAME_BYTES {
        return Err(CodecError::TooLong { len, max: MAX_FRAME_BYTES });
    }
    let Some(body) = bytes.get(4..4 + len) else { return Ok(None) };
    let (message, used) = wire::decode(body)?;
    // The header's version: blockLength, templateId, schemaId, then version.
    let version = body.get(6..8).map(|v| u16::from_le_bytes([v[0], v[1]]));
    if used != len && version <= Some(wire::SCHEMA_VERSION) {
        return Err(CodecError::Trailing);
    }
    Ok(Some((message, 4 + len)))
}

/// One log record, borrowed from where it lies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record<'a> {
    /// The term of the leader that appended it.
    pub term: u64,
    /// Its position in the log, from 1.
    pub index: u64,
    /// A log-entry message.
    pub payload: &'a [u8],
}

impl<'a> Record<'a> {
    /// Appends the record to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        debug_assert!(self.index > 0 && self.term > 0);
        let len = RECORD_HEADER - 4 + self.payload.len();
        assert!(len + 4 <= MAX_RECORD_BYTES, "record of {} bytes is over MAX_RECORD_BYTES", len + 4);
        out.extend_from_slice(&u32::try_from(len).expect("under MAX_RECORD_BYTES").to_le_bytes());
        let summed = out.len() + 8;
        out.extend_from_slice(&[0; 8]);
        out.extend_from_slice(&self.term.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(self.payload);
        let sum = checksum(&out[summed..]);
        out[summed - 8..summed].copy_from_slice(&sum.to_le_bytes());
    }

    /// The record at the start of `bytes`, and how many bytes it took, or `None` if `bytes` ends
    /// before it does (a torn write, at the end of a log).
    ///
    /// # Errors
    ///
    /// If its length is impossible or its checksum doesn't match.
    pub fn decode(bytes: &'a [u8]) -> Result<Option<(Self, usize)>, CodecError> {
        let Some(len) = bytes.get(..4) else { return Ok(None) };
        let len = usize::try_from(u32::from_le_bytes(len.try_into().expect("four bytes"))).expect("u32 fits usize");
        if len + 4 > MAX_RECORD_BYTES {
            return Err(CodecError::TooLong { len: len + 4, max: MAX_RECORD_BYTES });
        }
        if len + 4 < RECORD_HEADER {
            return Err(CodecError::Short);
        }
        let Some(record) = bytes.get(4..4 + len) else { return Ok(None) };
        let sum = u64::from_le_bytes(record[..8].try_into().expect("eight bytes"));
        if checksum(&record[8..]) != sum {
            return Err(CodecError::Checksum);
        }
        let term = u64::from_le_bytes(record[8..16].try_into().expect("eight bytes"));
        let index = u64::from_le_bytes(record[16..24].try_into().expect("eight bytes"));
        Ok(Some((Self { term, index, payload: &record[24..] }, 4 + len)))
    }
}

/// Every record in `bytes`, which must hold whole records only.
///
/// # Errors
///
/// If one is malformed or the last is cut short.
pub fn records(bytes: &[u8]) -> impl Iterator<Item = Result<Record<'_>, CodecError>> {
    let mut at = 0;
    std::iter::from_fn(move || {
        if at == bytes.len() {
            return None;
        }
        match Record::decode(&bytes[at..]) {
            Ok(Some((record, len))) => {
                at += len;
                Some(Ok(record))
            }
            Ok(None) => {
                at = bytes.len();
                Some(Err(CodecError::Short))
            }
            Err(error) => {
                at = bytes.len();
                Some(Err(error))
            }
        }
    })
}

/// CRC32C, in hardware where the CPU has it: catches a torn or garbled record.
pub(crate) fn checksum(bytes: &[u8]) -> u64 {
    checksum_on(CHECKSUM_START, bytes)
}

/// What [`checksum_on`] starts from.
pub(crate) const CHECKSUM_START: u64 = 0;

/// The checksum of what made `hash` and then `bytes`, for summing a stream.
pub(crate) fn checksum_on(hash: u64, bytes: &[u8]) -> u64 {
    let crc = u32::try_from(hash).expect("a checksum from checksum_on");
    u64::from(crc32c::crc32c_append(crc, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CRC32C's check value, and the same summed in pieces.
    #[test]
    fn the_checksum_is_crc32c_and_streams() {
        assert_eq!(checksum(b"123456789"), 0xe306_9283);
        assert_eq!(checksum_on(checksum(b"1234"), b"56789"), checksum(b"123456789"));
    }

    /// From a newer version: more after the message than this version knows is read past, and
    /// a message of a type it doesn't know is an error it can skip, not a broken connection.
    #[test]
    fn a_newer_versions_frames_are_tolerated() {
        let mut frame = Vec::new();
        encode_frame(&wire::KeepAlive { session: 7 }, &mut frame).unwrap();
        let newer = |frame: &[u8], version: u16, extra: &[u8]| {
            let mut out = frame.to_vec();
            out.extend_from_slice(extra);
            let len = u32::try_from(out.len() - 4).unwrap();
            out[..4].copy_from_slice(&len.to_le_bytes());
            out[10..12].copy_from_slice(&version.to_le_bytes());
            out
        };
        let longer = newer(&frame, wire::SCHEMA_VERSION + 1, b"new data");
        let (wire::Decoded::KeepAlive(read), used) = decode_frame(&longer).unwrap().unwrap() else { panic!() };
        assert_eq!((read.session(), used), (7, longer.len()));
        let same = newer(&frame, wire::SCHEMA_VERSION, b"new data");
        assert_eq!(decode_frame(&same).unwrap_err(), CodecError::Trailing, "the same version, malformed");

        let mut unknown = frame.clone();
        unknown[6..8].copy_from_slice(&999_u16.to_le_bytes());
        assert!(decode_frame(&unknown).unwrap_err().is_unknown_message());
        assert!(!decode_frame(&same).unwrap_err().is_unknown_message());
    }

    /// A hello from before the admin token (version 1) decodes, with none.
    #[test]
    fn an_older_hello_has_no_token() {
        let hello =
            wire::Hello { magic: MAGIC, service_id: 7, role: wire::Role::Admin, node_id: 0, epoch: 0, token: b"" };
        let mut frame = Vec::new();
        encode_frame(&hello, &mut frame).unwrap();
        // Without the token's length byte, and saying version 1.
        frame.pop();
        let len = u32::try_from(frame.len() - 4).unwrap();
        frame[..4].copy_from_slice(&len.to_le_bytes());
        frame[10..12].copy_from_slice(&1_u16.to_le_bytes());
        let (wire::Decoded::Hello(read), _) = decode_frame(&frame).unwrap().unwrap() else { panic!() };
        assert_eq!((read.service_id(), read.token()), (7, &b""[..]));
    }

    #[test]
    fn a_frame_round_trips_and_waits_for_all_its_bytes() {
        let mut out = Vec::new();
        let message = wire::Message { session: 7, client_seq: 3, payload: b"bytes" };
        encode_frame(&message, &mut out).unwrap();
        for cut in 0..out.len() {
            assert!(decode_frame(&out[..cut]).unwrap().is_none(), "cut at {cut}");
        }
        let (wire::Decoded::Message(read), used) = decode_frame(&out).unwrap().unwrap() else {
            panic!("not a Message")
        };
        assert_eq!((used, read.session(), read.client_seq(), read.payload()), (out.len(), 7, 3, &b"bytes"[..]));
    }

    #[test]
    fn an_oversized_frame_is_refused_before_it_arrives() {
        let len = u32::try_from(MAX_FRAME_BYTES + 1).unwrap();
        let error = decode_frame(&len.to_le_bytes()).unwrap_err();
        assert_eq!(error, CodecError::TooLong { len: MAX_FRAME_BYTES + 1, max: MAX_FRAME_BYTES });
    }

    #[test]
    fn a_frame_with_bytes_after_its_message_is_refused() {
        let mut out = Vec::new();
        encode_frame(&wire::KeepAlive { session: 1 }, &mut out).unwrap();
        out.push(0);
        out[0] += 1;
        assert_eq!(decode_frame(&out).unwrap_err(), CodecError::Trailing);
    }

    #[test]
    fn a_record_round_trips_and_a_flipped_bit_is_caught() {
        let mut out = Vec::new();
        Record { term: 2, index: 9, payload: b"entry" }.encode_into(&mut out);
        Record { term: 2, index: 10, payload: b"" }.encode_into(&mut out);
        let read: Vec<_> = records(&out).collect::<Result<_, _>>().unwrap();
        assert_eq!(
            read,
            [Record { term: 2, index: 9, payload: b"entry" }, Record { term: 2, index: 10, payload: b"" }]
        );
        for cut in 1..out.len() {
            if cut != RECORD_HEADER + 5 {
                assert!(records(&out[..cut]).any(|r| r.is_err()), "cut at {cut}");
            }
        }
        for byte in 4..RECORD_HEADER + 5 {
            let mut garbled = out.clone();
            garbled[byte] ^= 1;
            assert_eq!(Record::decode(&garbled).unwrap_err(), CodecError::Checksum, "byte {byte}");
        }
    }
}
