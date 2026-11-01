//! SBE (Simple Binary Encoding): what generated codecs share, [`codegen`], which generates them,
//! and [`raw`], for reading and writing fields one at a time without a schema.
//!
//! [`codegen::SbeGenerator`], from a `build.rs`, generates, for each message in the schema, a borrowed decoder (`NewOrderSingleRef`)
//! and a struct to encode (`NewOrderSingle`), plus a `decode` function that reads the message
//! header and picks the decoder by template ID. The generated code calls the functions here.
//!
//! A decoder checks the whole message when it's wrapped: the root block, every group entry and
//! every variable-length data field must lie within the buffer, and each block must be long enough
//! for the fields of the version it says it is. After that, reading a field can't fail and copies
//! nothing. Fields newer than a message's version read as absent; a block longer than the schema's
//! (from a newer version) is read as far as the schema knows it.

pub mod codegen;
pub mod raw;

use std::fmt;
use std::marker::PhantomData;

/// Why a message couldn't be decoded or encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SbeError {
    /// The buffer ends before the message does.
    Truncated,
    /// The message header names another schema.
    SchemaId(u16),
    /// The message header names a template the schema doesn't have.
    UnknownTemplate(u16),
    /// A block is shorter than the fields of its version need.
    BlockLength {
        /// The block length the message gives.
        actual: usize,
        /// The length its version's fields need.
        needed: usize,
    },
    /// A group has more entries, or a data field more bytes, than its encoding can say: or, read,
    /// a data field has more than its schema's `maxValue`.
    TooLong {
        /// The group or data field, as the schema names it.
        name: &'static str,
        /// How many entries or bytes it has.
        len: usize,
        /// The most its encoding allows.
        max: usize,
    },
}

impl fmt::Display for SbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => f.write_str("the buffer ends before the SBE message does"),
            Self::SchemaId(id) => write!(f, "the message is from schema {id}"),
            Self::UnknownTemplate(id) => write!(f, "unknown template ID {id}"),
            Self::BlockLength { actual, needed } => {
                write!(f, "block length {actual} is shorter than its version's fields ({needed})")
            }
            Self::TooLong { name, len, max } => write!(f, "{name} has {len}, more than its encoding allows ({max})"),
        }
    }
}

impl std::error::Error for SbeError {}

/// A message that encodes itself, header first: what generated codecs' message structs implement,
/// so code can send any of them.
pub trait Encode {
    /// The template ID in its header.
    const TEMPLATE_ID: u16;

    /// Appends the message to `out`, after a message header.
    ///
    /// # Errors
    ///
    /// If a group has more entries, or a data field more bytes, than its encoding allows. `out` is
    /// then as it was.
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), SbeError>;
}

/// A block that may have groups and data after it: a message's root, or a group entry. Generated
/// decoders implement it.
pub trait Block<'a>: Sized {
    /// Wraps the block at the start of `bytes`, which is `block_length` long and of schema version
    /// `version`, with the groups and data that follow it. Returns the decoder and how many bytes
    /// the block, groups and data take.
    ///
    /// # Errors
    ///
    /// If `bytes` ends before they do, or the block is too short for its version.
    fn wrap(bytes: &'a [u8], block_length: usize, version: u16) -> Result<(Self, usize), SbeError>;
}

/// A repeating group's entries, already checked, read in order.
pub struct Group<'a, E> {
    bytes: &'a [u8],
    block_length: usize,
    count: usize,
    version: u16,
    entry: PhantomData<E>,
}

impl<'a, E: Block<'a>> Group<'a, E> {
    /// Checks `count` entries of `block_length` at the start of `bytes`, and returns them and how
    /// many bytes they take.
    ///
    /// # Errors
    ///
    /// If an entry doesn't fit in `bytes`, or one of its blocks is too short for `version`.
    #[doc(hidden)]
    pub fn scan(bytes: &'a [u8], block_length: usize, count: usize, version: u16) -> Result<(Self, usize), SbeError> {
        let mut used = 0;
        for _ in 0..count {
            let rest = bytes.get(used..).ok_or(SbeError::Truncated)?;
            let (_, len) = E::wrap(rest, block_length, version)?;
            debug_assert!(len >= block_length, "an entry takes at least its block");
            used += len;
        }
        Ok((Self { bytes: &bytes[..used], block_length, count, version, entry: PhantomData }, used))
    }

    /// No entries: a group newer than the message's version.
    #[must_use]
    pub fn empty() -> Self {
        Self { bytes: &[], block_length: 0, count: 0, version: 0, entry: PhantomData }
    }

    /// How many entries are left.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether no entries are left.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl<'a, E: Block<'a>> Iterator for Group<'a, E> {
    type Item = E;

    fn next(&mut self) -> Option<E> {
        if self.count == 0 {
            debug_assert!(self.bytes.is_empty(), "the last entry ends the group");
            return None;
        }
        let (entry, len) =
            E::wrap(self.bytes, self.block_length, self.version).expect("entries are checked by Group::scan");
        self.bytes = &self.bytes[len..];
        self.count -= 1;
        Some(entry)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count, Some(self.count))
    }
}

impl<'a, E: Block<'a>> ExactSizeIterator for Group<'a, E> {}

impl<'a, E: Block<'a> + fmt::Debug> fmt::Debug for Group<'a, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(*self).finish()
    }
}

impl<E> Clone for Group<'_, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Group<'_, E> {}

// What generated codecs call: public so they can, but hidden from the docs, since it isn't an
// application's API and may change with the generator. `pad` and `Primitive` are the exceptions:
// applications fill fixed-length character fields with `pad`, and `raw` reads and writes
// `Primitive`s.

/// `len` bytes of `bytes` from `at`.
///
/// # Errors
///
/// [`SbeError::Truncated`] if `bytes` ends first.
#[doc(hidden)]
pub fn take(bytes: &[u8], at: usize, len: usize) -> Result<&[u8], SbeError> {
    at.checked_add(len).and_then(|end| bytes.get(at..end)).ok_or(SbeError::Truncated)
}

/// A length or count read from the wire, as a `usize`.
///
/// # Errors
///
/// [`SbeError::Truncated`] if it's more than the platform can address, so no buffer is that long.
#[doc(hidden)]
pub fn length(n: impl TryInto<usize>) -> Result<usize, SbeError> {
    n.try_into().map_err(|_| SbeError::Truncated)
}

/// A count or length to write in an encoding of type `T`, or one read, checked against the
/// schema's `max` as a write is.
///
/// # Errors
///
/// [`SbeError::TooLong`] if `len` is more than `T`, or `max` when the schema sets one, allows.
#[doc(hidden)]
pub fn count<T: TryFrom<usize> + Into<u64> + Copy>(name: &'static str, len: usize, max: T) -> Result<T, SbeError> {
    let too_long = || SbeError::TooLong { name, len, max: usize::try_from(max.into()).unwrap_or(usize::MAX) };
    let n = T::try_from(len).map_err(|_| too_long())?;
    if n.into() > max.into() {
        return Err(too_long());
    }
    Ok(n)
}

/// A char array's text: up to the first NUL, which pads it.
#[must_use]
#[doc(hidden)]
pub fn chars(bytes: &[u8]) -> &[u8] {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    &bytes[..end]
}

/// `text` padded with NULs to a char array of `N`.
///
/// # Panics
///
/// If `text` is longer than `N`.
#[must_use]
pub const fn pad<const N: usize>(text: &[u8]) -> [u8; N] {
    assert!(text.len() <= N, "text longer than the char array");
    let mut out = [0; N];
    let mut i = 0;
    while i < text.len() {
        out[i] = text[i];
        i += 1;
    }
    out
}

/// Appends `len` zero bytes to `out` and returns them, for a block or composite to be written in.
#[doc(hidden)]
pub fn reserve(out: &mut Vec<u8>, len: usize) -> &mut [u8] {
    let at = out.len();
    out.resize(at + len, 0);
    &mut out[at..]
}

/// A primitive type SBE encodes, read and written at an offset in either byte order: `u8` to
/// `u64`, `i8` to `i64`, `f32` and `f64`. A `char` is a `u8`.
pub trait Primitive: Copy {
    /// Its size in bytes.
    const SIZE: usize;
    /// Reads it, little-endian, from `bytes` at `at`.
    fn get_le(bytes: &[u8], at: usize) -> Self;
    /// Reads it, big-endian, from `bytes` at `at`.
    fn get_be(bytes: &[u8], at: usize) -> Self;
    /// Writes it, little-endian, into `bytes` at `at`.
    fn put_le(self, bytes: &mut [u8], at: usize);
    /// Writes it, big-endian, into `bytes` at `at`.
    fn put_be(self, bytes: &mut [u8], at: usize);
}

macro_rules! primitive {
    ($($t:ty),*) => {$(
        impl Primitive for $t {
            const SIZE: usize = size_of::<$t>();
            fn get_le(bytes: &[u8], at: usize) -> Self {
                <$t>::from_le_bytes(bytes[at..at + Self::SIZE].try_into().expect("a slice of SIZE"))
            }
            fn get_be(bytes: &[u8], at: usize) -> Self {
                <$t>::from_be_bytes(bytes[at..at + Self::SIZE].try_into().expect("a slice of SIZE"))
            }
            fn put_le(self, bytes: &mut [u8], at: usize) {
                bytes[at..at + Self::SIZE].copy_from_slice(&self.to_le_bytes());
            }
            fn put_be(self, bytes: &mut [u8], at: usize) {
                bytes[at..at + Self::SIZE].copy_from_slice(&self.to_be_bytes());
            }
        }
    )*};
}

primitive!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

/// Reads a little-endian `T` from `bytes` at `at`. Panics if it doesn't fit: decoders check
/// lengths when they're wrapped.
#[must_use]
#[doc(hidden)]
pub fn get_le<T: Primitive>(bytes: &[u8], at: usize) -> T {
    T::get_le(bytes, at)
}

/// Reads a big-endian `T` from `bytes` at `at`, as [`get_le`].
#[must_use]
#[doc(hidden)]
pub fn get_be<T: Primitive>(bytes: &[u8], at: usize) -> T {
    T::get_be(bytes, at)
}

/// Writes `value` little-endian into `bytes` at `at`.
#[doc(hidden)]
pub fn put_le<T: Primitive>(bytes: &mut [u8], at: usize, value: T) {
    value.put_le(bytes, at);
}

/// Writes `value` big-endian into `bytes` at `at`.
#[doc(hidden)]
pub fn put_be<T: Primitive>(bytes: &mut [u8], at: usize, value: T) {
    value.put_be(bytes, at);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_checks_the_end() {
        assert_eq!(take(b"abcd", 1, 2), Ok(&b"bc"[..]));
        assert_eq!(take(b"abcd", 3, 2), Err(SbeError::Truncated));
        assert_eq!(take(b"abcd", usize::MAX, 2), Err(SbeError::Truncated));
    }

    #[test]
    fn count_checks_the_type_and_the_schema_maximum() {
        assert_eq!(count::<u8>("g", 255, u8::MAX), Ok(255));
        assert_eq!(count::<u8>("g", 256, u8::MAX), Err(SbeError::TooLong { name: "g", len: 256, max: 255 }));
        assert_eq!(count::<u32>("d", 129, 128), Err(SbeError::TooLong { name: "d", len: 129, max: 128 }));
    }

    #[test]
    fn chars_stop_at_the_padding() {
        assert_eq!(chars(b"AB\0\0"), b"AB");
        assert_eq!(chars(b"ABCD"), b"ABCD");
        assert_eq!(pad::<4>(b"AB"), *b"AB\0\0");
    }

    #[test]
    fn primitives_round_trip_in_both_byte_orders() {
        let mut bytes = [0; 9];
        put_le(&mut bytes, 1, 0x0102_0304_u32);
        assert_eq!(bytes[1..5], [4, 3, 2, 1]);
        assert_eq!(get_le::<u32>(&bytes, 1), 0x0102_0304);
        put_be(&mut bytes, 1, -2_i64);
        assert_eq!(get_be::<i64>(&bytes, 1), -2);
    }
}
