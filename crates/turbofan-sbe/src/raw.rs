//! Schemaless SBE: fields written and read one at a time, in wire order, by code that knows the
//! layout itself rather than from a generated codec.
//!
//! A message is a header, its root block of fixed-size fields, then its groups (a group header,
//! then each entry's block and its own groups and data), then its variable-length data. A
//! [`Writer`] appends them in that order. A [`Reader`] reads them in the same order and checks
//! every read against the end of the buffer.
//!
//! ```
//! use turbofan_sbe::raw::{Header, Reader, Writer};
//!
//! let mut out = Vec::new();
//! let mut w = Writer::new(&mut out);
//! w.header(Header { block_length: 12, template_id: 1, schema_id: 7, version: 0 });
//! w.put(42_u64);
//! w.put(100_u32);
//! w.group_header(2, 1)?;
//! w.put(-3_i16);
//! w.data::<u16>(b"note")?;
//!
//! let mut r = Reader::new(&out);
//! let header = r.header()?;
//! // A block from a newer version may be longer: read the fields you know, and `block` skips
//! // the rest.
//! let mut block = r.block(header.block_length.into())?;
//! assert_eq!((block.get::<u64>()?, block.get::<u32>()?), (42, 100));
//! let (block_length, count) = r.group_header()?;
//! for _ in 0..count {
//!     assert_eq!(r.block(block_length.into())?.get::<i16>()?, -3);
//! }
//! assert_eq!(r.data::<u16>()?, b"note");
//! assert!(r.is_empty());
//! # Ok::<(), turbofan_sbe::SbeError>(())
//! ```

use crate::{Primitive, SbeError, count, length, take};

/// The standard message header: `blockLength`, `templateId`, `schemaId` and `version`, each a
/// `uint16`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The length of the root block.
    pub block_length: u16,
    /// Which message this is.
    pub template_id: u16,
    /// The schema it's from.
    pub schema_id: u16,
    /// The schema version it was encoded with.
    pub version: u16,
}

/// A length prefix for variable-length data: `u8`, `u16` or `u32`.
pub trait Length: Primitive + TryFrom<usize> + Into<u64> {
    /// The most it can say.
    const MAX: Self;
}

impl Length for u8 {
    const MAX: Self = u8::MAX;
}

impl Length for u16 {
    const MAX: Self = u16::MAX;
}

impl Length for u32 {
    const MAX: Self = u32::MAX;
}

/// Appends fields to a buffer, in wire order.
#[derive(Debug)]
pub struct Writer<'a> {
    out: &'a mut Vec<u8>,
    big_endian: bool,
}

impl<'a> Writer<'a> {
    /// A writer appending to `out`, little-endian (SBE's default byte order).
    pub fn new(out: &'a mut Vec<u8>) -> Self {
        Self { out, big_endian: false }
    }

    /// A writer appending to `out`, big-endian, for a schema whose `byteOrder` is `bigEndian`.
    pub fn big_endian(out: &'a mut Vec<u8>) -> Self {
        Self { out, big_endian: true }
    }

    /// Appends a primitive field.
    pub fn put<T: Primitive>(&mut self, value: T) {
        let at = self.out.len();
        self.out.resize(at + T::SIZE, 0);
        if self.big_endian { value.put_be(self.out, at) } else { value.put_le(self.out, at) }
    }

    /// Appends raw bytes: a fixed-length character array (see [`crate::pad`]), or padding.
    pub fn bytes(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }

    /// Appends the standard message header.
    pub fn header(&mut self, header: Header) {
        self.put(header.block_length);
        self.put(header.template_id);
        self.put(header.schema_id);
        self.put(header.version);
    }

    /// Appends the standard group header (`groupSizeEncoding`): each entry's block length, and
    /// how many entries follow. The entries are written after it.
    ///
    /// # Errors
    ///
    /// [`SbeError::TooLong`] if `count` is over `u16::MAX`. Nothing is written.
    pub fn group_header(&mut self, block_length: u16, count: usize) -> Result<(), SbeError> {
        let count = self::count("group", count, u16::MAX)?;
        self.put(block_length);
        self.put(count);
        Ok(())
    }

    /// Appends variable-length data: its length as an `L`, then its bytes.
    ///
    /// # Errors
    ///
    /// [`SbeError::TooLong`] if `L` can't hold its length. Nothing is written.
    pub fn data<L: Length>(&mut self, data: &[u8]) -> Result<(), SbeError> {
        let len = count("data", data.len(), L::MAX)?;
        self.put(len);
        self.bytes(data);
        Ok(())
    }
}

/// Reads fields from a buffer, in wire order, each checked against its end.
#[derive(Debug, Clone, Copy)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    big_endian: bool,
}

impl<'a> Reader<'a> {
    /// A reader of `bytes`, little-endian (SBE's default byte order).
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, big_endian: false }
    }

    /// A reader of `bytes`, big-endian, for a schema whose `byteOrder` is `bigEndian`.
    #[must_use]
    pub fn big_endian(bytes: &'a [u8]) -> Self {
        Self { bytes, big_endian: true }
    }

    /// The bytes not read yet.
    #[must_use]
    pub fn remaining(&self) -> &'a [u8] {
        self.bytes
    }

    /// Whether everything has been read.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Reads a primitive field.
    ///
    /// # Errors
    ///
    /// [`SbeError::Truncated`] if the buffer ends first.
    pub fn get<T: Primitive>(&mut self) -> Result<T, SbeError> {
        let field = self.bytes(T::SIZE)?;
        Ok(if self.big_endian { T::get_be(field, 0) } else { T::get_le(field, 0) })
    }

    /// Reads `len` raw bytes: a fixed-length character array, or padding to skip.
    ///
    /// # Errors
    ///
    /// [`SbeError::Truncated`] if the buffer ends first.
    pub fn bytes(&mut self, len: usize) -> Result<&'a [u8], SbeError> {
        let taken = take(self.bytes, 0, len)?;
        self.bytes = &self.bytes[len..];
        Ok(taken)
    }

    /// Reads the standard message header.
    ///
    /// # Errors
    ///
    /// [`SbeError::Truncated`] if the buffer ends first.
    pub fn header(&mut self) -> Result<Header, SbeError> {
        Ok(Header { block_length: self.get()?, template_id: self.get()?, schema_id: self.get()?, version: self.get()? })
    }

    /// Takes the next `len` bytes, a root block or a group entry's, as a reader of their own,
    /// and moves past them. Fields the block has beyond those read (from a newer version) are
    /// skipped.
    ///
    /// # Errors
    ///
    /// [`SbeError::Truncated`] if the buffer ends first.
    pub fn block(&mut self, len: usize) -> Result<Self, SbeError> {
        Ok(Self { bytes: self.bytes(len)?, big_endian: self.big_endian })
    }

    /// Reads the standard group header (`groupSizeEncoding`): each entry's block length, and how
    /// many entries follow.
    ///
    /// # Errors
    ///
    /// [`SbeError::Truncated`] if the buffer ends first.
    pub fn group_header(&mut self) -> Result<(u16, u16), SbeError> {
        Ok((self.get()?, self.get()?))
    }

    /// Reads variable-length data with a length prefix of `L`.
    ///
    /// # Errors
    ///
    /// [`SbeError::Truncated`] if the buffer ends first.
    pub fn data<L: Length>(&mut self) -> Result<&'a [u8], SbeError> {
        let len = length(self.get::<L>()?.into())?;
        self.bytes(len)
    }
}
