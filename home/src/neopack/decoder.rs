use crate::neopack::types::Result;
use crate::neopack::types::Error;
use crate::neopack::types::Tag;
use crate::neopack::macros::impl_from_bytes;
use crate::neopack::macros::decode_array_method;
use crate::neopack::macros::decode_val_as;
use crate::neopack::macros::decode_expect_tag;
use crate::neopack::macros::decode_record_prim;
use crate::neopack::macros::for_each_scalar;

pub(crate) trait FromBytes: Sized + Copy {
    const SIZE: usize;
    fn read_from(src: &[u8]) -> Self;
}

impl FromBytes for u8 {
    const SIZE: usize = 1;
    #[inline(always)] fn read_from(src: &[u8]) -> Self { src[0] }
}
impl FromBytes for i8 {
    const SIZE: usize = 1;
    #[inline(always)] fn read_from(src: &[u8]) -> Self { src[0] as i8 }
}
impl FromBytes for bool {
    const SIZE: usize = 1;
    #[inline(always)] fn read_from(src: &[u8]) -> Self { src[0] != 0 }
}

impl_from_bytes!(u16, 2); impl_from_bytes!(i16, 2);
impl_from_bytes!(u32, 4); impl_from_bytes!(i32, 4);
impl_from_bytes!(u64, 8); impl_from_bytes!(i64, 8);
impl_from_bytes!(f32, 4); impl_from_bytes!(f64, 8);

#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    #[inline]
    fn need(&self, n: usize) -> Result<()> {
        if self.remaining() < n {
            Err(Error::Pending(n - self.remaining()))
        } else {
            Ok(())
        }
    }

    fn read_primitive<T: FromBytes>(&mut self) -> Result<T> {
        self.need(T::SIZE)?;
        let val = T::read_from(&self.buf[self.pos..]);
        self.pos += T::SIZE;
        Ok(val)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.need(len)?;
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.need(len)?;
        self.pos += len;
        Ok(())
    }

    pub fn read_tag(&mut self) -> Result<Tag> {
        let byte: u8 = self.read_primitive()?;
        Tag::from_u8(byte).ok_or(Error::InvalidTag(byte))
    }

    pub fn peek_tag(&self) -> Result<Tag> {
        self.need(1)?;
        let byte = self.buf[self.pos];
        Tag::from_u8(byte).ok_or(Error::InvalidTag(byte))
    }

    for_each_scalar!(decode_expect_tag, ());

    pub fn str(&mut self) -> Result<&'a str> {
        self.expect_blob(Tag::String, |bytes| {
            std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)
        })
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        self.expect_blob(Tag::Bytes, |b| Ok(b))
    }

    pub fn record_raw(&mut self) -> Result<&'a [u8]> {
        self.expect_blob(Tag::Struct, |b| Ok(b))
    }

    fn expect_blob<F, T>(&mut self, expected: Tag, f: F) -> Result<T>
    where
        F: FnOnce(&'a [u8]) -> Result<T>,
    {
        let tag = self.read_tag()?;
        if tag != expected {
            return Err(Error::TypeMismatch);
        }
        let len: u32 = self.read_primitive()?;
        let bytes = self.read_bytes(len as usize)?;
        f(bytes)
    }

    pub fn value(&mut self) -> Result<ValueDecoder<'a>> {
        ValueDecoder::read(self)
    }

    pub fn skip_value(&mut self) -> Result<()> {
        let tag = self.read_tag()?;
        match tag {
            Tag::Bool | Tag::U8 | Tag::S8 => self.skip(1),
            Tag::U16 | Tag::S16 => self.skip(2),
            Tag::U32 | Tag::S32 | Tag::F32 => self.skip(4),
            Tag::U64 | Tag::S64 | Tag::F64 => self.skip(8),

            Tag::String | Tag::Bytes | Tag::Struct |
            Tag::List | Tag::Map | Tag::Array => {
                let len: u32 = self.read_primitive()?;
                self.skip(len as usize)
            }
        }
    }

    pub fn list(&mut self) -> Result<ListDecoder<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::List {
            return Err(Error::TypeMismatch);
        }
        let byte_len: u32 = self.read_primitive()?;
        let bytes = self.read_bytes(byte_len as usize)?;

        Ok(ListDecoder {
            reader: Decoder::new(bytes),
            end_pos: bytes.len(),
        })
    }

    pub fn map(&mut self) -> Result<MapDecoder<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Map {
            return Err(Error::TypeMismatch);
        }
        let byte_len: u32 = self.read_primitive()?;
        let bytes = self.read_bytes(byte_len as usize)?;

        Ok(MapDecoder {
            reader: Decoder::new(bytes),
            end_pos: bytes.len(),
        })
    }

    pub fn array(&mut self) -> Result<ArrayDecoder<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Array {
            return Err(Error::TypeMismatch);
        }
        let byte_len: u32 = self.read_primitive()?;
        let bytes = self.read_bytes(byte_len as usize)?;

        // Array setup requires parsing the header from the payload
        let mut inner = Decoder::new(bytes);
        let item_tag = Tag::from_u8(inner.read_primitive()?).ok_or(Error::InvalidTag(0))?;
        let stride: u32 = inner.read_primitive()?;

        let header_size = 5; // 1 (tag) + 4 (stride)
        let payload_len = bytes.len().saturating_sub(header_size);

        if stride == 0 || payload_len % (stride as usize) != 0 { return Err(Error::Malformed); }
        let count = payload_len / (stride as usize);

        Ok(ArrayDecoder {
            reader: inner, // pos is now after header
            item_tag,
            stride: stride as usize,
            remaining: count,
        })
    }

    pub fn record(&mut self) -> Result<RecordDecoder<'a>> {
        let bytes = self.record_raw()?;
        Ok(RecordDecoder::new(bytes))
    }
}

#[derive(Debug)]
pub struct ListDecoder<'a> {
    reader: Decoder<'a>,
    end_pos: usize,
}

impl<'a> ListDecoder<'a> {
    pub fn next(&mut self) -> Result<Option<ValueDecoder<'a>>> {
        if self.reader.pos >= self.end_pos {
            return Ok(None);
        }
        ValueDecoder::read(&mut self.reader).map(Some)
    }
}

#[derive(Debug)]
pub struct MapDecoder<'a> {
    reader: Decoder<'a>,
    end_pos: usize,
}

impl<'a> MapDecoder<'a> {
    pub fn next(&mut self) -> Result<Option<(&'a str, ValueDecoder<'a>)>> {
        if self.reader.pos >= self.end_pos {
            return Ok(None);
        }

        let tag = self.reader.read_tag()?;
        if tag != Tag::String { return Err(Error::TypeMismatch); }
        let k_len: u32 = self.reader.read_primitive()?;
        let k_bytes = self.reader.read_bytes(k_len as usize)?;
        let key = std::str::from_utf8(k_bytes).map_err(|_| Error::InvalidUtf8)?;

        let val = ValueDecoder::read(&mut self.reader)?;
        Ok(Some((key, val)))
    }
}

#[derive(Debug)]
pub struct ArrayDecoder<'a> {
    reader: Decoder<'a>,
    item_tag: Tag,
    stride: usize,
    remaining: usize,
}

impl<'a> ArrayDecoder<'a> {
    pub fn item_tag(&self) -> Tag { self.item_tag }
    pub fn stride(&self) -> usize { self.stride }
    pub fn remaining(&self) -> usize { self.remaining }

    pub fn next(&mut self) -> Result<Option<ValueDecoder<'a>>> {
        if self.remaining == 0 { return Ok(None); }
        self.remaining -= 1;

        let bytes = self.reader.read_bytes(self.stride)?;
        let value = ValueDecoder::from_untagged_bytes(self.item_tag, bytes)?;

        Ok(Some(value))
    }

    pub fn skip_all(&mut self) -> Result<()> {
        if self.remaining > 0 {
            let skip = self.remaining * self.stride;
            self.reader.skip(skip)?;
            self.remaining = 0;
        }
        Ok(())
    }

    for_each_scalar!(decode_array_method, ());
}

#[derive(Debug)]
pub enum ValueDecoder<'a> {
    // Fixed-size values (can appear in arrays)
    Bool(bool),
    U8(u8),
    S8(i8),
    U16(u16),
    S16(i16),
    U32(u32),
    S32(i32),
    U64(u64),
    S64(i64),
    F32(f32),
    F64(f64),
    Bytes(&'a [u8]),  // Fixed-length in arrays
    Struct(&'a [u8]), // Fixed-length in arrays

    /// Variable-size values (cannot appear in arrays)
    Str(&'a str),
    List(ListDecoder<'a>),
    Map(MapDecoder<'a>),
    Array(ArrayDecoder<'a>),
}

impl<'a> ValueDecoder<'a> {
    /// Decodes a value from a raw slice of bytes, assuming the given Tag.
    /// This is used for Array items (where stride is known) and by the main
    /// `read` method (once it determines length).
    pub fn from_untagged_bytes(tag: Tag, bytes: &'a [u8]) -> Result<Self> {
        use ValueDecoder::*;
        match tag {
            Tag::Bool => Ok(Bool(FromBytes::read_from(bytes))),
            Tag::U8   => Ok(U8(FromBytes::read_from(bytes))),
            Tag::S8   => Ok(S8(FromBytes::read_from(bytes))),
            Tag::U16  => Ok(U16(FromBytes::read_from(bytes))),
            Tag::S16  => Ok(S16(FromBytes::read_from(bytes))),
            Tag::U32  => Ok(U32(FromBytes::read_from(bytes))),
            Tag::S32  => Ok(S32(FromBytes::read_from(bytes))),
            Tag::U64  => Ok(U64(FromBytes::read_from(bytes))),
            Tag::S64  => Ok(S64(FromBytes::read_from(bytes))),
            Tag::F32  => Ok(F32(FromBytes::read_from(bytes))),
            Tag::F64  => Ok(F64(FromBytes::read_from(bytes))),

            Tag::Bytes => Ok(Bytes(bytes)),
            Tag::Struct => Ok(Struct(bytes)),

            Tag::String => {
                let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
                Ok(ValueDecoder::Str(s))
            }

            Tag::List => {
                Ok(List(ListDecoder {
                    reader: Decoder::new(bytes),
                    end_pos: bytes.len(),
                }))
            }

            Tag::Map => {
                Ok(Map(MapDecoder {
                    reader: Decoder::new(bytes),
                    end_pos: bytes.len(),
                }))
            }

            Tag::Array => {
                let mut inner = Decoder::new(bytes);
                let item_tag = Tag::from_u8(inner.read_primitive()?).ok_or(Error::InvalidTag(0))?;
                let stride: u32 = inner.read_primitive()?;

                let header_size = 5;
                let payload_len = bytes.len().saturating_sub(header_size);

                if stride == 0 || payload_len % (stride as usize) != 0 { return Err(Error::Malformed); }
                let count = payload_len / (stride as usize);

                Ok(Array(ArrayDecoder {
                    reader: inner,
                    item_tag,
                    stride: stride as usize,
                    remaining: count,
                }))
            }
        }
    }

    pub fn read(r: &mut Decoder<'a>) -> Result<Self> {
        let tag = r.read_tag()?;

        // Determine size of the payload
        let len = match tag {
            Tag::Bool | Tag::U8 | Tag::S8 => 1,
            Tag::U16 | Tag::S16 => 2,
            Tag::U32 | Tag::S32 | Tag::F32 => 4,
            Tag::U64 | Tag::S64 | Tag::F64 => 8,

            Tag::String | Tag::Bytes | Tag::Struct |
            Tag::List | Tag::Map | Tag::Array => {
                r.read_primitive::<u32>()? as usize
            }
        };

        let bytes = r.read_bytes(len)?;
        Self::from_untagged_bytes(tag, bytes)
    }

    for_each_scalar!(decode_val_as, ());

    pub fn as_str(&self) -> Result<&'a str> {
        match self { ValueDecoder::Str(v) => Ok(*v), _ => Err(Error::TypeMismatch) }
    }

    pub fn as_bytes(&self) -> Result<&'a [u8]> {
        match self { ValueDecoder::Bytes(v) => Ok(*v), _ => Err(Error::TypeMismatch) }
    }
}

// TODO: figure out decoding validation
pub struct RecordDecoder<'a> {
    inner: Decoder<'a>,
    end: usize,
}

impl<'a> RecordDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            end: data.len(),
            inner: Decoder::new(data),
        }
    }

    pub fn new_unchecked(data: &'a [u8]) -> Self {
        Self {
            end: data.len(),
            inner: Decoder::new(data),
        }
    }

    pub fn remaining(&self) -> usize {
        self.inner.remaining()
    }

    pub fn raw(&self) -> &'a [u8] {
        self.inner.buf
    }

    for_each_scalar!(decode_record_prim, ());

    pub fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.inner.read_bytes(len)
    }
}

impl<'a> Drop for RecordDecoder<'a> {
    fn drop(&mut self) {
        if self.inner.pos != self.end {
            debug_assert!(false, "RecordReader dropped with unread bytes");
        }
    }
}
