use crate::neopack::types::{Tag, Error, Result};
use crate::neopack::macros::{
    for_each_scalar, decode_expect_tag, decode_record_prim, decode_val_as, decode_array_method, impl_from_bytes
};

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

#[derive(Clone)]
pub struct Reader<'a> {
    pub buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
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

    pub fn record_blob(&mut self) -> Result<&'a [u8]> {
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

    pub fn value(&mut self) -> Result<ValueReader<'a>> {
        ValueReader::read(self)
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

    pub fn list(&mut self) -> Result<ListIter<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::List {
            return Err(Error::TypeMismatch);
        }
        let byte_len: u32 = self.read_primitive()?;
        let end_pos = self.pos + (byte_len as usize);
        if end_pos > self.buf.len() {
             return Err(Error::Pending(end_pos - self.buf.len()));
        }

        Ok(ListIter {
            reader: self.clone(),
            end_pos,
        })
    }

    pub fn map(&mut self) -> Result<MapIter<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Map {
            return Err(Error::TypeMismatch);
        }
        let byte_len: u32 = self.read_primitive()?;
        let end_pos = self.pos + (byte_len as usize);
        if end_pos > self.buf.len() {
             return Err(Error::Pending(end_pos - self.buf.len()));
        }

        Ok(MapIter {
            reader: self.clone(),
            end_pos,
        })
    }

    pub fn array(&mut self) -> Result<ArrayIter<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Array {
            return Err(Error::TypeMismatch);
        }
        let byte_len: u32 = self.read_primitive()?;
        let end_pos = self.pos + (byte_len as usize);
        if end_pos > self.buf.len() {
             return Err(Error::Pending(end_pos - self.buf.len()));
        }

        let item_tag = Tag::from_u8(self.read_primitive()?).ok_or(Error::InvalidTag(0))?;
        let stride: u32 = self.read_primitive()?;

        let header_size = 5;
        if byte_len < header_size { return Err(Error::Malformed); }
        let payload_len = byte_len - header_size;
        if stride == 0 || payload_len % stride != 0 { return Err(Error::Malformed); }
        let count = (payload_len / stride) as usize;

        Ok(ArrayIter {
            reader: self.clone(),
            item_tag,
            stride: stride as usize,
            remaining: count,
        })
    }

    pub fn record(&mut self) -> Result<RecordReader<'a>> {
        let bytes = self.record_blob()?;
        Ok(RecordReader::new(bytes))
    }
}

pub struct ListIter<'a> {
    reader: Reader<'a>,
    end_pos: usize,
}

impl<'a> ListIter<'a> {
    pub fn next(&mut self) -> Result<Option<ValueReader<'a>>> {
        if self.reader.pos >= self.end_pos {
            return Ok(None);
        }
        ValueReader::read(&mut self.reader).map(Some)
    }
}

pub struct MapIter<'a> {
    reader: Reader<'a>,
    end_pos: usize,
}

impl<'a> MapIter<'a> {
    pub fn next(&mut self) -> Result<Option<(&'a str, ValueReader<'a>)>> {
        if self.reader.pos >= self.end_pos {
            return Ok(None);
        }

        let tag = self.reader.read_tag()?;
        if tag != Tag::String { return Err(Error::TypeMismatch); }
        let k_len: u32 = self.reader.read_primitive()?;
        let k_bytes = self.reader.read_bytes(k_len as usize)?;
        let key = std::str::from_utf8(k_bytes).map_err(|_| Error::InvalidUtf8)?;

        let val = ValueReader::read(&mut self.reader)?;
        Ok(Some((key, val)))
    }
}

pub struct ArrayIter<'a> {
    reader: Reader<'a>,
    item_tag: Tag,
    stride: usize,
    remaining: usize,
}

impl<'a> ArrayIter<'a> {
    pub fn item_tag(&self) -> Tag { self.item_tag }
    pub fn stride(&self) -> usize { self.stride }
    pub fn remaining(&self) -> usize { self.remaining }

    pub fn next(&mut self) -> Result<Option<&'a [u8]>> {
        if self.remaining == 0 { return Ok(None); }
        self.remaining -= 1;
        self.reader.read_bytes(self.stride).map(Some)
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

pub enum ValueReader<'a> {
    Bool(bool),
    U8(u8),   S8(i8),
    U16(u16), S16(i16),
    U32(u32), S32(i32),
    U64(u64), S64(i64),
    F32(f32), F64(f64),
    String(&'a str),
    Bytes(&'a [u8]),
    Struct(&'a [u8]),
    List(ListIter<'a>),
    Map(MapIter<'a>),
    Array(ArrayIter<'a>),
}

impl<'a> ValueReader<'a> {
    fn read(r: &mut Reader<'a>) -> Result<Self> {
        let tag = r.read_tag()?;

        match tag {
            Tag::Bool => Ok(ValueReader::Bool(r.read_primitive()?)),
            Tag::U8   => Ok(ValueReader::U8(r.read_primitive()?)),
            Tag::S8   => Ok(ValueReader::S8(r.read_primitive()?)),
            Tag::U16  => Ok(ValueReader::U16(r.read_primitive()?)),
            Tag::S16  => Ok(ValueReader::S16(r.read_primitive()?)),
            Tag::U32  => Ok(ValueReader::U32(r.read_primitive()?)),
            Tag::S32  => Ok(ValueReader::S32(r.read_primitive()?)),
            Tag::U64  => Ok(ValueReader::U64(r.read_primitive()?)),
            Tag::S64  => Ok(ValueReader::S64(r.read_primitive()?)),
            Tag::F32  => Ok(ValueReader::F32(r.read_primitive()?)),
            Tag::F64  => Ok(ValueReader::F64(r.read_primitive()?)),

            Tag::String => {
                let len: u32 = r.read_primitive()?;
                let bytes = r.read_bytes(len as usize)?;
                let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
                Ok(ValueReader::String(s))
            }
            Tag::Bytes => {
                let len: u32 = r.read_primitive()?;
                Ok(ValueReader::Bytes(r.read_bytes(len as usize)?))
            }
            Tag::Struct => {
                let len: u32 = r.read_primitive()?;
                Ok(ValueReader::Struct(r.read_bytes(len as usize)?))
            }
            Tag::List => {
                let byte_len: u32 = r.read_primitive()?;
                let end_pos = r.pos + (byte_len as usize);
                let iter = ListIter {
                    reader: r.clone(),
                    end_pos,
                };
                r.pos = end_pos;
                Ok(ValueReader::List(iter))
            }
            Tag::Map => {
                let byte_len: u32 = r.read_primitive()?;
                let end_pos = r.pos + (byte_len as usize);
                let iter = MapIter {
                    reader: r.clone(),
                    end_pos,
                };
                r.pos = end_pos;
                Ok(ValueReader::Map(iter))
            }
            Tag::Array => {
                let byte_len: u32 = r.read_primitive()?;
                let item_tag = Tag::from_u8(r.read_primitive()?).ok_or(Error::InvalidTag(0))?;
                let stride: u32 = r.read_primitive()?;

                let header_size = 5;
                if byte_len < header_size { return Err(Error::Malformed); }
                let payload_len = byte_len - header_size;
                if stride == 0 || payload_len % stride != 0 { return Err(Error::Malformed); }
                let count = (payload_len / stride) as usize;

                let body_start = r.pos;
                let body_end = body_start + payload_len as usize;
                let iter = ArrayIter {
                    reader: r.clone(),
                    item_tag,
                    stride: stride as usize,
                    remaining: count,
                };
                r.pos = body_end;
                Ok(ValueReader::Array(iter))
            }
        }
    }

    for_each_scalar!(decode_val_as, ());

    pub fn as_str(&self) -> Result<&'a str> {
        match self { ValueReader::String(v) => Ok(*v), _ => Err(Error::TypeMismatch) }
    }

    pub fn as_bytes(&self) -> Result<&'a [u8]> {
        match self { ValueReader::Bytes(v) => Ok(*v), _ => Err(Error::TypeMismatch) }
    }
}

pub struct RecordReader<'a> {
    inner: Reader<'a>,
    end: usize,
    validate: bool,
}

impl<'a> RecordReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            end: data.len(),
            inner: Reader::new(data),
            validate: true
        }
    }

    pub fn new_unchecked(data: &'a [u8]) -> Self {
        Self {
            end: data.len(),
            inner: Reader::new(data),
            validate: false
        }
    }

    pub fn skip_validation(mut self) -> Self {
        self.validate = false;
        self
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

impl<'a> Drop for RecordReader<'a> {
    fn drop(&mut self) {
        if self.validate && self.inner.pos != self.end {
            debug_assert!(false, "RecordReader dropped with unread bytes");
        }
    }
}
