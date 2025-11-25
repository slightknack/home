use crate::neopack::types::{Tag, Error, Result};

/// Streaming decoder that can parse incrementally from a byte buffer.
/// Returns Error::Pending when more bytes are needed.
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

    fn need(&self, n: usize) -> Result<()> {
        if self.remaining() < n {
            Err(Error::Pending(n - self.remaining()))
        } else {
            Ok(())
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        self.need(1)?;
        let val = self.buf[self.pos];
        self.pos += 1;
        Ok(val)
    }

    fn read_i8(&mut self) -> Result<i8> {
        self.need(1)?;
        let val = self.buf[self.pos] as i8;
        self.pos += 1;
        Ok(val)
    }

    fn read_u16(&mut self) -> Result<u16> {
        self.need(2)?;
        let bytes = [self.buf[self.pos], self.buf[self.pos + 1]];
        self.pos += 2;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_i16(&mut self) -> Result<i16> {
        self.need(2)?;
        let bytes = [self.buf[self.pos], self.buf[self.pos + 1]];
        self.pos += 2;
        Ok(i16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32> {
        self.need(4)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_i32(&mut self) -> Result<i32> {
        self.need(4)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(i32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        self.need(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.need(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(i64::from_le_bytes(bytes))
    }

    fn read_f32(&mut self) -> Result<f32> {
        self.need(4)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(f32::from_le_bytes(bytes))
    }

    fn read_f64(&mut self) -> Result<f64> {
        self.need(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.need(len)?;
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    pub fn read_tag(&mut self) -> Result<Tag> {
        let byte = self.read_u8()?;
        Tag::from_u8(byte).ok_or(Error::InvalidTag(byte))
    }

    pub fn peek_tag(&self) -> Result<Tag> {
        self.need(1)?;
        let byte = self.buf[self.pos];
        Tag::from_u8(byte).ok_or(Error::InvalidTag(byte))
    }

    pub fn value(&mut self) -> Result<ValueReader<'a>> {
        ValueReader::read(self)
    }

    pub fn bool(&mut self) -> Result<bool> {
        let tag = self.read_tag()?;
        if tag != Tag::Bool {
            return Err(Error::TypeMismatch);
        }
        let val = self.read_u8()?;
        Ok(val != 0)
    }

    pub fn u8(&mut self) -> Result<u8> {
        let tag = self.read_tag()?;
        if tag != Tag::U8 {
            return Err(Error::TypeMismatch);
        }
        self.read_u8()
    }

    pub fn i8(&mut self) -> Result<i8> {
        let tag = self.read_tag()?;
        if tag != Tag::S8 {
            return Err(Error::TypeMismatch);
        }
        self.read_i8()
    }

    pub fn u16(&mut self) -> Result<u16> {
        let tag = self.read_tag()?;
        if tag != Tag::U16 {
            return Err(Error::TypeMismatch);
        }
        self.read_u16()
    }

    pub fn i16(&mut self) -> Result<i16> {
        let tag = self.read_tag()?;
        if tag != Tag::S16 {
            return Err(Error::TypeMismatch);
        }
        self.read_i16()
    }

    pub fn u32(&mut self) -> Result<u32> {
        let tag = self.read_tag()?;
        if tag != Tag::U32 {
            return Err(Error::TypeMismatch);
        }
        self.read_u32()
    }

    pub fn i32(&mut self) -> Result<i32> {
        let tag = self.read_tag()?;
        if tag != Tag::S32 {
            return Err(Error::TypeMismatch);
        }
        self.read_i32()
    }

    pub fn u64(&mut self) -> Result<u64> {
        let tag = self.read_tag()?;
        if tag != Tag::U64 {
            return Err(Error::TypeMismatch);
        }
        self.read_u64()
    }

    pub fn i64(&mut self) -> Result<i64> {
        let tag = self.read_tag()?;
        if tag != Tag::S64 {
            return Err(Error::TypeMismatch);
        }
        self.read_i64()
    }

    pub fn f32(&mut self) -> Result<f32> {
        let tag = self.read_tag()?;
        if tag != Tag::F32 {
            return Err(Error::TypeMismatch);
        }
        self.read_f32()
    }

    pub fn f64(&mut self) -> Result<f64> {
        let tag = self.read_tag()?;
        if tag != Tag::F64 {
            return Err(Error::TypeMismatch);
        }
        self.read_f64()
    }

    pub fn str(&mut self) -> Result<&'a str> {
        let tag = self.read_tag()?;
        if tag != Tag::String {
            return Err(Error::TypeMismatch);
        }
        let len = self.read_u16()? as usize;
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let tag = self.read_tag()?;
        if tag != Tag::Bytes {
            return Err(Error::TypeMismatch);
        }
        let len = self.read_u16()? as usize;
        self.read_bytes(len)
    }

    pub fn struct_blob(&mut self) -> Result<&'a [u8]> {
        let tag = self.read_tag()?;
        if tag != Tag::Struct {
            return Err(Error::TypeMismatch);
        }
        let len = self.read_u16()? as usize;
        self.read_bytes(len)
    }

    pub fn struct_reader(&mut self) -> Result<StructReader<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Struct {
            return Err(Error::TypeMismatch);
        }
        let len = self.read_u16()? as usize;
        let data = self.read_bytes(len)?;
        Ok(StructReader::new(data))
    }

    pub fn list(&mut self) -> Result<ListIter<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::List {
            return Err(Error::TypeMismatch);
        }
        let count = self.read_u16()? as usize;
        Ok(ListIter {
            reader: Reader { buf: self.buf, pos: self.pos },
            remaining: count,
            parent: self,
        })
    }

    pub fn map(&mut self) -> Result<MapIter<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Map {
            return Err(Error::TypeMismatch);
        }
        let count = self.read_u16()? as usize;
        Ok(MapIter {
            reader: Reader { buf: self.buf, pos: self.pos },
            remaining: count,
            parent: self,
        })
    }

    pub fn array(&mut self) -> Result<ArrayIter<'a>> {
        let tag = self.read_tag()?;
        if tag != Tag::Array {
            return Err(Error::TypeMismatch);
        }
        let item_tag_byte = self.read_u8()?;
        let item_tag = Tag::from_u8(item_tag_byte).ok_or(Error::InvalidTag(item_tag_byte))?;
        let stride = self.read_u16()? as usize;
        let count = self.read_u16()? as usize;
        
        // Validate total array size fits in u16
        let total_size = stride.checked_mul(count).ok_or(Error::Malformed)?;
        if total_size > u16::MAX as usize {
            return Err(Error::Malformed);
        }
        
        Ok(ArrayIter {
            reader: Reader { buf: self.buf, pos: self.pos },
            item_tag,
            stride,
            remaining: count,
            parent: self,
        })
    }
}

pub enum ValueReader<'a> {
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
            Tag::Bool => {
                let val = r.read_u8()?;
                Ok(ValueReader::Bool(val != 0))
            }
            Tag::U8 => Ok(ValueReader::U8(r.read_u8()?)),
            Tag::S8 => Ok(ValueReader::S8(r.read_i8()?)),
            Tag::U16 => Ok(ValueReader::U16(r.read_u16()?)),
            Tag::S16 => Ok(ValueReader::S16(r.read_i16()?)),
            Tag::U32 => Ok(ValueReader::U32(r.read_u32()?)),
            Tag::S32 => Ok(ValueReader::S32(r.read_i32()?)),
            Tag::U64 => Ok(ValueReader::U64(r.read_u64()?)),
            Tag::S64 => Ok(ValueReader::S64(r.read_i64()?)),
            Tag::F32 => Ok(ValueReader::F32(r.read_f32()?)),
            Tag::F64 => Ok(ValueReader::F64(r.read_f64()?)),
            Tag::String => {
                let len = r.read_u16()? as usize;
                let bytes = r.read_bytes(len)?;
                let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
                Ok(ValueReader::String(s))
            }
            Tag::Bytes => {
                let len = r.read_u16()? as usize;
                Ok(ValueReader::Bytes(r.read_bytes(len)?))
            }
            Tag::Struct => {
                let len = r.read_u16()? as usize;
                Ok(ValueReader::Struct(r.read_bytes(len)?))
            }
            Tag::List => {
                let count = r.read_u16()? as usize;
                Ok(ValueReader::List(ListIter {
                    reader: Reader { buf: r.buf, pos: r.pos },
                    remaining: count,
                    parent: r,
                }))
            }
            Tag::Map => {
                let count = r.read_u16()? as usize;
                Ok(ValueReader::Map(MapIter {
                    reader: Reader { buf: r.buf, pos: r.pos },
                    remaining: count,
                    parent: r,
                }))
            }
            Tag::Array => {
                let item_tag_byte = r.read_u8()?;
                let item_tag = Tag::from_u8(item_tag_byte).ok_or(Error::InvalidTag(item_tag_byte))?;
                let stride = r.read_u16()? as usize;
                let count = r.read_u16()? as usize;
                
                let total_size = stride.checked_mul(count).ok_or(Error::Malformed)?;
                if total_size > u16::MAX as usize {
                    return Err(Error::Malformed);
                }
                
                Ok(ValueReader::Array(ArrayIter {
                    reader: Reader { buf: r.buf, pos: r.pos },
                    item_tag,
                    stride,
                    remaining: count,
                    parent: r,
                }))
            }
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            ValueReader::Bool(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_u8(&self) -> Result<u8> {
        match self {
            ValueReader::U8(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_i8(&self) -> Result<i8> {
        match self {
            ValueReader::S8(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_u16(&self) -> Result<u16> {
        match self {
            ValueReader::U16(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_i16(&self) -> Result<i16> {
        match self {
            ValueReader::S16(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_u32(&self) -> Result<u32> {
        match self {
            ValueReader::U32(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_i32(&self) -> Result<i32> {
        match self {
            ValueReader::S32(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_u64(&self) -> Result<u64> {
        match self {
            ValueReader::U64(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_i64(&self) -> Result<i64> {
        match self {
            ValueReader::S64(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_f32(&self) -> Result<f32> {
        match self {
            ValueReader::F32(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_f64(&self) -> Result<f64> {
        match self {
            ValueReader::F64(v) => Ok(*v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_str(&self) -> Result<&'a str> {
        match self {
            ValueReader::String(v) => Ok(v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_bytes(&self) -> Result<&'a [u8]> {
        match self {
            ValueReader::Bytes(v) => Ok(v),
            _ => Err(Error::TypeMismatch),
        }
    }

    pub fn as_struct(&self) -> Result<&'a [u8]> {
        match self {
            ValueReader::Struct(v) => Ok(v),
            _ => Err(Error::TypeMismatch),
        }
    }
}

pub struct ListIter<'a> {
    pub reader: Reader<'a>,
    pub remaining: usize,
    pub parent: *mut Reader<'a>,
}

impl<'a> ListIter<'a> {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn next(&mut self) -> Result<Option<ValueReader<'a>>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let val = ValueReader::read(&mut self.reader)?;
        Ok(Some(val))
    }

    pub fn skip_rest(mut self) -> Result<()> {
        while self.remaining > 0 {
            self.remaining -= 1;
            ValueReader::read(&mut self.reader)?;
        }
        Ok(())
    }
}

impl<'a> Drop for ListIter<'a> {
    fn drop(&mut self) {
        // Skip remaining items to keep stream synchronized
        while self.remaining > 0 {
            self.remaining -= 1;
            // Best effort - ignore errors in Drop
            let _ = ValueReader::read(&mut self.reader);
        }
        unsafe {
            (*self.parent).pos = self.reader.pos;
        }
    }
}

pub struct MapIter<'a> {
    pub reader: Reader<'a>,
    pub remaining: usize,
    pub parent: *mut Reader<'a>,
}

impl<'a> MapIter<'a> {
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn next(&mut self) -> Result<Option<(&'a str, ValueReader<'a>)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        
        let key_tag = self.reader.read_tag()?;
        if key_tag != Tag::String {
            return Err(Error::TypeMismatch);
        }
        let key_len = self.reader.read_u16()? as usize;
        let key_bytes = self.reader.read_bytes(key_len)?;
        let key = std::str::from_utf8(key_bytes).map_err(|_| Error::InvalidUtf8)?;
        
        let value = ValueReader::read(&mut self.reader)?;
        Ok(Some((key, value)))
    }

    pub fn skip_rest(mut self) -> Result<()> {
        while self.remaining > 0 {
            self.next()?;
        }
        Ok(())
    }
}

impl<'a> Drop for MapIter<'a> {
    fn drop(&mut self) {
        // Skip remaining entries to keep stream synchronized
        while self.remaining > 0 {
            // Best effort - ignore errors in Drop
            let _ = self.next();
        }
        unsafe {
            (*self.parent).pos = self.reader.pos;
        }
    }
}

pub struct ArrayIter<'a> {
    pub reader: Reader<'a>,
    pub item_tag: Tag,
    pub stride: usize,
    pub remaining: usize,
    pub parent: *mut Reader<'a>,
}

impl<'a> ArrayIter<'a> {
    pub fn item_tag(&self) -> Tag {
        self.item_tag
    }

    pub fn stride(&self) -> usize {
        self.stride
    }

    pub fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn next(&mut self) -> Result<Option<&'a [u8]>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let chunk = self.reader.read_bytes(self.stride)?;
        Ok(Some(chunk))
    }

    pub fn skip_rest(mut self) -> Result<()> {
        let skip_bytes = self.stride.checked_mul(self.remaining).ok_or(Error::Malformed)?;
        self.reader.read_bytes(skip_bytes)?;
        self.remaining = 0;
        Ok(())
    }
}

impl<'a> Drop for ArrayIter<'a> {
    fn drop(&mut self) {
        // Arrays have known size, so we can skip to the end efficiently
        if self.remaining > 0 {
            let skip_bytes = self.stride * self.remaining;
            // Best effort - ignore errors in Drop
            let _ = self.reader.read_bytes(skip_bytes);
        }
        unsafe {
            (*self.parent).pos = self.reader.pos;
        }
    }
}

/// Reader for struct blobs with sequential field access.
/// Validates that all bytes are consumed on drop (if validate_on_drop is true).
pub struct StructReader<'a> {
    pub data: &'a [u8],
    pub pos: usize,
    pub validate_on_drop: bool,
}

impl<'a> StructReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0, validate_on_drop: true }
    }

    /// Create a StructReader without drop validation.
    /// Use this when you only need to inspect part of the struct.
    pub fn new_unchecked(data: &'a [u8]) -> Self {
        Self { data, pos: 0, validate_on_drop: false }
    }

    /// Disable drop validation. Call this if you don't want to read all fields.
    pub fn skip_validation(mut self) -> Self {
        self.validate_on_drop = false;
        self
    }

    /// Explicitly consume remaining bytes without reading them
    pub fn consume_rest(&mut self) {
        self.pos = self.data.len();
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Get the raw underlying bytes
    pub fn raw(&self) -> &'a [u8] {
        self.data
    }

    fn need(&self, n: usize) -> Result<()> {
        if self.remaining() < n {
            Err(Error::Pending(n - self.remaining()))
        } else {
            Ok(())
        }
    }

    pub fn u8(&mut self) -> Result<u8> {
        self.need(1)?;
        let val = self.data[self.pos];
        self.pos += 1;
        Ok(val)
    }

    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.u8()? as i8)
    }

    pub fn u16(&mut self) -> Result<u16> {
        self.need(2)?;
        let bytes = [self.data[self.pos], self.data[self.pos + 1]];
        self.pos += 2;
        Ok(u16::from_le_bytes(bytes))
    }

    pub fn i16(&mut self) -> Result<i16> {
        self.need(2)?;
        let bytes = [self.data[self.pos], self.data[self.pos + 1]];
        self.pos += 2;
        Ok(i16::from_le_bytes(bytes))
    }

    pub fn u32(&mut self) -> Result<u32> {
        self.need(4)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn i32(&mut self) -> Result<i32> {
        self.need(4)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(i32::from_le_bytes(bytes))
    }

    pub fn u64(&mut self) -> Result<u64> {
        self.need(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_le_bytes(bytes))
    }

    pub fn i64(&mut self) -> Result<i64> {
        self.need(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(i64::from_le_bytes(bytes))
    }

    pub fn f32(&mut self) -> Result<f32> {
        self.need(4)?;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 4]);
        self.pos += 4;
        Ok(f32::from_le_bytes(bytes))
    }

    pub fn f64(&mut self) -> Result<f64> {
        self.need(8)?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(f64::from_le_bytes(bytes))
    }

    pub fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.need(len)?;
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }
}

impl<'a> Drop for StructReader<'a> {
    fn drop(&mut self) {
        if self.validate_on_drop && self.pos != self.data.len() {
            panic!(
                "StructReader dropped with {} unread bytes (read {} of {} total)",
                self.remaining(),
                self.pos,
                self.data.len()
            );
        }
    }
}
