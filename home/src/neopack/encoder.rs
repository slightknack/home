//! Encoder for neopack binary format

use super::types::{Tag, Error, Result};

pub struct Encoder {
    pub buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    fn write_tag(&mut self, tag: Tag) {
        self.buf.push(tag as u8);
    }

    fn write_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn write_blob(&mut self, tag: Tag, data: &[u8]) -> Result<()> {
        if data.len() > u16::MAX as usize {
            return Err(Error::BlobTooLarge(data.len()));
        }
        self.write_tag(tag);
        self.write_u16(data.len() as u16);
        self.buf.extend_from_slice(data);
        Ok(())
    }

    // Scalars
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.write_tag(Tag::Bool);
        self.buf.push(if v { 1 } else { 0 });
        self
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.write_tag(Tag::U8);
        self.buf.push(v);
        self
    }

    pub fn i8(&mut self, v: i8) -> &mut Self {
        self.write_tag(Tag::S8);
        self.buf.push(v as u8);
        self
    }

    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.write_tag(Tag::U16);
        self.write_u16(v);
        self
    }

    pub fn i16(&mut self, v: i16) -> &mut Self {
        self.write_tag(Tag::S16);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.write_tag(Tag::U32);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        self.write_tag(Tag::S32);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.write_tag(Tag::U64);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.write_tag(Tag::S64);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn f32(&mut self, v: f32) -> &mut Self {
        self.write_tag(Tag::F32);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.write_tag(Tag::F64);
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    // Blobs
    pub fn str(&mut self, v: &str) -> Result<&mut Self> {
        self.write_blob(Tag::String, v.as_bytes())?;
        Ok(self)
    }

    pub fn bytes(&mut self, v: &[u8]) -> Result<&mut Self> {
        self.write_blob(Tag::Bytes, v)?;
        Ok(self)
    }

    pub fn struct_blob(&mut self, v: &[u8]) -> Result<&mut Self> {
        self.write_blob(Tag::Struct, v)?;
        Ok(self)
    }

    // Container Starters
    pub fn list(&mut self) -> ListEncoder<'_> {
        let start = self.buf.len();
        self.write_tag(Tag::List);
        self.write_u16(0); // Placeholder count
        ListEncoder {
            parent: self,
            start,
            count: 0,
        }
    }

    pub fn map(&mut self) -> MapEncoder<'_> {
        let start = self.buf.len();
        self.write_tag(Tag::Map);
        self.write_u16(0); // Placeholder count
        MapEncoder {
            parent: self,
            start,
            count: 0,
        }
    }

    pub fn array(&mut self, item_tag: Tag, stride: usize) -> Result<ArrayEncoder<'_>> {
        if stride == 0 {
            return Err(Error::InvalidStride(0));
        }
        if stride > u16::MAX as usize {
            return Err(Error::InvalidStride(stride));
        }
        let start = self.buf.len();
        self.write_tag(Tag::Array);
        self.buf.push(item_tag as u8);
        self.write_u16(stride as u16);
        self.write_u16(0); // Placeholder count
        Ok(ArrayEncoder {
            parent: self,
            start,
            stride,
            count: 0,
        })
    }
}

pub struct ListEncoder<'a> {
    pub parent: &'a mut Encoder,
    pub start: usize,
    pub count: u16,
}

impl<'a> ListEncoder<'a> {
    fn inc_count(&mut self) -> Result<()> {
        self.count = self.count.checked_add(1)
            .ok_or(Error::ContainerFull)?;
        Ok(())
    }

    pub fn bool(&mut self, v: bool) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.bool(v);
        Ok(self)
    }

    pub fn u8(&mut self, v: u8) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.u8(v);
        Ok(self)
    }

    pub fn i8(&mut self, v: i8) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.i8(v);
        Ok(self)
    }

    pub fn u16(&mut self, v: u16) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.u16(v);
        Ok(self)
    }

    pub fn i16(&mut self, v: i16) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.i16(v);
        Ok(self)
    }

    pub fn u32(&mut self, v: u32) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.u32(v);
        Ok(self)
    }

    pub fn i32(&mut self, v: i32) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.i32(v);
        Ok(self)
    }

    pub fn u64(&mut self, v: u64) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.u64(v);
        Ok(self)
    }

    pub fn i64(&mut self, v: i64) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.i64(v);
        Ok(self)
    }

    pub fn f32(&mut self, v: f32) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.f32(v);
        Ok(self)
    }

    pub fn f64(&mut self, v: f64) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.f64(v);
        Ok(self)
    }

    pub fn str(&mut self, v: &str) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.str(v)?;
        Ok(self)
    }

    pub fn bytes(&mut self, v: &[u8]) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.bytes(v)?;
        Ok(self)
    }

    pub fn struct_blob(&mut self, v: &[u8]) -> Result<&mut Self> {
        self.inc_count()?;
        self.parent.struct_blob(v)?;
        Ok(self)
    }

    pub fn list(&mut self) -> Result<ListEncoder<'_>> {
        self.inc_count()?;
        Ok(self.parent.list())
    }

    pub fn map(&mut self) -> Result<MapEncoder<'_>> {
        self.inc_count()?;
        Ok(self.parent.map())
    }

    pub fn array(&mut self, item_tag: Tag, stride: usize) -> Result<ArrayEncoder<'_>> {
        self.inc_count()?;
        self.parent.array(item_tag, stride)
    }

    pub fn finish(self) -> &'a mut Encoder {
        // Patch count before returning
        let count_bytes = self.count.to_le_bytes();
        self.parent.buf[self.start + 1] = count_bytes[0];
        self.parent.buf[self.start + 2] = count_bytes[1];
        let parent_ptr = self.parent as *mut Encoder;
        std::mem::forget(self);
        unsafe { &mut *parent_ptr }
    }
}

impl<'a> Drop for ListEncoder<'a> {
    fn drop(&mut self) {
        // Patch count at start + 1 (after tag)
        let count_bytes = self.count.to_le_bytes();
        self.parent.buf[self.start + 1] = count_bytes[0];
        self.parent.buf[self.start + 2] = count_bytes[1];
    }
}

pub struct MapEncoder<'a> {
    pub parent: &'a mut Encoder,
    pub start: usize,
    pub count: u16,
}

impl<'a> MapEncoder<'a> {
    /// Write a map entry with a closure that writes the value.
    /// This ensures values are always written for keys.
    pub fn entry<F>(&mut self, key: &str, f: F) -> Result<()>
    where
        F: FnOnce(&mut Encoder) -> Result<()>,
    {
        self.parent.str(key)?;
        f(self.parent)?;
        self.count = self.count.checked_add(1)
            .ok_or(Error::ContainerFull)?;
        Ok(())
    }

    /// Legacy API: Write a key and get a value encoder.
    /// WARNING: You MUST call a method on the returned MapValueEncoder.
    /// Dropping it without writing a value will panic.
    #[deprecated(since = "0.1.0", note = "Use entry() instead for compile-time safety")]
    pub fn key(&mut self, k: &str) -> Result<MapValueEncoder<'_>> {
        self.parent.str(k)?;
        self.count = self.count.checked_add(1)
            .ok_or(Error::ContainerFull)?;
        Ok(MapValueEncoder {
            parent: &mut self.parent,
            consumed: false,
        })
    }

    pub fn finish(self) -> &'a mut Encoder {
        // Patch count before returning
        let count_bytes = self.count.to_le_bytes();
        self.parent.buf[self.start + 1] = count_bytes[0];
        self.parent.buf[self.start + 2] = count_bytes[1];
        let parent_ptr = self.parent as *mut Encoder;
        std::mem::forget(self);
        unsafe { &mut *parent_ptr }
    }
}

impl<'a> Drop for MapEncoder<'a> {
    fn drop(&mut self) {
        // Patch count at start + 1
        let count_bytes = self.count.to_le_bytes();
        self.parent.buf[self.start + 1] = count_bytes[0];
        self.parent.buf[self.start + 2] = count_bytes[1];
    }
}

#[must_use = "map value must be provided by calling a value method"]
pub struct MapValueEncoder<'a> {
    pub parent: &'a mut Encoder,
    pub consumed: bool,
}

impl<'a> MapValueEncoder<'a> {
    pub fn bool(mut self, v: bool) {
        self.parent.bool(v);
        self.consumed = true;
    }

    pub fn u8(mut self, v: u8) {
        self.parent.u8(v);
        self.consumed = true;
    }

    pub fn i8(mut self, v: i8) {
        self.parent.i8(v);
        self.consumed = true;
    }

    pub fn u16(mut self, v: u16) {
        self.parent.u16(v);
        self.consumed = true;
    }

    pub fn i16(mut self, v: i16) {
        self.parent.i16(v);
        self.consumed = true;
    }

    pub fn u32(mut self, v: u32) {
        self.parent.u32(v);
        self.consumed = true;
    }

    pub fn i32(mut self, v: i32) {
        self.parent.i32(v);
        self.consumed = true;
    }

    pub fn u64(mut self, v: u64) {
        self.parent.u64(v);
        self.consumed = true;
    }

    pub fn i64(mut self, v: i64) {
        self.parent.i64(v);
        self.consumed = true;
    }

    pub fn f32(mut self, v: f32) {
        self.parent.f32(v);
        self.consumed = true;
    }

    pub fn f64(mut self, v: f64) {
        self.parent.f64(v);
        self.consumed = true;
    }

    pub fn str(mut self, v: &str) -> Result<()> {
        self.parent.str(v)?;
        self.consumed = true;
        Ok(())
    }

    pub fn bytes(mut self, v: &[u8]) -> Result<()> {
        self.parent.bytes(v)?;
        self.consumed = true;
        Ok(())
    }

    pub fn struct_blob(mut self, v: &[u8]) -> Result<()> {
        self.parent.struct_blob(v)?;
        self.consumed = true;
        Ok(())
    }

    pub fn list(self) -> ListEncoder<'a> {
        let parent_ptr = self.parent as *mut Encoder;
        self.consumed = true;
        unsafe { &mut *parent_ptr }.list()
    }

    pub fn map(self) -> MapEncoder<'a> {
        let parent_ptr = self.parent as *mut Encoder;
        self.consumed = true;
        unsafe { &mut *parent_ptr }.map()
    }

    pub fn array(self, item_tag: Tag, stride: usize) -> Result<ArrayEncoder<'a>> {
        let parent_ptr = self.parent as *mut Encoder;
        self.consumed = true;
        unsafe { &mut *parent_ptr }.array(item_tag, stride)
    }
}

impl<'a> Drop for MapValueEncoder<'a> {
    fn drop(&mut self) {
        // If value was never written (mem::forget not called), this is a programmer error
        if !self.consumed {
            panic!("MapValueEncoder dropped without writing a value - this violates the API contract that every key must have a value");
        }
    }
}

pub struct ArrayEncoder<'a> {
    pub parent: &'a mut Encoder,
    pub start: usize,
    pub stride: usize,
    pub count: u16,
}

impl<'a> ArrayEncoder<'a> {
    pub fn push(&mut self, data: &[u8]) -> Result<()> {
        if data.len() != self.stride {
            return Err(Error::Malformed);
        }
        self.count = self.count.checked_add(1)
            .ok_or(Error::ContainerFull)?;
        self.parent.buf.extend_from_slice(data);
        Ok(())
    }

    pub fn finish(self) -> &'a mut Encoder {
        // Patch count before returning
        let count_bytes = self.count.to_le_bytes();
        self.parent.buf[self.start + 4] = count_bytes[0];
        self.parent.buf[self.start + 5] = count_bytes[1];
        let parent_ptr = self.parent as *mut Encoder;
        self.consumed = true;
        unsafe { &mut *parent_ptr }
    }
}

impl<'a> Drop for ArrayEncoder<'a> {
    fn drop(&mut self) {
        // Patch count at start + 1 (tag) + 1 (item_tag) + 2 (stride)
        let count_bytes = self.count.to_le_bytes();
        self.parent.buf[self.start + 4] = count_bytes[0];
        self.parent.buf[self.start + 5] = count_bytes[1];
    }
}
