use super::types::{Tag, Error, Result};
use super::macros::{
    encode_root_multibyte, encode_array_multibyte, encode_record_multibyte,
    encode_wrapper_api, encode_wrapper_method, for_each_multibyte_scalar
};
use std::mem;

/// A growable buffer that encodes data into the NeoPack format.
pub struct Encoder {
    pub buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    #[inline(always)]
    fn write_tag(&mut self, tag: Tag) {
        self.buf.push(tag as u8);
    }

    #[inline(always)]
    fn write_u32_raw(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn write_blob(&mut self, tag: Tag, data: &[u8]) -> Result<()> {
        if data.len() > u32::MAX as usize {
            return Err(Error::BlobTooLarge(data.len()));
        }
        self.write_tag(tag);
        self.write_u32_raw(data.len() as u32);
        self.buf.extend_from_slice(data);
        Ok(())
    }

    #[inline]
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.write_tag(Tag::Bool);
        self.buf.push(v as u8);
        self
    }

    #[inline]
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.write_tag(Tag::U8);
        self.buf.push(v);
        self
    }

    #[inline]
    pub fn i8(&mut self, v: i8) -> &mut Self {
        self.write_tag(Tag::S8);
        self.buf.push(v as u8);
        self
    }

    for_each_multibyte_scalar!(encode_root_multibyte, ());

    pub fn str(&mut self, v: &str) -> Result<&mut Self> {
        self.write_blob(Tag::String, v.as_bytes())?;
        Ok(self)
    }

    pub fn bytes(&mut self, v: &[u8]) -> Result<&mut Self> {
        self.write_blob(Tag::Bytes, v)?;
        Ok(self)
    }

    pub fn record_blob(&mut self, v: &[u8]) -> Result<&mut Self> {
        self.write_blob(Tag::Struct, v)?;
        Ok(self)
    }

    pub fn list(&mut self) -> ListEncoder<'_> {
        self.write_tag(Tag::List);
        ListEncoder::new(self)
    }

    pub fn map(&mut self) -> MapEncoder<'_> {
        self.write_tag(Tag::Map);
        MapEncoder::new(self)
    }

    pub fn array(&mut self, item_tag: Tag, stride: usize) -> Result<ArrayEncoder<'_>> {
        if stride == 0 || stride > u32::MAX as usize {
            return Err(Error::InvalidStride(stride));
        }
        self.write_tag(Tag::Array);

        let len_offset = self.buf.len();
        self.write_u32_raw(0); // Placeholder for ByteLen

        self.buf.push(item_tag as u8);
        self.write_u32_raw(stride as u32);

        let body_start = len_offset + 4;

        Ok(ArrayEncoder {
            scope: PatchScope::manual(self, len_offset, body_start),
            stride,
        })
    }

    /// Starts a standard Record (opaque struct with a Tag and Length header).
    pub fn record(&mut self) -> RecordEncoder<'_> {
        self.write_tag(Tag::Struct);
        RecordEncoder {
            scope: PatchScope::new(self)
        }
    }
}

struct PatchScope<'a> {
    parent: &'a mut Encoder,
    len_offset: usize,
    body_start_offset: usize,
}

impl<'a> PatchScope<'a> {
    fn new(parent: &'a mut Encoder) -> Self {
        let len_offset = parent.buf.len();
        parent.buf.extend_from_slice(&[0; 4]);
        let body_start_offset = parent.buf.len();
        Self { parent, len_offset, body_start_offset }
    }

    fn manual(parent: &'a mut Encoder, len_offset: usize, body_start_offset: usize) -> Self {
        Self { parent, len_offset, body_start_offset }
    }

    fn flush(&mut self) {
        let current_len = self.parent.buf.len();
        let body_len = current_len.saturating_sub(self.body_start_offset);
        let len_bytes = (body_len as u32).to_le_bytes();
        let dest = &mut self.parent.buf[self.len_offset..self.len_offset + 4];
        dest.copy_from_slice(&len_bytes);
    }

    fn finish(mut self) -> &'a mut Encoder {
        self.flush();
        let parent_ptr = self.parent as *mut Encoder;
        mem::forget(self);
        unsafe { &mut *parent_ptr }
    }
}

impl<'a> Drop for PatchScope<'a> {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct ListEncoder<'a> {
    scope: PatchScope<'a>,
}

impl<'a> ListEncoder<'a> {
    fn new(parent: &'a mut Encoder) -> Self {
        Self { scope: PatchScope::new(parent) }
    }

    encode_wrapper_api!([&mut self], &mut Self, '_;
        parent: self.scope.parent;
        pre: {};
        post: self
    );

    pub fn finish(self) -> &'a mut Encoder {
        self.scope.finish()
    }
}

pub struct MapEncoder<'a> {
    scope: PatchScope<'a>,
}

impl<'a> MapEncoder<'a> {
    fn new(parent: &'a mut Encoder) -> Self {
        Self { scope: PatchScope::new(parent) }
    }

    #[must_use]
    pub fn key(&mut self, k: &str) -> Result<MapValueEncoder<'_>> {
        self.scope.parent.str(k)?;
        Ok(MapValueEncoder {
            parent: self.scope.parent,
        })
    }

    pub fn entry<F>(&mut self, key: &str, f: F) -> Result<&mut Self>
    where
        F: FnOnce(MapValueEncoder<'_>) -> Result<()>,
    {
        let val_enc = self.key(key)?;
        f(val_enc)?;
        Ok(self)
    }

    pub fn finish(self) -> &'a mut Encoder {
        self.scope.finish()
    }
}

#[must_use]
pub struct MapValueEncoder<'a> {
    parent: &'a mut Encoder,
}

impl<'a> MapValueEncoder<'a> {
    encode_wrapper_api!([self], (), 'a;
        parent: self.parent;
        pre: {};
        post: ()
    );
}

pub struct RecordEncoder<'a> {
    scope: PatchScope<'a>,
}

impl<'a> RecordEncoder<'a> {
    pub fn push(&mut self, data: &[u8]) -> Result<&mut Self> {
        self.scope.parent.buf.extend_from_slice(data);
        Ok(self)
    }

    #[inline]
    pub fn bool(&mut self, v: bool) -> Result<&mut Self> {
        self.scope.parent.write_tag(Tag::Bool);
        self.scope.parent.buf.push(v as u8);
        Ok(self)
    }

    #[inline]
    pub fn u8(&mut self, v: u8) -> Result<&mut Self> {
        self.scope.parent.write_tag(Tag::U8);
        self.scope.parent.buf.push(v);
        Ok(self)
    }

    #[inline]
    pub fn i8(&mut self, v: i8) -> Result<&mut Self> {
        self.scope.parent.write_tag(Tag::S8);
        self.scope.parent.buf.push(v as u8);
        Ok(self)
    }

    for_each_multibyte_scalar!(encode_record_multibyte, ());

    pub fn finish(self) -> &'a mut Encoder {
        self.scope.finish()
    }
}

pub struct ArrayEncoder<'a> {
    scope: PatchScope<'a>,
    stride: usize,
}

impl<'a> ArrayEncoder<'a> {
    pub unsafe fn push_unchecked(&mut self, data: &[u8]) -> Result<()> {
        self.scope.parent.buf.extend_from_slice(data);
        Ok(())
    }

    pub fn push(&mut self, data: &[u8]) -> Result<()> {
        if data.len() != self.stride {
            return Err(Error::Malformed);
        }
        unsafe { self.push_unchecked(data) }
    }

    #[inline]
    pub fn bool(&mut self, v: bool) -> Result<()> {
        self.scope.parent.write_tag(Tag::Bool);
        self.scope.parent.buf.push(v as u8);
        Ok(())
    }

    #[inline]
    pub fn u8(&mut self, v: u8) -> Result<()> {
        self.scope.parent.write_tag(Tag::U8);
        self.scope.parent.buf.push(v);
        Ok(())
    }

    #[inline]
    pub fn i8(&mut self, v: i8) -> Result<()> {
        self.scope.parent.write_tag(Tag::S8);
        self.scope.parent.buf.push(v as u8);
        Ok(())
    }

    for_each_multibyte_scalar!(encode_array_multibyte, ());

    /// Starts writing a fixed-size record into the array.
    pub fn fixed_record(&mut self) -> FixedRecordEncoder<'_, 'a> {
        let start = self.scope.parent.buf.len();
        FixedRecordEncoder {
            parent: self,
            start,
        }
    }

    pub fn finish(self) -> &'a mut Encoder {
        self.scope.finish()
    }
}

pub struct FixedRecordEncoder<'p, 'a> {
    parent: &'p mut ArrayEncoder<'a>,
    start: usize,
}

impl<'p, 'a> FixedRecordEncoder<'p, 'a> {
    pub fn push(&mut self, data: &[u8]) -> Result<&mut Self> {
        // We bypass stride checks until finish
        unsafe { self.parent.push_unchecked(data)?; }
        Ok(self)
    }

    #[inline]
    pub fn bool(&mut self, v: bool) -> Result<&mut Self> {
        self.parent.scope.parent.write_tag(Tag::Bool);
        self.parent.scope.parent.buf.push(v as u8);
        Ok(self)
    }

    #[inline]
    pub fn u8(&mut self, v: u8) -> Result<&mut Self> {
        self.parent.scope.parent.write_tag(Tag::U8);
        self.parent.scope.parent.buf.push(v);
        Ok(self)
    }

    #[inline]
    pub fn i8(&mut self, v: i8) -> Result<&mut Self> {
        self.parent.scope.parent.write_tag(Tag::S8);
        self.parent.scope.parent.buf.push(v as u8);
        Ok(self)
    }

    for_each_multibyte_scalar!(encode_record_multibyte, ());

    pub fn finish(self) -> Result<&'p mut ArrayEncoder<'a>> 
    where
        'a: 'p,
    {
        let end = self.parent.scope.parent.buf.len();
        let written = end - self.start;
        if written != self.parent.stride {
            return Err(Error::Malformed);
        }
        Ok(self.parent)
    }
}
