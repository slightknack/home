//! Compact binary format
//! This is a compact, streaming-friendly binary file format

use std::io::Read;

pub enum ArrayTag {
    Bool,
    S16,
    N16,
    S64,
    F32,
    F64,
    // Struct,
    // Slice,
}

#[repr(u8)]
pub enum Tag {
    Bool,   // 1-byte bool
    S16,    // 2-byte i16
    N16,    // 2-byte u16
    S64,    // 8-byte i64
    N64,    // 8-byte u64
    F32,    // 4-byte double
    F64,    // 8-byte double
    String, // utf-8 encoded
    Bytes,  // raw bytes
    Bitmap, // Each bit is true or false
    // Slice,  // fixed-size slice of bytes
    List,   // [bytes] = [tag][len][bytes]...[tag][len][bytes] (sequence of fields)
    Map,    // [bytes] = [field key][field val]...[field key][field val]
    Array,  // [bytes] = [item tag][bytes per item (fixed) as 8 bytes][item 0][item 1]
    Struct, // [bytes] = [num fields][field array tag 1]...[field array tag n][field 1 fixed size]...
}

pub enum TagValue {
    Bool(bool),
    S16(i16),
    N16(u16),
    S64(i64),
    N64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Bitmap(Vec<u8>),
    // Slice(...),
    List(Decoder),
    Map(DecoderMap),
    Array(DecoderArray),
    // Struct(DecoderStruct),
}

/// serialized format:
/// [tag][bytes.len() as 8 le bytes][bytes]
struct Field {
    pub tag: Tag,
    pub bytes: Vec<u8>,
    pub start: usize,
}

impl Field {
    /// Convert the bytes, decode or construct the decoder as necessary
    pub fn to_value(self) -> TagValue {

    }
}

struct Decoder {
    pub start: usize,
    pub bytes_after_start: Vec<u8>,
}

pub enum DecoderError {
    Io(std::io::Error),
    Pending,
    InvalidTag(u8),
}

impl Decoder {
    pub fn empty() -> Self {
        Decoder {
            start: 0,
            bytes_after_start: Vec::new(),
        }
    }

    pub fn new(bytes: Vec<u8>, start: usize) -> Self {
        Decoder {
            start,
            bytes_after_start: bytes,
        }
    }

    pub fn add_bytes(&mut self, bytes: &[u8]) {
        self.bytes_after_start.extend_from_slice(bytes);
    }

    pub fn add_from_reader(&mut self, reader: &mut dyn Read) -> Result<(), DecoderError> {
        let mut buffer = [0; 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(n) => self.add_bytes(&buffer[..n]),
                Err(e) => return Err(DecoderError::Io(e)),
            }
        }
    }

    /// Read tag, len:
    ///
    /// split bytes_after_start:
    /// [tag][len] | [bytes] | ...
    /// ^^^^^^^^^^   ^^^^^^^   ^^^
    /// discard      keep      new bytes_after_start
    ///
    /// - increment start by discard.len() + keep.len()
    /// - set bytes_after_start to last split
    /// - error `Pending` if not enough data has been written, do not advance
    /// - error `InvalidTag` if applicable, do not advance
    ///
    /// create field with tag, len, keep
    ///
    pub fn next_field(&mut self) -> Result<Field, DecoderError> {

    }
}

pub struct DecoderMap(pub Decoder);

impl DecoderMap {
    pub fn next_kv(&mut self) -> Result<(Field, Field), DecoderError> {
        let key = self.0.next_field()?;
        let value = self.0.next_field()?;
        return Ok((key, value));
    }
}

pub struct DecoderArray {
    pub tag: Tag,
    pub item_size: u8,
    pub decoder: Decoder,
}

impl DecoderArray {
    /// parse out:
    /// [item tag][bytes per item (fixed)][item 0][item 1]...
    /// initialize decoder with
    pub fn new(contents: Vec<u8>) -> Self {

    }
}
