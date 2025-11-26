//! NeoDisk: Compressed append-only logs using zstd with mmap support
//!
//! Simplified implementation focused on core functionality:
//! - Append neopack messages
//! - Compress in frames
//! - Read messages by ID
//! - Memory-efficient via frame caching

use std::fs::{File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use memmap2::Mmap;

const MAGIC: &[u8; 8] = b"NEODISK\0";
const VERSION: u8 = 1;
const DEFAULT_FRAME_SIZE: usize = 1024 * 1024; // 1MB

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Compression(String),
    InvalidMagic,
    InvalidVersion(u8),
    MessageNotFound(u64),
    FrameNotFound(usize),
    Neopack(crate::neopack::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<crate::neopack::Error> for Error {
    fn from(e: crate::neopack::Error) -> Self {
        Error::Neopack(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageId(pub u64);

/// File header structure
#[derive(Debug)]
struct Header {
    version: u8,
    frame_size: u64,
    message_count: u64,
}

impl Header {
    fn new(frame_size: u64) -> Self {
        Self {
            version: VERSION,
            frame_size,
            message_count: 0,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        buf.extend_from_slice(MAGIC);
        buf.push(self.version);
        buf.push(0); // flags
        buf.extend_from_slice(&[0; 6]); // reserved
        buf.extend_from_slice(&self.frame_size.to_le_bytes());
        buf.extend_from_slice(&self.message_count.to_le_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 32 {
            return Err(Error::InvalidMagic);
        }
        if &bytes[0..8] != MAGIC {
            return Err(Error::InvalidMagic);
        }
        let version = bytes[8];
        if version != VERSION {
            return Err(Error::InvalidVersion(version));
        }
        let frame_size = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let message_count = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        
        Ok(Self {
            version,
            frame_size,
            message_count,
        })
    }
}

/// Frame metadata in the index
#[derive(Debug, Clone)]
struct FrameInfo {
    offset: u64,
    compressed_size: u64,
    decompressed_size: u64,
    message_count: u64,
    first_message_id: u64,
}

/// Writer for append-only neodisk files
#[derive(Debug)]
pub struct NeoDiskWriter {
    path: PathBuf,
    file: File,
    frame_size: usize,
    buffer: Vec<u8>,
    message_count: u64,
    frames: Vec<FrameInfo>,
    current_frame_messages: u64,
}

impl NeoDiskWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_frame_size(path, DEFAULT_FRAME_SIZE)
    }

    pub fn create_with_frame_size<P: AsRef<Path>>(path: P, frame_size: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // Write header
        let header = Header::new(frame_size as u64);
        file.write_all(&header.to_bytes())?;

        Ok(Self {
            path,
            file,
            frame_size,
            buffer: Vec::with_capacity(frame_size),
            message_count: 0,
            frames: Vec::new(),
            current_frame_messages: 0,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        // Read existing file to get state
        let existing_data = std::fs::read(&path)?;
        if existing_data.len() < 32 {
            return Err(Error::InvalidMagic);
        }
        
        // Parse header
        let header = Header::from_bytes(&existing_data[..32])?;
        let frame_size = header.frame_size as usize;
        
        // Parse footer (last 16 bytes: 8 for index_offset + 8 for MAGIC)
        if existing_data.len() < 48 { // header + footer minimum
            return Err(Error::InvalidMagic);
        }
        let footer_start = existing_data.len() - 16;
        let index_offset = u64::from_le_bytes(
            existing_data[footer_start..footer_start + 8].try_into().unwrap()
        ) as usize;
        
        // Verify footer magic
        if &existing_data[footer_start + 8..footer_start + 16] != MAGIC {
            return Err(Error::InvalidMagic);
        }
        
        // Parse index to rebuild frames
        let mut frames = Vec::new();
        let mut pos = index_offset;
        
        // Read frame count
        let frame_count = u64::from_le_bytes(
            existing_data[pos..pos + 8].try_into().unwrap()
        ) as usize;
        pos += 8;
        
        // Read frame entries (each is 5 * 8 = 40 bytes)
        for _ in 0..frame_count {
            let offset = u64::from_le_bytes(existing_data[pos..pos + 8].try_into().unwrap());
            let compressed_size = u64::from_le_bytes(existing_data[pos + 8..pos + 16].try_into().unwrap());
            let decompressed_size = u64::from_le_bytes(existing_data[pos + 16..pos + 24].try_into().unwrap());
            let message_count = u64::from_le_bytes(existing_data[pos + 24..pos + 32].try_into().unwrap());
            let first_message_id = u64::from_le_bytes(existing_data[pos + 32..pos + 40].try_into().unwrap());
            
            frames.push(FrameInfo {
                offset,
                compressed_size,
                decompressed_size,
                message_count,
                first_message_id,
            });
            pos += 40;
        }
        
        // Calculate total message count
        let message_count = if let Some(last_frame) = frames.last() {
            last_frame.first_message_id + last_frame.message_count
        } else {
            0
        };
        
        // Open file for appending
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        
        // Seek to before index to overwrite index/footer on next flush
        file.seek(SeekFrom::Start(index_offset as u64))?;
        
        Ok(Self {
            path,
            file,
            frame_size,
            buffer: Vec::with_capacity(frame_size),
            message_count,
            frames,
            current_frame_messages: 0,
        })
    }

    pub fn append(&mut self, message: &[u8]) -> Result<MessageId> {
        // Add message to buffer
        self.buffer.extend_from_slice(message);
        let id = MessageId(self.message_count);
        self.message_count += 1;
        self.current_frame_messages += 1;

        // Flush frame if buffer is full
        if self.buffer.len() >= self.frame_size {
            self.flush_frame()?;
        }

        Ok(id)
    }

    fn flush_frame(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let offset = self.file.stream_position()?;
        let decompressed_size = self.buffer.len() as u64;
        
        // Compress
        let compressed = zstd::encode_all(&self.buffer[..], 3)
            .map_err(|e| Error::Compression(e.to_string()))?;
        
        let compressed_size = compressed.len() as u64;

        // Write compressed frame
        self.file.write_all(&compressed)?;

        // Record frame info
        let first_message_id = self.message_count - self.current_frame_messages;
        self.frames.push(FrameInfo {
            offset,
            compressed_size,
            decompressed_size,
            message_count: self.current_frame_messages,
            first_message_id,
        });

        // Clear buffer
        self.buffer.clear();
        self.current_frame_messages = 0;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        // Flush any remaining data
        self.flush_frame()?;

        // Write index
        let index_offset = self.file.stream_position()?;
        
        // Frame count
        self.file.write_all(&(self.frames.len() as u64).to_le_bytes())?;
        
        // Frame entries
        for frame in &self.frames {
            self.file.write_all(&frame.offset.to_le_bytes())?;
            self.file.write_all(&frame.compressed_size.to_le_bytes())?;
            self.file.write_all(&frame.decompressed_size.to_le_bytes())?;
            self.file.write_all(&frame.message_count.to_le_bytes())?;
            self.file.write_all(&frame.first_message_id.to_le_bytes())?;
        }

        // Write footer
        self.file.write_all(&index_offset.to_le_bytes())?;
        self.file.write_all(MAGIC)?;

        // Update header with final message count
        self.file.seek(SeekFrom::Start(24))?;
        self.file.write_all(&self.message_count.to_le_bytes())?;
        
        self.file.sync_all()?;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.message_count
    }
}

/// Reader for neodisk files
#[derive(Debug)]
pub struct NeoDiskReader {
    _path: PathBuf,
    mmap: Mmap,
    header: Header,
    frames: Vec<FrameInfo>,
    index_offset: u64,
}

impl NeoDiskReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Read header
        let header = Header::from_bytes(&mmap[..32])?;

        // Read footer to find index
        if mmap.len() < 48 {
            return Err(Error::InvalidMagic);
        }
        let footer_pos = mmap.len() - 16;
        let index_offset = u64::from_le_bytes(mmap[footer_pos..footer_pos + 8].try_into().unwrap());
        
        if &mmap[footer_pos + 8..footer_pos + 16] != MAGIC {
            return Err(Error::InvalidMagic);
        }

        // Read index
        let frames = Self::read_index(&mmap, index_offset as usize)?;

        Ok(Self {
            _path: path,
            mmap,
            header,
            frames,
            index_offset,
        })
    }

    fn read_index(mmap: &[u8], offset: usize) -> Result<Vec<FrameInfo>> {
        let mut pos = offset;
        let frame_count = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let mut frames = Vec::with_capacity(frame_count as usize);
        for _ in 0..frame_count {
            let offset = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let compressed_size = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let decompressed_size = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let message_count = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let first_message_id = u64::from_le_bytes(mmap[pos..pos + 8].try_into().unwrap());
            pos += 8;

            frames.push(FrameInfo {
                offset,
                compressed_size,
                decompressed_size,
                message_count,
                first_message_id,
            });
        }

        Ok(frames)
    }

    pub fn len(&self) -> u64 {
        self.header.message_count
    }

    pub fn read(&self, id: MessageId) -> Result<Vec<u8>> {
        // Find frame containing this message
        let frame_idx = self.find_frame(id.0)?;
        let frame_info = &self.frames[frame_idx];

        // Decompress frame
        let decompressed = self.decompress_frame(frame_idx)?;

        // Parse messages in frame to find the right one
        let message_offset_in_frame = (id.0 - frame_info.first_message_id) as usize;
        
        use crate::neopack::{Cursor, Decoder};
        let cursor = Cursor::new(&decompressed);
        let mut decoder = Decoder::with_cursor(cursor);

        // Skip to target message
        for _ in 0..message_offset_in_frame {
            decoder.skip_value()?;
        }

        // Extract raw bytes of target message
        let msg = decoder.raw_value()?;
        Ok(msg.to_vec())
    }

    fn find_frame(&self, message_id: u64) -> Result<usize> {
        for (idx, frame) in self.frames.iter().enumerate() {
            if message_id >= frame.first_message_id 
                && message_id < frame.first_message_id + frame.message_count {
                return Ok(idx);
            }
        }
        Err(Error::MessageNotFound(message_id))
    }

    fn decompress_frame(&self, frame_idx: usize) -> Result<Vec<u8>> {
        let frame = self.frames.get(frame_idx)
            .ok_or(Error::FrameNotFound(frame_idx))?;

        let start = frame.offset as usize;
        let end = start + frame.compressed_size as usize;
        let compressed = &self.mmap[start..end];

        zstd::decode_all(compressed)
            .map_err(|e| Error::Compression(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::neopack::Encoder;

    #[test]
    fn test_write_and_read() -> Result<()> {
        let path = "/tmp/test_neodisk.nd";
        
        // Write
        {
            let mut writer = NeoDiskWriter::create(path)?;
            
            for i in 0..10 {
                let mut enc = Encoder::new();
                enc.u64(i).unwrap();
                writer.append(enc.as_bytes())?;
            }
            
            writer.flush()?;
        }

        // Read
        {
            let reader = NeoDiskReader::open(path)?;
            assert_eq!(reader.len(), 10);

            for i in 0..10 {
                let msg = reader.read(MessageId(i))?;
                use crate::neopack::Decoder;
                let mut dec = Decoder::new(&msg);
                assert_eq!(dec.u64().unwrap(), i);
            }
        }

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn test_multiple_frames() -> Result<()> {
        let path = "/tmp/test_neodisk_frames.nd";
        
        {
            let mut writer = NeoDiskWriter::create_with_frame_size(path, 100)?;
            
            // Write enough to create multiple frames
            for i in 0..50 {
                let mut enc = Encoder::new();
                enc.str(&format!("message_{}", i)).unwrap();
                writer.append(enc.as_bytes())?;
            }
            
            writer.flush()?;
        }

        {
            let reader = NeoDiskReader::open(path)?;
            assert_eq!(reader.len(), 50);

            // Read from different frames
            let msg0 = reader.read(MessageId(0))?;
            let msg25 = reader.read(MessageId(25))?;
            let msg49 = reader.read(MessageId(49))?;

            use crate::neopack::Decoder;
            let mut dec = Decoder::new(&msg0);
            assert_eq!(dec.str().unwrap(), "message_0");

            let mut dec = Decoder::new(&msg25);
            assert_eq!(dec.str().unwrap(), "message_25");

            let mut dec = Decoder::new(&msg49);
            assert_eq!(dec.str().unwrap(), "message_49");
        }

        std::fs::remove_file(path)?;
        Ok(())
    }
}
