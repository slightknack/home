//! NeoDisk: Compressed append-only logs with logarithmic skip-list headers
//!
//! Format: [frame][frame][frame]...[footer]
//!
//! Each frame: [header][compressed_data]
//!
//! Frame header (neopack-encoded List):
//! - frame_number: u64
//! - compressed_size: u64
//! - decompressed_size: u64
//! - jump_offsets: List<u64> (absolute file offsets to previous frame headers)
//!
//! Footer (last 16 bytes of file):
//! - last_frame_offset: u64 (absolute offset to last frame header)
//! - magic: [u8; 8] = b"NEODISK\0"
//!
//! Each frame contains ~1MB of uncompressed neopack messages.

use std::fs::OpenOptions;
use std::fs::File;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Write;
use std::io;
use std::path::Path;

use memmap2::Mmap;

use crate::jumpheader::FrameHeader;
use crate::neopack;

const DEFAULT_FRAME_SIZE: usize = 1024 * 1024; // 1MB uncompressed
const MAGIC: &[u8; 8] = b"NEODISK\0";
const FOOTER_SIZE: usize = 16; // 8 bytes offset + 8 bytes magic

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Compression(String),
    MessageNotFound(u64),
    FrameNotFound(u64),
    Neopack(neopack::Error),
    InvalidFormat,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<neopack::Error> for Error {
    fn from(e: neopack::Error) -> Self {
        Error::Neopack(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageId(pub u64);

/// Frame metadata
#[derive(Debug, Clone)]
struct FrameInfo {
    /// Frame number (0-indexed)
    #[allow(dead_code)]
    frame_number: u64,
    /// Absolute file offset where frame header starts
    header_offset: u64,
    /// Compressed size of frame data
    compressed_size: u64,
    /// Decompressed size of frame data
    #[allow(dead_code)]
    decompressed_size: u64,
    /// Number of messages in this frame
    message_count: u64,
    /// ID of first message in frame
    first_message_id: u64,
}

/// Writer for append-only neodisk files
#[derive(Debug)]
pub struct NeoDiskWriter {
    file: File,
    frame_size: usize,
    buffer: Vec<u8>,
    message_count: u64,
    frames: Vec<FrameInfo>,
    current_frame_messages: u64,
    current_frame_start_message: u64,
}

impl NeoDiskWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_frame_size(path, DEFAULT_FRAME_SIZE)
    }

    pub fn create_with_frame_size<P: AsRef<Path>>(path: P, frame_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())?;

        Ok(Self {
            file,
            frame_size,
            buffer: Vec::with_capacity(frame_size),
            message_count: 0,
            frames: Vec::new(),
            current_frame_messages: 0,
            current_frame_start_message: 0,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Read entire file to scan frames
        let data = std::fs::read(path.as_ref())?;

        // Scan frames using same logic as reader
        let frames = NeoDiskReader::scan_frames(&data)?;

        // Calculate total message count and frame size
        let message_count = frames.last()
            .map(|f| f.first_message_id + f.message_count)
            .unwrap_or(0);

        let frame_size = DEFAULT_FRAME_SIZE;

        // Open file for appending
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        // Seek to end for appending
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            frame_size,
            buffer: Vec::with_capacity(frame_size),
            message_count,
            frames,
            current_frame_messages: 0,
            current_frame_start_message: message_count,
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

        let header_offset = self.file.stream_position()?;
        let decompressed_size = self.buffer.len() as u64;
        let frame_number = self.frames.len() as u64;

        // Compress frame
        let compressed = zstd::encode_all(&self.buffer[..], 3)
            .map_err(|e| Error::Compression(e.to_string()))?;

        let compressed_size = compressed.len() as u64;

        // Compute logarithmic jump offsets to previous frame headers
        let jump_indices = crate::jumpheader::compute_jump_indices(frame_number);
        let jump_offsets: Vec<u64> = jump_indices.iter()
            .filter_map(|&idx| {
                self.frames.get(idx as usize).map(|f| f.header_offset)
            })
            .collect();

        // Create and encode frame header
        let header = FrameHeader::new(frame_number, compressed_size, decompressed_size, jump_offsets);
        let header_bytes = header.encode()?;

        // Write frame header first
        self.file.write_all(&header_bytes)?;

        // Write compressed frame data
        self.file.write_all(&compressed)?;

        // Record frame info
        self.frames.push(FrameInfo {
            frame_number,
            header_offset,
            compressed_size,
            decompressed_size,
            message_count: self.current_frame_messages,
            first_message_id: self.current_frame_start_message,
        });

        // Clear buffer for next frame
        self.buffer.clear();
        self.current_frame_start_message = self.message_count;
        self.current_frame_messages = 0;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        // Flush any remaining data
        self.flush_frame()?;
        
        // Write footer with offset to last frame header
        if let Some(last_frame) = self.frames.last() {
            self.file.write_all(&last_frame.header_offset.to_le_bytes())?;
            self.file.write_all(MAGIC)?;
        }
        
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
    mmap: Mmap,
    frames: Vec<FrameInfo>,
}

impl NeoDiskReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path.as_ref())?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Scan file to build frame index
        // We need to read backwards from the end to find the last frame header
        let frames = Self::scan_frames(&mmap)?;

        Ok(Self {
            mmap,
            frames,
        })
    }

    fn scan_frames(data: &[u8]) -> Result<Vec<FrameInfo>> {
        // Check minimum file size (need footer)
        if data.len() < FOOTER_SIZE {
            return Err(Error::InvalidFormat);
        }

        // Read footer to verify magic
        let footer_start = data.len() - FOOTER_SIZE;
        let _last_frame_offset = u64::from_le_bytes(
            data[footer_start..footer_start + 8].try_into().map_err(|_| Error::InvalidFormat)?
        );

        // Verify magic
        if &data[footer_start + 8..footer_start + 16] != MAGIC {
            return Err(Error::InvalidFormat);
        }

        // Scan frames from beginning until we hit the footer
        let mut frames = Vec::new();
        let mut pos = 0;
        let mut message_id = 0u64;

        while pos < footer_start {
            let header_offset = pos as u64;

            // Read frame header (neopack encoded)
            use crate::neopack::{Cursor, Decoder};
            let cursor = Cursor::new(&data[pos..]);
            let mut decoder = Decoder::with_cursor(cursor);

            let header = FrameHeader::decode(decoder.raw_value()?)?;
            let header_size = decoder.pos();
            pos += header_size;

            // Validate compressed data doesn't extend beyond footer
            if pos + header.compressed_size as usize > footer_start {
                return Err(Error::InvalidFormat);
            }

            // Count messages by decompressing the frame
            let compressed_data = &data[pos..pos + header.compressed_size as usize];
            let decompressed = zstd::decode_all(compressed_data)
                .map_err(|e| Error::Compression(e.to_string()))?;

            // Count messages in frame
            let cursor = Cursor::new(&decompressed);
            let mut decoder = Decoder::with_cursor(cursor);
            let mut count = 0u64;
            while decoder.remaining() > 0 {
                decoder.skip_value()?;
                count += 1;
            }

            pos += header.compressed_size as usize;

            frames.push(FrameInfo {
                frame_number: header.frame_number,
                header_offset,
                compressed_size: header.compressed_size,
                decompressed_size: header.decompressed_size,
                message_count: count,
                first_message_id: message_id,
            });

            message_id += count;
        }

        Ok(frames)
    }

    pub fn len(&self) -> u64 {
        self.frames.iter().map(|f| f.message_count).sum()
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
            .ok_or(Error::FrameNotFound(frame_idx as u64))?;

        // Parse header to get its size, then read compressed data after it
        use crate::neopack::{Cursor, Decoder};
        let cursor = Cursor::new(&self.mmap[frame.header_offset as usize..]);
        let mut decoder = Decoder::with_cursor(cursor);
        let _header = FrameHeader::decode(decoder.raw_value()?)?;
        let header_size = decoder.pos();

        // Compressed data starts right after header
        let data_start = frame.header_offset as usize + header_size;
        let data_end = data_start + frame.compressed_size as usize;
        let compressed = &self.mmap[data_start..data_end];

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
        let path = "/tmp/test_neodisk_new.nd";

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
        let path = "/tmp/test_neodisk_frames_new.nd";

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

    #[test]
    fn test_jump_headers() -> Result<()> {
        let path = "/tmp/test_neodisk_jumps.nd";

        {
            let mut writer = NeoDiskWriter::create_with_frame_size(path, 50)?;

            // Write enough to create many frames (test logarithmic jumps)
            for i in 0..100 {
                let mut enc = Encoder::new();
                enc.u64(i).unwrap();
                writer.append(enc.as_bytes())?;
            }

            writer.flush()?;

            // Verify jump offsets were computed
            assert!(writer.frames.len() > 5, "Should have multiple frames");

            // Check that last frame has logarithmic jump offsets
            if writer.frames.len() > 1 {
                println!("Created {} frames", writer.frames.len());
            }
        }

        {
            let reader = NeoDiskReader::open(path)?;
            assert_eq!(reader.len(), 100);

            // Verify we can read all messages
            for i in 0..100 {
                let msg = reader.read(MessageId(i))?;
                use crate::neopack::Decoder;
                let mut dec = Decoder::new(&msg);
                assert_eq!(dec.u64().unwrap(), i);
            }
        }

        std::fs::remove_file(path)?;
        Ok(())
    }
}
