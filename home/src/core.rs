//! A core is an append-only log of byte messages.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use crate::neopack;
use crate::neodisk::{NeoDiskWriter, NeoDiskReader, MessageId as DiskMessageId};

#[derive(Debug)]
pub enum CoreError {
    AlreadyCached,
    NotCached,
    CoreFull,
    FutureMessage,
    Io(std::io::Error),
    Neopack(neopack::Error),
    NeoDisk(crate::neodisk::Error),
}

impl From<std::io::Error> for CoreError {
    fn from(err: std::io::Error) -> Self {
        CoreError::Io(err)
    }
}

impl From<neopack::Error> for CoreError {
    fn from(err: neopack::Error) -> Self {
        CoreError::Neopack(err)
    }
}

impl From<crate::neodisk::Error> for CoreError {
    fn from(err: crate::neodisk::Error) -> Self {
        CoreError::NeoDisk(err)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub u16);

impl MessageId {
    pub fn to_file_name(&self) -> String {
        format!("{:04x}.bin", self.0)
    }
}

#[derive(Debug)]
pub struct Core {
    disk_writer: Option<NeoDiskWriter>,
    disk_reader: Option<NeoDiskReader>,
    cache: HashMap<MessageId, Vec<u8>>,
    next_id: MessageId,
}

impl Core {
    pub fn create_mem() -> Self {
        Self {
            disk_writer: None,
            disk_reader: None,
            cache: HashMap::new(),
            next_id: MessageId(0),
        }
    }

    pub fn create(path: PathBuf) -> Result<Self, CoreError> {
        let writer = NeoDiskWriter::create(path)?;
        Ok(Self {
            disk_writer: Some(writer),
            disk_reader: None,
            cache: HashMap::new(),
            next_id: MessageId(0),
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CoreError> {
        let path = path.as_ref();
        
        let reader = NeoDiskReader::open(&path)?;
        let size = reader.len();
        let writer = NeoDiskWriter::open(&path)?;
        
        Ok(Self {
            disk_writer: Some(writer),
            disk_reader: Some(reader),
            cache: HashMap::new(),
            next_id: MessageId(size as u16),
        })
    }

    pub fn flush(&mut self) -> Result<(), CoreError> {
        if let Some(ref mut writer) = self.disk_writer {
            writer.flush()?;
        }
        Ok(())
    }

    pub fn len(&self) -> MessageId {
        self.next_id
    }

    fn check_future_message(&self, id: MessageId) -> Result<(), CoreError> {
        if id.0 >= self.next_id.0 {
            Err(CoreError::FutureMessage)
        } else {
            Ok(())
        }
    }

    pub fn load_message(&mut self, id: MessageId) -> Result<(), CoreError> {
        self.check_future_message(id)?;
        
        // Already in cache
        if self.cache.contains_key(&id) {
            return Ok(());
        }

        // Load from disk if available - unwrap from neopack Bytes
        if let Some(ref reader) = self.disk_reader {
            let encoded = reader.read(DiskMessageId(id.0 as u64))?;
            let mut dec = neopack::Decoder::new(&encoded);
            let contents = dec.bytes()?.to_vec();
            self.cache.insert(id, contents);
        }
        
        Ok(())
    }

    pub fn add_message(&mut self, contents: &[u8]) -> Result<MessageId, CoreError> {
        if self.next_id.0 == 0xFFFF {
            return Err(CoreError::CoreFull);
        }
        
        let id = self.next_id;
        
        // Write to disk if available - wrap in neopack Bytes for framing
        if let Some(ref mut writer) = self.disk_writer {
            let mut enc = neopack::Encoder::new();
            enc.bytes(contents)?;
            writer.append(enc.as_bytes())?;
        }
        
        // Add to cache (raw contents)
        self.cache.insert(id, contents.to_vec());
        
        self.next_id = MessageId(id.0 + 1);
        Ok(id)
    }

    pub fn get_contents(&mut self, id: MessageId) -> Result<&[u8], CoreError> {
        self.check_future_message(id)?;
        self.load_message(id)?;
        
        self.cache.get(&id)
            .map(|v| v.as_slice())
            .ok_or(CoreError::NotCached)
    }
}
