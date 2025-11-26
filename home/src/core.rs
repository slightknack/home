//! A core is an append-only log of byte messages.

use std::io::Write;
use std::ops::Range;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use crate::neopack;
use crate::neopack::Decoder;
use crate::neopack::Encoder;

const INFO_CORE: &'static str = "info.npk";

#[derive(Debug)]
pub enum CoreError {
    AlreadyCached,
    NotCached,
    CoreFull,
    FutureMessage,
    Io(std::io::Error),
    Neopack(neopack::Error),
    BadInfoVersion(u8),
    BadInfoSchema,
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

#[derive(Debug, Clone)]
pub struct Message(Range<usize>);


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub u16);

impl MessageId {
    pub const FULL: MessageId = MessageId(0xFFFF);

    pub fn is_full(&self) -> bool {
        self.0 == 0xFFFF
    }

    pub fn to_file_name(&self) -> String {
        format!("{:04x}.bin", self.0)
    }
}

#[derive(Debug)]
pub struct Core {
    pub path: Option<PathBuf>,
    pub begin_flush: MessageId,
    pub cache: Vec<u8>,
    pub messages: BTreeMap<MessageId, Message>,
    pub next_id: MessageId,
}

impl Core {
    pub fn create_mem() -> Self {
        Self {
            path: None,
            begin_flush: MessageId(0),
            cache: Vec::new(),
            messages: BTreeMap::new(),
            next_id: MessageId(0),
        }
    }

    pub fn create(path: PathBuf) -> Self {
        Self {
            path: Some(path),
            begin_flush: MessageId(0),
            cache: Vec::new(),
            messages: BTreeMap::new(),
            next_id: MessageId(0),
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, CoreError>  {
        let path = path.as_ref();
        let size = Self::read_size(path)?;

        let mut core = Self::create(path.to_path_buf());
        let start_id = MessageId(size);
        core.begin_flush = start_id;
        core.next_id = start_id;
        return Ok(core);
    }

    fn read_size(path: &Path) -> Result<u16, CoreError> {
        let core_info = path.join(INFO_CORE);
        let bytes = std::fs::read(core_info)?;

        let mut reader = Decoder::new(&bytes);
        let mut map = reader.map()?;
        let Some(("version", version)) = map.next()? else { return Err(CoreError::BadInfoSchema) };
        let version = version.as_u8()?;
        if 0xFF != version { return Err(CoreError::BadInfoVersion(version)) };
        let Some(("size", size)) = map.next()? else { return Err(CoreError::BadInfoSchema) };
        let size = size.as_u16()?;
        if !map.next()?.is_none() { return Err(CoreError::BadInfoSchema) };

        Ok(size)
    }

    fn write_size(size: u16, path: &Path) -> Result<(), CoreError> {
        let mut core_info = std::fs::File::create(path.join(INFO_CORE))?;

        let mut encoder = Encoder::new();
        let mut map = encoder.map()?;
        map.key("version")?.u8(0xFF)?;
        map.key("size")?.u16(size)?;
        map.finish()?;

        let buf = encoder.as_bytes();
        core_info.write_all(buf)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), CoreError> {
        // No-op if we're using an in-memory store
        let Some(ref path) = self.path else { return Ok(()) };

        let start_id = self.begin_flush;

        // Flush out all messages
        for (id, message) in self.messages.range(start_id..) {
            let name = id.to_file_name();
            let mut file = std::fs::File::create(path.join(name))?;
            file.write_all(&self.cache[message.0.clone()])?;
        }

        let size = self.next_id.0;
        Self::write_size(size, path)?;

        self.begin_flush = self.next_id;
        Ok(())
    }

    pub fn len(&self) -> MessageId {
        self.next_id
    }

    fn check_future_message(&mut self, id: MessageId) -> Result<(), CoreError> {
        if id >= self.next_id {
            Err(CoreError::FutureMessage)
        } else {
            Ok(())
        }
    }

    fn cache_message(&mut self, id: MessageId, contents: &[u8]) -> Result<(), CoreError> {
        self.check_future_message(id)?;
        if self.messages.contains_key(&id) { return Err(CoreError::AlreadyCached); }

        self.cache.extend_from_slice(contents);
        let range = self.cache.len() - contents.len()..self.cache.len();
        self.messages.insert(id, Message(range));
        Ok(())
    }

    pub fn load_message(&mut self, id: MessageId) -> Result<(), CoreError> {
        self.check_future_message(id)?;
        if self.messages.contains_key(&id) { return Ok(()); }

        // No-op if we're using an in-memory store
        let Some(ref path) = self.path else { return Ok(()) };

        let name = id.to_file_name();
        let contents = std::fs::read(path.join(name))?;
        self.cache_message(id, &contents)?;
        Ok(())
    }

    pub fn add_message(&mut self, contents: &[u8]) -> Result<MessageId, CoreError> {
        if self.next_id.is_full() { return Err(CoreError::CoreFull); }
        let id = self.next_id;
        self.next_id = MessageId(id.0.saturating_add(1).min(0xFFFF));
        self.cache_message(id, contents)?;
        Ok(id)
    }

    pub fn get_contents(&mut self, id: MessageId) -> Result<&[u8], CoreError> {
        self.check_future_message(id)?;
        self.load_message(id)?;
        match self.messages.get(&id) {
            Some(message) => Ok(&self.cache[message.0.clone()]),
            None => Err(CoreError::NotCached),
        }
    }
}
