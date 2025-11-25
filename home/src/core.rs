//! A core is an append-only log of byte messages.

use std::io::Write;
use std::ops::Range;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

const CORE_INFO: &'static str = "core.info";

#[derive(Debug)]
pub enum CoreError {
    AlreadyCached,
    NotCached,
    CoreFull,
    FutureMessage,
    Io(std::io::Error),
    CoreInfoInvalid(std::num::ParseIntError),
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
        let core_info = path.join(CORE_INFO);
        let size = std::fs::read_to_string(core_info).map_err(|e| CoreError::Io(e))?;
        let size = u16::from_str_radix(&size.trim(), 10).map_err(|e| CoreError::CoreInfoInvalid(e))?;

        let mut core = Self::create(path.to_path_buf());
        let start_id = MessageId(size);
        core.begin_flush = start_id;
        core.next_id = start_id;
        return Ok(core);
    }

    pub fn flush(&mut self) -> Result<(), CoreError> {
        // No-op if we're using an in-memory store
        let Some(ref path) = self.path else { return Ok(()) };

        let start_id = self.begin_flush;

        // Flush out all messages
        for (id, message) in self.messages.range(start_id..) {
            match message {
                Message::NotCached => return Err(CoreError::NotCached),
                Message::Cached(range) => {
                    let name = id.to_file_name();
                    let mut file = std::fs::File::create(path.join(name))
                        .map_err(|e| CoreError::Io(e))?;
                    file.write_all(&self.cache[range.clone()])
                        .map_err(|e| CoreError::Io(e))?;
                }
            }
        }

        let size = self.next_id.0;
        let mut core_info = std::fs::File::create(path.join(CORE_INFO))
            .map_err(|e| CoreError::Io(e))?;
        core_info.write_all(size.to_string().as_bytes())
            .map_err(|e| CoreError::Io(e))?;
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

    fn cache_contents(&mut self, id: MessageId, contents: &[u8]) -> Result<(), CoreError> {
        self.check_future_message(id)?;
        if self.messages.contains_key(&id) { return Err(CoreError::AlreadyCached); }

        self.cache.extend_from_slice(contents);
        let range = self.cache.len() - contents.len()..self.cache.len();
        self.messages.insert(id, Message::Cached(range));
        Ok(())
    }

    pub fn load_message(&mut self, id: MessageId) -> Result<(), CoreError> {
        self.check_future_message(id)?;
        if self.messages.contains_key(&id) { return Err(CoreError::AlreadyCached); }

        // No-op if we're using an in-memory store
        let Some(ref path) = self.path else { return Ok(()) };

        let name = id.to_file_name();
        let contents = std::fs::read(path.join(name))
            .map_err(|e| CoreError::Io(e))?;
        self.cache_contents(id, &contents)?;
        Ok(())
    }

    pub fn add_message(&mut self, contents: &[u8]) -> Result<MessageId, CoreError> {
        if self.next_id.is_full() { return Err(CoreError::CoreFull); }
        let id = self.next_id;
        self.next_id = MessageId(id.0.saturating_add(1).min(0xFFFF));
        self.cache_contents(id, contents)?;
        Ok(id)
    }

    fn get_message(&mut self, id: MessageId) -> Result<Message, CoreError> {
        self.check_future_message(id)?;
        match self.messages.get(&id) {
            Some(m) => Ok(m.clone()),
            None => {
                self.messages.insert(id, Message::NotCached);
                Ok(Message::NotCached)
            }
        }
    }

    pub fn get_contents(&mut self, id: MessageId) -> Result<&[u8], CoreError> {
        self.check_future_message(id)?;
        match self.get_message(id)? {
            Message::Cached(range) => Ok(&self.cache[range]),
            Message::NotCached => Err(CoreError::NotCached),
        }
    }
}

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

#[derive(Debug, Clone)]
pub enum Message {
    Cached(Range<usize>),
    NotCached,
}
