mod macros;

pub mod types;
pub mod encoder;
pub mod decoder;

pub use types::{Tag, Error, Result};
pub use encoder::{Encoder, ListEncoder, MapEncoder, ArrayEncoder};
pub use decoder::{Reader, ValueReader, ListIter, MapIter, ArrayIter, RecordReader};

#[cfg(test)]
mod tests;
