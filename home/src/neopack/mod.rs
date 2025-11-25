pub mod types;
pub mod encoder;
pub mod decoder;

pub use types::{Tag, Error, Result};
pub use encoder::{Encoder, ListEncoder, MapEncoder, MapValueEncoder, ArrayEncoder};
pub use decoder::{Reader, ValueReader, ListIter, MapIter, ArrayIter, StructReader};

#[cfg(test)]
mod tests;

