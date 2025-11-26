mod macros;

pub mod types;
pub mod encoder;
pub mod decoder;

pub use types::Result;
pub use types::Error;
pub use types::Tag;

pub use encoder::Encoder;
pub use encoder::ListEncoder;
pub use encoder::MapEncoder;
pub use encoder::ArrayEncoder;
pub use encoder::RecordEncoder;
pub use encoder::RecordBodyEncoder;

pub use decoder::ValueReader;
pub use decoder::Reader;
pub use decoder::ListIter;
pub use decoder::MapIter;
pub use decoder::ArrayIter;
pub use decoder::RecordReader;

#[cfg(test)]
mod tests;
