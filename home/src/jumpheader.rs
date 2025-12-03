//! Logarithmic frame skip list for efficient backward traversal
//!
//! # Design
//!
//! Each frame N stores back-pointers to previous frames using a logarithmic skip list.
//! The skip list is based on the binary representation of (N - 1).
//!
//! ## Algorithm
//!
//! For frame N, we compute N - 1 as a sum of powers of two.
//! For each power of two 2^k in this sum, we store a pointer to the frame at position (N - 2^k).
//!
//! ## Example
//!
//! Frame 24: N - 1 = 23 = 16 + 4 + 2 + 1 = 2^4 + 2^2 + 2^1 + 2^0
//! Pointers: [8, 20, 22, 23] (frame 24 - 16, 24 - 4, 24 - 2, 24 - 1)
//!
//! ## Traversal Complexity
//!
//! - Space: O(log N) pointers per frame
//! - Time: O(log N) jumps to reach any previous frame
//!
//! ## Format
//!
//! ```text
//! [compressed_frame_data]
//! [frame_header]:
//!   - compressed_size: u64
//!   - decompressed_size: u64
//!   - jump_list: List<u64>  (absolute file offsets to previous frame starts)
//! ```

use crate::neopack::{Encoder, Decoder, Error as NeopackError};

#[derive(Debug, Clone)]
pub struct FrameHeader {
    /// Frame number (0-indexed)
    pub frame_number: u64,

    /// Size of compressed frame data in bytes
    pub compressed_size: u64,

    /// Size of decompressed frame data in bytes
    pub decompressed_size: u64,

    /// Logarithmic skip list: absolute file offsets to previous frame headers
    /// For frame N, contains pointers based on binary decomposition of (N-1)
    pub jump_offsets: Vec<u64>,
}

impl FrameHeader {
    /// Create a new frame header
    pub fn new(
        frame_number: u64,
        compressed_size: u64,
        decompressed_size: u64,
        jump_offsets: Vec<u64>,
    ) -> Self {
        Self {
            frame_number,
            compressed_size,
            decompressed_size,
            jump_offsets,
        }
    }

    /// Encode frame header to neopack format
    pub fn encode(&self) -> Result<Vec<u8>, NeopackError> {
        let mut enc = Encoder::new();
        let mut list = enc.list()?;
        list.u64(self.frame_number)?;
        list.u64(self.compressed_size)?;
        list.u64(self.decompressed_size)?;

        // Encode jump offsets as a nested list
        let mut jumps = list.list()?;
        for offset in &self.jump_offsets {
            jumps.u64(*offset)?;
        }
        jumps.finish()?;

        list.finish()?;
        Ok(enc.into_bytes())
    }

    /// Decode frame header from neopack format
    pub fn decode(bytes: &[u8]) -> Result<Self, NeopackError> {
        use crate::neopack::ValueDecoder;

        let mut dec = Decoder::new(bytes);
        let mut list = dec.list()?;

        let frame_number = list.next()?.ok_or(NeopackError::Malformed)?.as_u64()?;
        let compressed_size = list.next()?.ok_or(NeopackError::Malformed)?.as_u64()?;
        let decompressed_size = list.next()?.ok_or(NeopackError::Malformed)?.as_u64()?;

        let jump_list_val = list.next()?.ok_or(NeopackError::Malformed)?;
        let mut jump_list = match jump_list_val {
            ValueDecoder::List(l) => l,
            _ => return Err(NeopackError::TypeMismatch),
        };

        let mut jump_offsets = Vec::new();
        while let Some(val) = jump_list.next()? {
            jump_offsets.push(val.as_u64()?);
        }

        Ok(Self {
            frame_number,
            compressed_size,
            decompressed_size,
            jump_offsets,
        })
    }
}

/// Compute logarithmic skip list for frame N
///
/// Returns a list of frame indices to jump to, based on the binary
/// decomposition of (N - 1).
///
/// For frame N, we compute N-1 as a sum of powers of two, then store
/// the cumulative sum at each step. These are the frames we can jump back to.
///
/// # Example
///
/// ```
/// use home::jumpheader::compute_jump_indices;
///
/// // Frame 24: 23 = 16 + 4 + 2 + 1
/// // Cumulative: 16, 16+4=20, 20+2=22, 22+1=23
/// let jumps = compute_jump_indices(24);
/// assert_eq!(jumps, vec![16, 20, 22, 23]);
/// ```
pub fn compute_jump_indices(frame_index: u64) -> Vec<u64> {
    if frame_index == 0 {
        return vec![];
    }

    let n_minus_1 = frame_index - 1;
    let mut jumps = Vec::new();
    let mut accumulator = 0u64;

    // Iterate through set bits in n_minus_1 from highest to lowest
    for bit_pos in (0..64).rev() {
        if (n_minus_1 & (1 << bit_pos)) != 0 {
            let power_of_two = 1u64 << bit_pos;
            accumulator += power_of_two;
            jumps.push(accumulator);
        }
    }

    jumps
}

/// Find a jump path from frame `from` to frame `to` using logarithmic skip lists
///
/// Returns the sequence of frame indices to visit, or None if `to` >= `from`.
///
/// # Example
///
/// ```
/// use home::jumpheader::find_jump_path;
///
/// // Jump from frame 24 to frame 17
/// let path = find_jump_path(24, 17).unwrap();
/// // Possible path: [24, 20, 18, 17]
/// ```
pub fn find_jump_path(from: u64, to: u64) -> Option<Vec<u64>> {
    if to >= from {
        return None;
    }

    let mut path = vec![from];
    let mut current = from;

    while current > to {
        let jumps = compute_jump_indices(current);

        if jumps.is_empty() {
            // No jumps available, can only go to frame 0
            if to == 0 {
                path.push(0);
                return Some(path);
            }
            return None;
        }

        // Find the smallest jump that is still >= to (closest to target without overshooting)
        let next = jumps.iter()
            .find(|&&jump_target| jump_target >= to)
            .copied()?;

        path.push(next);
        current = next;
    }

    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_jump_indices() {
        // Frame 0: no jumps
        assert_eq!(compute_jump_indices(0), vec![]);

        // Frame 1: 0 = (nothing), no back pointers
        assert_eq!(compute_jump_indices(1), vec![]);

        // Frame 2: 1 = 2^0, cumulative: 1
        assert_eq!(compute_jump_indices(2), vec![1]);

        // Frame 8: 7 = 4 + 2 + 1 = 2^2 + 2^1 + 2^0
        // Cumulative (high to low): 4, 4+2=6, 6+1=7
        assert_eq!(compute_jump_indices(8), vec![4, 6, 7]);

        // Frame 24: 23 = 16 + 4 + 2 + 1
        // Cumulative (high to low): 16, 16+4=20, 20+2=22, 22+1=23
        assert_eq!(compute_jump_indices(24), vec![16, 20, 22, 23]);
    }

    #[test]
    fn show_all_jumps_0_to_255() {
        for i in 0..=255 {
            let jumps = compute_jump_indices(i);
            println!("{}: {:?}", i, jumps);
        }
    }

    #[test]
    fn test_find_jump_path() {
        // Jump from 24 to 17
        let path = find_jump_path(24, 17).unwrap();
        assert_eq!(path[0], 24);
        assert_eq!(path[path.len() - 1], 17);

        // Verify each jump is valid
        for i in 1..path.len() {
            let jumps = compute_jump_indices(path[i - 1]);
            assert!(jumps.contains(&path[i]),
                "Frame {} cannot jump to frame {}", path[i - 1], path[i]);
        }

        // Jump from 100 to 50
        let path = find_jump_path(100, 50).unwrap();
        // println!("{:?}", path);
        // panic!();
        assert_eq!(path[0], 100);
        assert_eq!(path[path.len() - 1], 50);
        assert!(path.len() <= 7, "Path should be logarithmic: {} steps", path.len());
    }

    #[test]
    fn test_jump_path_bounds() {
        // Cannot jump forward
        assert_eq!(find_jump_path(10, 20), None);

        // Cannot jump to same frame
        assert_eq!(find_jump_path(10, 10), None);
    }

    #[test]
    fn test_frame_header_encode_decode() {
        let header = FrameHeader::new(
            42,
            12345,
            1048576,
            vec![0, 1000, 2000, 3000],
        );

        let encoded = header.encode().unwrap();
        let decoded = FrameHeader::decode(&encoded).unwrap();

        assert_eq!(decoded.frame_number, header.frame_number);
        assert_eq!(decoded.compressed_size, header.compressed_size);
        assert_eq!(decoded.decompressed_size, header.decompressed_size);
        assert_eq!(decoded.jump_offsets, header.jump_offsets);
    }
}
