//! Stateless Post-Order Tree Indexing
//!
//! This module provides constant-time, stateless mapping between a linear
//! sequence of items and a hierarchical, perfect k-ary tree structure
//! constructed over those items.
//!
//! Conceptual Model
//!
//! We imagine an append-only sequence of "items" (leaves). As items are added,
//! we implicitly construct a tree where every internal node has exactly `width`
//! children. The nodes are numbered using a Post-Order Traversal. This
//! numbering scheme has specific properties that allow us to calculate
//! relationships using only integer arithmetic and bit manipulation, without
//! allocating memory for tree nodes.
//!
//! The Index Spaces
//!
//! The module distinguishes between two types of indices to prevent confusion:
//!
//! 1. ItemId (n): The 0-based sequence number of the data leaves.
//!
//! 2. CoveringId (y): The 0-based index of a node in the post-order
//! traversal. This space includes both the leaves (items) and the internal
//! nodes (parents).
//!
//! For a binary tree (width 2), the sequence of CoveringId types looks like this:
//! Leaf, Leaf, Parent, Leaf, Leaf, Parent, Grandparent...
//!
//! Key Invariants
//!
//! 1. Post-Order Placement: A parent node always appears immediately after its
//! last (right-most) child in the CoveringIndex sequence.
//!
//! 2. Geometric Distribution: Internal nodes are inserted at regular intervals
//! based on the tree width (w). A parent of height 1 is generated every w
//! leaves; a parent of height 2 every w^2 leaves, etc.
//!
//! Algorithms
//!
//! Mapping Items to Coverings (coverings_for_item)
//!
//! When the n-th item is added, it may complete multiple levels of the tree.
//! The algorithm calculates the post-order index y by observing that the
//! position is equal to n plus the count of all internal nodes created strictly
//! before n.
//!
//! The number of internal nodes is calculated using a geometric series
//! summation:
//!
//! offset = floor(n/w) + floor(n/w^2) + ...
//!
//! The "height" of the stack of nodes created at this step is determined by the
//! divisibility of (n+1) by w (calculated via trailing zeros in base w).
//!
//! Mapping Coverings to Ranges (covering_range)
//!
//! To find what range of items a specific node y covers, we must first
//! determine if y is a leaf or an internal node. We do this by inverting the
//! mapping logic. Since the function mapping n -> y is strictly monotonic, we
//! estimate n using the density of internal nodes (approx 1/(w-1)) and refine
//! the estimate via local search.
//!
//! Once the corresponding leaf n and height h are found:
//!
//! - End: The node covers the range ending at n + 1.
//! - Start: The node covers w^h items, so start is (n + 1) - w^h.
//!
//! Tree Navigation (children_for_covering)
//!
//! Because the tree is perfect (or assumed perfect for the calculation), a node
//! at height h has exactly w children of height h - 1. Since a parent appears
//! immediately after its last child, we can calculate the indices of all
//! siblings by subtracting the size of the subtrees (w^(h-1)) iteratively,
//! stepping backwards from the parent's index.
//!
//! Time Complexity: All operations are O(log_w N), where N is the index
//! magnitude. Since N fits in 64-bit integers, these are effectively O(1)
//! constant time operations (bounded by approx 64 loops max, but usually far
//! fewer).
//!
//! Space Complexity: O(1). No memory is allocated for the tree structure.


use std::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CoveringId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ItemId(pub u64);

/// Returns the range of items covered by the node at the given covering index.
pub fn covering_range(y: CoveringId, width: u64) -> Range<ItemId> {
    assert!(width > 1 && width.is_power_of_two());

    let (n, h) = decode_covering(y.0, width);
    let len = width.pow(h);
    let end = n + 1;

    return Range { start: ItemId(end - len), end: ItemId(end) };
}

/// Returns the range of covering indices that are completed when the nth item is added.
pub fn coverings_for_item(n: ItemId, width: u64) -> Range<CoveringId> {
    assert!(width > 1 && width.is_power_of_two());

    let start = map_item_to_covering(n.0, width);
    let height = count_trailing_zeros_base_w(n.0 + 1, width);

    return Range { start: CoveringId(start), end: CoveringId(start + 1 + height as u64) };
}

/// Returns the child covering indices for a given covering index.
pub fn children_for_covering(y: CoveringId, width: u64) -> Vec<CoveringId> {
    assert!(width > 1 && width.is_power_of_two());

    let (n, h) = decode_covering(y.0, width);
    if h == 0 { return Vec::new(); }

    let child_h = h - 1;
    let stride = width.pow(child_h);

    return (0..width).rev().map(|k| {
        let child_end_n = n - (k * stride);
        CoveringId(map_item_to_covering(child_end_n, width) + child_h as u64)
    }).collect();
}

fn map_item_to_covering(n: u64, w: u64) -> u64 {
    let mut offset = 0;
    let mut div = w;
    while div <= n {
        offset += n / div;
        if n / div < w { break; }
        div *= w;
    }
    return n + offset;
}

fn count_trailing_zeros_base_w(n: u64, w: u64) -> u32 {
    if n == 0 { return 0; }
    let bits = w.trailing_zeros();
    return n.trailing_zeros() / bits;
}

fn decode_covering(y: u64, w: u64) -> (u64, u32) {
    // Approx: n ~= y * (w-1) / w
    let mut n = (y * (w - 1)) / w;

    loop {
        let start = map_item_to_covering(n, w);
        let h_max = count_trailing_zeros_base_w(n + 1, w);

        if y < start {
            n -= 1;
        } else if y > start + h_max as u64 {
            n += 1;
        } else {
            return (n, (y - start) as u32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_tree() {
        assert_eq!(covering_range(CoveringId(7), 2), ItemId(4)..ItemId(5));
        assert_eq!(covering_range(CoveringId(8), 2), ItemId(5)..ItemId(6));
    }

    #[test]
    fn quad_tree() {
        assert_eq!(covering_range(CoveringId(3), 4), ItemId(3)..ItemId(4));
        assert_eq!(covering_range(CoveringId(4), 4), ItemId(0)..ItemId(4));
    }

    #[test]
    fn item_coverings() {
        assert_eq!(coverings_for_item(ItemId(3), 4), CoveringId(3)..CoveringId(5));
        assert_eq!(coverings_for_item(ItemId(4), 4), CoveringId(5)..CoveringId(6));
    }

    #[test]
    fn children() {
        let c0 = children_for_covering(CoveringId(20), 4);
        assert_eq!(c0, vec![CoveringId(4), CoveringId(9), CoveringId(14), CoveringId(19)]);

        let c1 = children_for_covering(CoveringId(4), 4);
        assert_eq!(c1, vec![CoveringId(0), CoveringId(1), CoveringId(2), CoveringId(3)]);
    }
}
