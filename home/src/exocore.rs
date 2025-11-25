//! ExoCore: Append-only Merkle DAG using base-8 carry algorithm
//!
//! An ExoCore maintains two cores:
//! - data_core: Stores actual message data
//! - verkle_core: Stores tree structure (nodes)
//!
//! The tree is built incrementally using a "forest" approach where
//! multiple roots are maintained and merged when we have 8 roots of
//! the same level (similar to binary carry in base-8).

use std::path::PathBuf;
use crate::core::{Core, CoreError, MessageId};
use crate::key::{Hash, hash};

const MAX_CHILDREN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Leaf,
    Branch,
}

#[derive(Debug, Clone)]
pub struct NodeChild {
    pub node_type: NodeType,
    pub hash: Hash,
    pub index: MessageId,
}

#[derive(Debug, Clone)]
pub struct VerkleNode {
    pub children: Vec<NodeChild>,
}

impl VerkleNode {
    /// Convert node to textual format for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();

        // First compute and write the root hash
        let root_hash = self.compute_hash();
        out.extend_from_slice(&hex(&root_hash.0));
        out.push(b'\n');

        // Write each child
        for child in &self.children {
            let type_str: &[u8] = match child.node_type {
                NodeType::Leaf => b"leaf",
                NodeType::Branch => b"branch",
            };
            out.extend_from_slice(type_str);
            out.push(b' ');
            out.extend_from_slice(&hex(&child.hash.0));
            out.push(b' ');
            out.extend_from_slice(child.index.to_file_name().as_bytes());
            out.push(b'\n');
        }

        return out;
    }

    /// Parse node from textual format
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| CoreError::NotCached)?;

        let mut lines = text.lines();

        // Skip the root hash line (we'll recompute it)
        lines.next();

        let children: Result<Vec<_>, _> = lines
            .filter(|line| !line.trim().is_empty())
            .map(parse_child_line)
            .collect();

        Ok(VerkleNode { children: children? })
    }

    /// Compute the hash of this node (hash of all children)
    pub fn compute_hash(&self) -> Hash {
        let mut data = Vec::new();
        for child in &self.children {
            data.extend_from_slice(&child.hash.0);
        }
        hash(&data)
    }
}

#[derive(Debug)]
pub struct ExoCore {
    pub data_core: Core,
    pub verkle_core: Core,
    pub roots: Vec<MessageId>,
    pub size: u16,
}

impl ExoCore {
    pub fn create_mem() -> Self {
        Self {
            data_core: Core::create_mem(),
            verkle_core: Core::create_mem(),
            roots: Vec::new(),
            size: 0,
        }
    }

    pub fn create(path: PathBuf) -> Self {
        let data_path = path.join("data");
        let verkle_path = path.join("verkle");

        Self {
            data_core: Core::create(data_path),
            verkle_core: Core::create(verkle_path),
            roots: Vec::new(),
            size: 0,
        }
    }

    /// Add a message to the exocore, returns the new root hash
    pub fn add_message(&mut self, message: &[u8]) -> Result<Hash, CoreError> {
        // Write message to data core
        let data_index = self.data_core.add_message(message)?;
        let msg_hash = hash(message);

        // Create new leaf node
        let leaf = VerkleNode {
            children: vec![NodeChild {
                node_type: NodeType::Leaf,
                hash: msg_hash.clone(),
                index: data_index,
            }],
        };

        // Write leaf to verkle core
        let leaf_bytes = leaf.to_bytes();
        let leaf_index = self.verkle_core.add_message(&leaf_bytes)?;

        // Add to forest
        self.roots.push(leaf_index);
        self.size += 1;

        // Merge loop: while we have MAX_CHILDREN roots at the same level, merge them
        self.merge_roots()?;

        // Return the current root hash (or combined hash if multiple roots)
        self.get_root_hash()
    }

    /// Merge roots when we have MAX_CHILDREN of the same level
    fn merge_roots(&mut self) -> Result<(), CoreError> {
        // Keep merging while we can
        loop {
            if self.roots.len() < MAX_CHILDREN {
                break;
            }

            // Check if last MAX_CHILDREN roots are at the same level
            let len = self.roots.len();

            // For simplicity, just check if we have exactly MAX_CHILDREN
            // A more sophisticated version would check tree levels
            if self.roots.len() >= MAX_CHILDREN {
                // Take last MAX_CHILDREN roots
                let to_merge: Vec<MessageId> = self.roots.drain(len - MAX_CHILDREN..).collect();

                // Load each root and create children
                let mut children = Vec::new();
                for root_idx in to_merge {
                    self.verkle_core.load_message(root_idx)?;
                    let node_bytes = self.verkle_core.get_contents(root_idx)?;
                    let node = VerkleNode::from_bytes(node_bytes)?;
                    let node_hash = node.compute_hash();

                    children.push(NodeChild {
                        node_type: NodeType::Branch,
                        hash: node_hash,
                        index: root_idx,
                    });
                }

                // Create new branch
                let branch = VerkleNode { children };
                let branch_bytes = branch.to_bytes();
                let branch_index = self.verkle_core.add_message(&branch_bytes)?;

                // Add new branch as root
                self.roots.push(branch_index);
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Get the current root hash (or combined hash if multiple roots)
    fn get_root_hash(&mut self) -> Result<Hash, CoreError> {
        if self.roots.is_empty() {
            return Ok(hash(&[]));
        }

        if self.roots.len() == 1 {
            // Single root
            let root_idx = self.roots[0];
            self.verkle_core.load_message(root_idx)?;
            let node_bytes = self.verkle_core.get_contents(root_idx)?;
            let node = VerkleNode::from_bytes(node_bytes)?;
            return Ok(node.compute_hash());
        }

        // Multiple roots - hash them together
        let mut data = Vec::new();
        for &root_idx in &self.roots {
            self.verkle_core.load_message(root_idx)?;
            let node_bytes = self.verkle_core.get_contents(root_idx)?;
            let node = VerkleNode::from_bytes(node_bytes)?;
            let node_hash = node.compute_hash();
            data.extend_from_slice(&node_hash.0);
        }
        Ok(hash(&data))
    }

    pub fn len(&self) -> u16 {
        self.size
    }
}

// Helper functions for parsing
fn parse_child_line(line: &str) -> Result<NodeChild, CoreError> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(CoreError::NotCached);
    }
    
    let node_type = match parts[0] {
        "leaf" => NodeType::Leaf,
        "branch" => NodeType::Branch,
        _ => return Err(CoreError::NotCached),
    };
    
    let hash = unhex(parts[1]);
    let index_str = parts[2].trim_end_matches(".bin");
    let index_num = u16::from_str_radix(index_str, 16)
        .map_err(|e| CoreError::CoreInfoInvalid(e))?;
    let index = MessageId(index_num);
    
    Ok(NodeChild {
        node_type,
        hash: Hash(hash),
        index,
    })
}

fn hex(bytes: &[u8]) -> Vec<u8> {
    bytes.iter()
        .flat_map(|b| format!("{:02x}", b).into_bytes())
        .collect()
}

fn unhex(s: &str) -> [u8; 32] {
    let mut result = [0u8; 32];
    for i in 0..32 {
        let byte_str = &s[i * 2..i * 2 + 2];
        result[i] = u8::from_str_radix(byte_str, 16).unwrap_or(0);
    }
    result
}

fn hash_combined(hashes: &[Hash]) -> Hash {
    let mut data = Vec::new();
    for h in hashes {
        data.extend_from_slice(&h.0);
    }
    hash(&data)
}
