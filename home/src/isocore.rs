//! IsoCore: Append-only Merkle DAG using covering tree indexing
//!
//! An IsoCore maintains two cores:
//!
//! - data_core: Stores actual message data
//! - verkle_core: Stores tree structure (nodes)
//!
//! IsoCore uses the covering tree module to precisely calculate which
//! nodes need to be created when each item is added. This gives us:
//!
//! - O(1) space: No need to track a forest of roots
//! - Deterministic: The tree structure is fully determined by the count
//! - Stateless navigation: Can compute any node's children without state

use std::path::PathBuf;
use crate::core::{Core, CoreError, MessageId};
use crate::key::{Hash, hash};
use crate::covering::{CoveringId, ItemId, coverings_for_item, children_for_covering};

const WIDTH: u64 = 8;

#[derive(Debug)]
pub enum IsoCoreError {
    Core(CoreError),
    Utf8,
    NodeFormat,
    NodeType,
    HexEncoding,
    MessageIdParse(std::num::ParseIntError),
}

impl From<CoreError> for IsoCoreError {
    fn from(e: CoreError) -> Self {
        return IsoCoreError::Core(e);
    }
}

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
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();

        let root_hash = self.compute_hash();
        out.extend_from_slice(&root_hash.to_hex());
        out.push(b'\n');

        for child in &self.children {
            let type_str: &[u8] = match child.node_type {
                NodeType::Leaf => b"leaf",
                NodeType::Branch => b"branch",
            };
            out.extend_from_slice(type_str);
            out.push(b' ');
            out.extend_from_slice(&child.hash.to_hex());
            out.push(b' ');
            out.extend_from_slice(child.index.to_file_name().as_bytes());
            out.push(b'\n');
        }

        return out;
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IsoCoreError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| IsoCoreError::Utf8)?;

        let mut lines = text.lines();
        lines.next();

        let children: Result<Vec<_>, _> = lines
            .filter(|line| !line.trim().is_empty())
            .map(parse_child_line)
            .collect();

        return Ok(VerkleNode { children: children? });
    }

    pub fn compute_hash(&self) -> Hash {
        let mut data = Vec::new();
        for child in &self.children {
            data.extend_from_slice(&child.hash.0);
        }
        return hash(&data);
    }
}

#[derive(Debug)]
pub struct IsoCore {
    pub data_core: Core,
    pub verkle_core: Core,
    pub size: u16,
}

impl IsoCore {
    pub fn create_mem() -> Self {
        return Self {
            data_core: Core::create_mem(),
            verkle_core: Core::create_mem(),
            size: 0,
        };
    }

    pub fn create(path: PathBuf) -> Self {
        let data_path = path.join("data");
        let verkle_path = path.join("verkle");

        return Self {
            data_core: Core::create(data_path),
            verkle_core: Core::create(verkle_path),
            size: 0,
        };
    }

    pub fn add_message(&mut self, message: &[u8]) -> Result<Hash, IsoCoreError> {
        let data_index = self.data_core.add_message(message)?;
        let msg_hash = hash(message);

        let item_id = ItemId(self.size as u64);
        let covering_range = coverings_for_item(item_id, WIDTH);

        for covering_id in covering_range.start.0..covering_range.end.0 {
            let covering_id = CoveringId(covering_id);
            let node = self.build_node(covering_id, msg_hash.clone(), data_index)?;
            let node_bytes = node.to_bytes();
            self.verkle_core.add_message(&node_bytes)?;
        }

        self.size += 1;

        return self.get_root_hash();
    }

    fn build_node(&mut self, covering_id: CoveringId, leaf_hash: Hash, leaf_index: MessageId) -> Result<VerkleNode, IsoCoreError> {
        let children_ids = children_for_covering(covering_id, WIDTH);

        if children_ids.is_empty() {
            return Ok(VerkleNode {
                children: vec![NodeChild {
                    node_type: NodeType::Leaf,
                    hash: leaf_hash,
                    index: leaf_index,
                }],
            });
        }

        let mut children = Vec::new();
        for child_covering_id in children_ids {
            let child_verkle_id = MessageId(child_covering_id.0 as u16);

            self.verkle_core.load_message(child_verkle_id)?;
            let child_bytes = self.verkle_core.get_contents(child_verkle_id)?;
            let child_node = VerkleNode::from_bytes(child_bytes)?;
            let child_hash = child_node.compute_hash();

            children.push(NodeChild {
                node_type: NodeType::Branch,
                hash: child_hash,
                index: child_verkle_id,
            });
        }

        return Ok(VerkleNode { children });
    }

    fn get_root_hash(&mut self) -> Result<Hash, IsoCoreError> {
        if self.size == 0 {
            return Ok(hash(&[]));
        }

        let last_item = ItemId((self.size - 1) as u64);
        let covering_range = coverings_for_item(last_item, WIDTH);
        let root_covering = CoveringId(covering_range.end.0 - 1);
        let root_verkle = MessageId(root_covering.0 as u16);

        self.verkle_core.load_message(root_verkle)?;
        let root_bytes = self.verkle_core.get_contents(root_verkle)?;
        let root_node = VerkleNode::from_bytes(root_bytes)?;

        return Ok(root_node.compute_hash());
    }

    pub fn len(&self) -> u16 {
        return self.size;
    }
}

fn parse_child_line(line: &str) -> Result<NodeChild, IsoCoreError> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(IsoCoreError::NodeFormat);
    }

    let node_type = match parts[0] {
        "leaf" => NodeType::Leaf,
        "branch" => NodeType::Branch,
        _ => return Err(IsoCoreError::NodeType),
    };

    let hash = Hash::from_hex(parts[1]);
    let index_str = parts[2].trim_end_matches(".bin");
    let index_num = u16::from_str_radix(index_str, 16)
        .map_err(|e| IsoCoreError::MessageIdParse(e))?;
    let index = MessageId(index_num);

    return Ok(NodeChild {
        node_type,
        hash,
        index,
    });
}
