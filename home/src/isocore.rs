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

use std::path::{Path, PathBuf};
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
}

impl IsoCore {
    pub fn create_mem() -> Self {
        return Self {
            data_core: Core::create_mem(),
            verkle_core: Core::create_mem(),
        };
    }

    pub fn create(path: PathBuf) -> Self {
        let data_path = path.join("data");
        let verkle_path = path.join("verkle");

        return Self {
            data_core: Core::create(data_path),
            verkle_core: Core::create(verkle_path),
        };
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, IsoCoreError> {
        let path = path.as_ref();
        let data_path = path.join("data");
        let verkle_path = path.join("verkle");

        return Ok(Self {
            data_core: Core::load(data_path)?,
            verkle_core: Core::load(verkle_path)?,
        });
    }

    pub fn add_message(&mut self, message: &[u8]) -> Result<Hash, IsoCoreError> {
        let data_index = self.data_core.add_message(message)?;
        let msg_hash = hash(message);

        let item_id = ItemId((self.len().0 - 1) as u64);
        let coverings = coverings_for_item(item_id, WIDTH);

        for covering_id_val in coverings.range().start.0..coverings.range().end.0 {
            let covering_id = CoveringId(covering_id_val);
            let node = self.build_node(covering_id, msg_hash.clone(), data_index)?;
            let node_bytes = node.to_bytes();
            self.verkle_core.add_message(&node_bytes)?;
        }

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
        for child_id in children_ids {
            let child_node = self.get_node(child_id)?;
            children.push(NodeChild {
                node_type: NodeType::Branch,
                hash: child_node.compute_hash(),
                index: child_id.to_verkle_id(),
            });
        }

        return Ok(VerkleNode { children });
    }

    fn get_root_hash(&mut self) -> Result<Hash, IsoCoreError> {
        let len = self.len();
        if len.0 == 0 {
            return Ok(hash(&[]));
        }

        let last_item = ItemId((len.0 - 1) as u64);
        let coverings = coverings_for_item(last_item, WIDTH);
        let root_node = self.get_node(coverings.root())?;

        return Ok(root_node.compute_hash());
    }

    pub fn len(&self) -> MessageId {
        return self.data_core.len();
    }

    fn load_node(&mut self, covering_id: CoveringId) -> Result<(), IsoCoreError> {
        let verkle_id = covering_id.to_verkle_id();
        self.verkle_core.load_message(verkle_id)?;
        return Ok(());
    }

    fn get_node(&mut self, covering_id: CoveringId) -> Result<VerkleNode, IsoCoreError> {
        self.load_node(covering_id)?;
        let verkle_id = covering_id.to_verkle_id();
        let bytes = self.verkle_core.get_contents(verkle_id)?;
        return VerkleNode::from_bytes(bytes);
    }

    pub fn get_message(&mut self, item_id: ItemId) -> Result<&[u8], IsoCoreError> {
        let coverings = coverings_for_item(item_id, WIDTH);
        let leaf_node = self.get_node(coverings.leaf())?;

        if leaf_node.children.len() != 1 || leaf_node.children[0].node_type != NodeType::Leaf {
            return Err(IsoCoreError::NodeFormat);
        }

        let data_id = leaf_node.children[0].index;
        self.data_core.load_message(data_id)?;

        return Ok(self.data_core.get_contents(data_id)?);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isocore_create_and_add_message() {
        let mut isocore = IsoCore::create_mem();
        
        let msg1 = b"hello world";
        let hash1 = isocore.add_message(msg1).unwrap();
        
        assert_eq!(isocore.len().0, 1);
        
        let retrieved = isocore.get_message(ItemId(0)).unwrap();
        assert_eq!(retrieved, msg1);
        
        let hash1_again = isocore.get_root_hash().unwrap();
        assert_eq!(hash1, hash1_again);
    }

    #[test]
    fn isocore_multiple_messages() {
        let mut isocore = IsoCore::create_mem();
        
        let messages = vec![
            b"message 1",
            b"message 2",
            b"message 3",
            b"message 4",
        ];
        
        for msg in &messages {
            isocore.add_message(*msg).unwrap();
        }
        
        assert_eq!(isocore.len().0, 4);
        
        for (i, msg) in messages.iter().enumerate() {
            let retrieved = isocore.get_message(ItemId(i as u64)).unwrap();
            assert_eq!(retrieved, *msg);
        }
    }

    #[test]
    fn isocore_lazy_loading() {
        let mut isocore = IsoCore::create_mem();
        
        isocore.add_message(b"test message").unwrap();
        isocore.add_message(b"another message").unwrap();
        
        let msg = isocore.get_message(ItemId(0)).unwrap();
        assert_eq!(msg, b"test message");
        
        let msg = isocore.get_message(ItemId(1)).unwrap();
        assert_eq!(msg, b"another message");
    }

    #[test]
    fn isocore_empty_len() {
        let isocore = IsoCore::create_mem();
        assert_eq!(isocore.len().0, 0);
    }

    #[test]
    fn verkle_node_serialization() {
        let node = VerkleNode {
            children: vec![
                NodeChild {
                    node_type: NodeType::Leaf,
                    hash: hash(b"test"),
                    index: MessageId(0),
                },
            ],
        };
        
        let bytes = node.to_bytes();
        let parsed = VerkleNode::from_bytes(&bytes).unwrap();
        
        assert_eq!(parsed.children.len(), 1);
        assert_eq!(parsed.children[0].node_type, NodeType::Leaf);
        assert_eq!(parsed.children[0].index, MessageId(0));
    }
}
