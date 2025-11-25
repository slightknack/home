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

use std::path::Path;
use std::path::PathBuf;
use std::io::Write;
use crate::core::MessageId;
use crate::core::CoreError;
use crate::core::Core;
use crate::key::hash;
use crate::key::Hash;
use crate::key::KeyPair;
use crate::key::KeyPub;
use crate::key::Signature;
use crate::covering::children_for_covering;
use crate::covering::coverings_for_item;
use crate::covering::ItemId;
use crate::covering::CoveringId;
use crate::covering::get_peaks;

const WIDTH: u64 = 8;
const INFO_ISOCORE: &'static str = "isocore.info";
const DIR_DATA: &'static str = "data";
const DIR_VERKLE: &'static str = "verkle";
const DIR_SIG: &'static str = "sig";

#[derive(Debug)]
pub enum IsoCoreError {
    Core(CoreError),
    Utf8,
    NodeFormat,
    NodeType,
    HexEncoding,
    MessageIdParse(std::num::ParseIntError),
    IntegrityError,
    SignerMismatch,
    Io(std::io::Error),
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

#[derive(Debug, Clone)]
pub struct SignatureBlock {
    pub global_root: Hash,
    pub signature: Signature,
}

impl SignatureBlock {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.global_root.to_hex());
        out.push(b'\n');
        out.extend_from_slice(&self.signature.0);
        return out;
    }
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
    pub path: Option<PathBuf>,
    pub signer: KeyPub,
    pub data_core: Core,
    pub verkle_core: Core,
    pub sig_core: Core,
}

impl IsoCore {
    pub fn create_mem(signer: &KeyPair) -> Self {
        return Self {
            path: None,
            signer: signer.key_pub.clone(),
            data_core: Core::create_mem(),
            verkle_core: Core::create_mem(),
            sig_core: Core::create_mem(),
        };
    }

    pub fn create(path: PathBuf, signer: &KeyPair) -> Result<Self, IsoCoreError> {
        let data_path = path.join(DIR_DATA);
        let verkle_path = path.join(DIR_VERKLE);
        let sig_path = path.join(DIR_SIG);
        
        // Write isocore.info with public key
        let info_path = path.join(INFO_ISOCORE);
        let mut file = std::fs::File::create(info_path)
            .map_err(|e| IsoCoreError::Io(e))?;
        file.write_all(&signer.key_pub.0)
            .map_err(|e| IsoCoreError::Io(e))?;

        return Ok(Self {
            path: Some(path),
            signer: signer.key_pub.clone(),
            data_core: Core::create(data_path),
            verkle_core: Core::create(verkle_path),
            sig_core: Core::create(sig_path),
        });
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, IsoCoreError> {
        let path = path.as_ref();
        let data_path = path.join(DIR_DATA);
        let verkle_path = path.join(DIR_VERKLE);
        let sig_path = path.join(DIR_SIG);
        
        // Read isocore.info to get public key
        let info_path = path.join(INFO_ISOCORE);
        let pubkey_bytes = std::fs::read(info_path)
            .map_err(|e| IsoCoreError::Io(e))?;
        if pubkey_bytes.len() != 32 {
            return Err(IsoCoreError::NodeFormat);
        }
        let mut pubkey_array = [0u8; 32];
        pubkey_array.copy_from_slice(&pubkey_bytes);

        return Ok(Self {
            path: Some(path.to_path_buf()),
            signer: KeyPub(pubkey_array),
            data_core: Core::load(data_path)?,
            verkle_core: Core::load(verkle_path)?,
            sig_core: Core::load(sig_path)?,
        });
    }

    pub fn add_message(&mut self, message: &[u8], signer: &KeyPair) -> Result<Hash, IsoCoreError> {
        // Verify signer matches IsoCore's public key
        if signer.key_pub != self.signer {
            return Err(IsoCoreError::SignerMismatch);
        }
        
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

        // Bag the peaks: get all peak roots and hash them together
        let current_len = self.len().0 as u64;
        let peaks = get_peaks(current_len, WIDTH);
        
        let mut peak_hashes = Vec::new();
        for peak_id in peaks {
            let peak_node = self.get_node(peak_id)?;
            peak_hashes.push(peak_node.compute_hash());
        }
        
        // Concatenate all peak hashes and hash them to create global root
        let mut global_data = Vec::new();
        for peak_hash in &peak_hashes {
            global_data.extend_from_slice(&peak_hash.0);
        }
        let global_root = hash(&global_data);
        
        // Sign the global root
        let signature = signer.sign(&global_root.0);
        
        // Store signature block
        let sig_block = SignatureBlock {
            global_root: global_root.clone(),
            signature,
        };
        self.sig_core.add_message(&sig_block.to_bytes())?;

        return Ok(global_root);
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
        let expected_hash = leaf_node.children[0].hash.clone();
        
        self.data_core.load_message(data_id)?;
        let data = self.data_core.get_contents(data_id)?;
        
        // Verify data integrity
        let actual_hash = hash(data);
        if actual_hash != expected_hash {
            return Err(IsoCoreError::IntegrityError);
        }

        return Ok(data);
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
        let signer = KeyPair::ephemeral();
        let mut isocore = IsoCore::create_mem(&signer);

        let msg1 = b"hello world";
        let hash1 = isocore.add_message(msg1, &signer).unwrap();

        assert_eq!(isocore.len().0, 1);

        let retrieved = isocore.get_message(ItemId(0)).unwrap();
        assert_eq!(retrieved, msg1);

        // Verify global root is stored in sig_core
        assert_eq!(isocore.sig_core.len().0, 1);
        
        // The global root should be deterministic for the same message
        assert!(!hash1.0.iter().all(|&b| b == 0));
    }

    #[test]
    fn isocore_multiple_messages() {
        let signer = KeyPair::ephemeral();
        let mut isocore = IsoCore::create_mem(&signer);

        let messages = vec![
            b"message 1",
            b"message 2",
            b"message 3",
            b"message 4",
        ];

        for msg in &messages {
            isocore.add_message(*msg, &signer).unwrap();
        }

        assert_eq!(isocore.len().0, 4);

        for (i, msg) in messages.iter().enumerate() {
            let retrieved = isocore.get_message(ItemId(i as u64)).unwrap();
            assert_eq!(retrieved, *msg);
        }
    }

    #[test]
    fn isocore_lazy_loading() {
        let signer = KeyPair::ephemeral();
        let mut isocore = IsoCore::create_mem(&signer);

        isocore.add_message(b"test message", &signer).unwrap();
        isocore.add_message(b"another message", &signer).unwrap();

        let msg = isocore.get_message(ItemId(0)).unwrap();
        assert_eq!(msg, b"test message");

        let msg = isocore.get_message(ItemId(1)).unwrap();
        assert_eq!(msg, b"another message");
    }

    #[test]
    fn isocore_empty_len() {
        let signer = KeyPair::ephemeral();
        let isocore = IsoCore::create_mem(&signer);
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
