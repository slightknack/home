use std::path::PathBuf;

use crate::core::Core;
use crate::key::Hash;
use crate::key::KeyPub;
use crate::key::KeyPair;
use crate::key::hash;

pub enum Owner {
    Writer(KeyPair),
    Reader(KeyPub),
}

impl Owner {
    pub fn emphemeral() -> Self {
        Owner::Writer(KeyPair::ephemeral())
    }
}

pub struct IsoCore {
    pub owner: Owner,
    pub core: Core,
    pub merkle_tree: Verkle,
}

impl IsoCore {
    pub fn create_mem(owner: Owner) -> Self {
        let core = Core::create_mem();
        let merkle_tree = Verkle::new();
        Self {
            owner,
            core,
            merkle_tree,
        }
    }

    pub fn create(owner: Owner, path: PathBuf) -> Self {
        let core = Core::create(path);
        let merkle_tree = Verkle::new();
        Self {
            owner,
            core,
            merkle_tree,
        }
    }
}

pub struct Verkle {
    pub root: Hash,
    pub nodes: BTreeMap<Hash, Node>,
    pub subtrees: Vec<NodeId>,
    pub size: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct NodeId(u16);

pub enum Node {
    Leaf(Hash),
    Branch(Hash, Vec<NodeId>),
}

impl Verkle {
    pub const MAX_SPLAY: usize = 8;

    pub fn new() -> Self {
        Verkle {
            root: hash(&[]),
            nodes: Vec::new(),
            subtrees: Vec::new(),
            size: 0,
        }
    }

    pub fn as_root_node(&self) -> Node {
        Node::Branch(self.root.clone(), self.subtrees.clone())
    }

    /// Adds a message to the Verkle Tree, and returns the updated root hash
    pub fn add_message(&mut self, hash: Hash) -> Hash {

    }
}
