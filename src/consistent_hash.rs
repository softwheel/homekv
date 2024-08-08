use md5::{Digest, Md5};
use std::collections::{BTreeMap, HashMap};

pub type HashFunction = fn(&[u8]) -> Vec<u8>;

pub trait ConsistentHashNode {
    fn name(&self) -> &str;
    fn is_valid(&self) -> bool;
}

pub struct ConsistentHash<N>
where
    N: ConsistentHashNode,
{
    virtual_nodes: BTreeMap<Vec<u8>, String>,
    node_replicas: HashMap<String, (N, usize)>,
    hash_func: HashFunction,
}

impl<N> ConsistentHash<N>
where
    N: ConsistentHashNode,
{
    pub fn new(node_replicas: Vec<(N, usize)>) -> Self {
        let ch = Self {
            virtual_nodes: BTreeMap::new(),
            node_replicas: HashMap::new(),
            hash_func: md5_hash,
        };
        ch.init(node_replicas)
    }

    pub fn new_with_hash_func(node_replicas: Vec<(N, usize)>, hash_func: HashFunction) -> Self {
        let ch = Self {
            virtual_nodes: BTreeMap::new(),
            node_replicas: HashMap::new(),
            hash_func,
        };
        ch.init(node_replicas)
    }

    fn init(mut self, node_replicas: Vec<(N, usize)>) -> Self {
        node_replicas.into_iter().for_each(|(node, n_replicas)| {
            self.add(node, n_replicas);
        });
        self
    }

    fn get_vnode_key(&self, node_name: &str, i: usize) -> Vec<u8> {
        let virtual_node_id = format!("{}-{}", node_name, i);
        (self.hash_func)(virtual_node_id.as_bytes())
    }

    pub fn add(&mut self, node: N, n_replicas: usize) {
        self.remove(node.name());
        let node_name = node.name();
        for i in 0..n_replicas {
            let vnode_key = self.get_vnode_key(node_name, i);
            self.virtual_nodes.insert(vnode_key, node_name.to_string());
        }
        self.node_replicas
            .insert(node_name.to_string(), (node, n_replicas));
    }

    pub fn remove(&mut self, node_name: &str) -> Option<(N, usize)> {
        if let Some((node, n_replicas)) = self.node_replicas.remove(node_name) {
            for i in 0..n_replicas {
                let vnode_key = self.get_vnode_key(node_name, i);
                self.virtual_nodes.remove(vnode_key.as_slice());
            }
            Some((node, n_replicas))
        } else {
            None
        }
    }

    pub fn nodes(&self) -> Vec<&N> {
        self.node_replicas
            .values()
            .map(|(node, _)| node)
            .collect::<Vec<_>>()
    }

    pub fn nodes_mut(&mut self) -> Vec<&mut N> {
        self.node_replicas
            .values_mut()
            .map(|(node, _)| node)
            .collect::<Vec<_>>()
    }

    pub fn get(&self, key: &[u8]) -> Option<&N> {
        self.get_with_tolerance(key, 0)
    }

    pub fn get_with_tolerance(&self, key: &[u8], tolerance: usize) -> Option<&N> {
        self.get_position_key(key, tolerance)
            .and_then(move |position_key| {
                self.virtual_nodes
                    .get(&position_key)
                    .map(|node_name| &(self.node_replicas.get(node_name).unwrap().0))
            })
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut N> {
        self.get_mut_with_tolerance(key, 0)
    }

    pub fn get_mut_with_tolerance(&mut self, key: &[u8], tolerance: usize) -> Option<&mut N> {
        self.get_position_key(key, tolerance)
            .and_then(move |position_key| {
                if let Some(node_name) = self.virtual_nodes.get(&position_key) {
                    Some(&mut (self.node_replicas.get_mut(node_name).unwrap().0))
                } else {
                    None
                }
            })
    }

    fn get_position_key(&self, key: &[u8], tolerance: usize) -> Option<Vec<u8>> {
        if self.node_replicas.is_empty() {
            return None;
        }

        let mut tolerance = if tolerance >= self.virtual_nodes.len() {
            self.virtual_nodes.len() - 1
        } else {
            tolerance
        };
        let hashed_key = (self.hash_func)(key);
        for (position_key, node_name) in self
            .virtual_nodes
            .range(hashed_key..)
            .chain(self.virtual_nodes.iter())
        {
            if let Some((node, _)) = self.node_replicas.get(node_name) {
                if node.is_valid() {
                    return Some(position_key.clone());
                }
            }
            if tolerance == 0 {
                return None;
            } else {
                tolerance -= 1;
            }
        }

        None
    }
}

pub fn md5_hash(data: &[u8]) -> Vec<u8> {
    let mut digest = Md5::default();
    digest.update(data);
    digest.finalize().to_vec()
}
