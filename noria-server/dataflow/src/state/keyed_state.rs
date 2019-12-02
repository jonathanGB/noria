use fnv::FnvBuildHasher;
use indexmap::IndexMap;
use std::ops::Bound;
use std::rc::Rc;

use super::mk_key::MakeKey;
use crate::prelude::*;
use crate::KeyRange;
use common::SizeOf;

type FnvHashMap<K, V> = IndexMap<K, V, FnvBuildHasher>;

#[allow(clippy::type_complexity)]
pub(super) enum KeyedState {
    Single(FnvHashMap<DataType, Vec<Row>>),
    Double(FnvHashMap<(DataType, DataType), Vec<Row>>),
    Tri(FnvHashMap<(DataType, DataType, DataType), Vec<Row>>),
    Quad(FnvHashMap<(DataType, DataType, DataType, DataType), Vec<Row>>),
    Quin(FnvHashMap<(DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
    Sex(FnvHashMap<(DataType, DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
    RangeSingle(FnvHashMap<(Bound<DataType>, Bound<DataType>), Vec<Row>>),
    RangeDouble(FnvHashMap<(Bound<(DataType, DataType)>, Bound<(DataType, DataType)>), Vec<Row>>),
    RangeMany(FnvHashMap<(Bound<Vec<DataType>>, Bound<Vec<DataType>>), Vec<Row>>),
}

impl KeyedState {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<&'a Vec<Row>> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => m.get(k),
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => m.get(k),
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => m.get(k),
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => m.get(k),
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => m.get(k),
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => m.get(k),
            (&KeyedState::RangeSingle(ref m), &KeyType::RangeSingle(ref k)) => m.get(k),
            (&KeyedState::RangeDouble(ref m), &KeyType::RangeDouble(ref k)) => m.get(k),
            (&KeyedState::RangeMany(ref m), &KeyType::RangeMany(ref k)) => m.get(k),
            _ => unreachable!(),
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, KeyRange)> {
        let (rs, key) = match *self {
            KeyedState::Single(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index).map(|(k, rs)| (rs, KeyRange::Point(vec![k])))
            }
            KeyedState::Double(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1])))
            }
            KeyedState::Tri(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2])))
            }
            KeyedState::Quad(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2, k.3])))
            }
            KeyedState::Quin(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2, k.3, k.4])))
            }
            KeyedState::Sex(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2, k.3, k.4, k.5])))
            }
            KeyedState::RangeSingle(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|((start, end), rs)| (rs, KeyRange::RangeSingle(start, end)))
            }
            KeyedState::RangeDouble(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|((start, end), rs)| (rs, KeyRange::RangeDouble(start, end)))
            }
            KeyedState::RangeMany(ref mut m) => {
                let index = seed % m.len();
                m.swap_remove_index(index)
                    .map(|((start, end), rs)| (rs, KeyRange::RangeMany(start, end)))
            }
        }?;
        Some((
            rs.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum(),
            key,
        ))
    }

    /// Remove all rows for the given key, returning the number of bytes freed.
    pub(super) fn evict(&mut self, key: &KeyRange) -> u64 {
        match *self {
            KeyedState::Single(ref mut m) => m.swap_remove(&(key.get_ref_key_point()[0])),
            KeyedState::Double(ref mut m) => {
                m.swap_remove::<(DataType, _)>(&MakeKey::from_key(key.get_ref_key_point()))
            }
            KeyedState::Tri(ref mut m) => {
                m.swap_remove::<(DataType, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
            }
            KeyedState::Quad(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
            }
            KeyedState::Quin(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
            }
            KeyedState::Sex(ref mut m) => {
                m.swap_remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
            }
            KeyedState::RangeSingle(ref mut m) => {
                if let KeyRange::RangeSingle(start, end) = key {
                    m.swap_remove(&(start.clone(), end.clone()))
                } else {
                    unreachable!()
                }
            }
            KeyedState::RangeDouble(ref mut m) => {
                if let KeyRange::RangeDouble(start, end) = key {
                    m.swap_remove(&(start.clone(), end.clone()))
                } else {
                    unreachable!()
                }
            }
            KeyedState::RangeMany(ref mut m) => {
                if let KeyRange::RangeMany(start, end) = key {
                    m.swap_remove(&(start.clone(), end.clone()))
                } else {
                    unreachable!()
                }
            }
        }
        .map(|rows| {
            rows.iter()
                .filter(|r| Rc::strong_count(&r.0) == 1)
                .map(SizeOf::deep_size_of)
                .sum()
        })
        .unwrap_or(0)
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(FnvHashMap::default()),
            2 => KeyedState::Double(FnvHashMap::default()),
            3 => KeyedState::Tri(FnvHashMap::default()),
            4 => KeyedState::Quad(FnvHashMap::default()),
            5 => KeyedState::Quin(FnvHashMap::default()),
            6 => KeyedState::Sex(FnvHashMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
