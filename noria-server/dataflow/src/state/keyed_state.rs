use fnv::FnvBuildHasher;
use indexmap::IndexMap;
use std::ops::Bound;
use std::rc::Rc;
use std::collections::BTreeMap;

use super::mk_key::MakeKey;
use crate::prelude::*;
use crate::KeyRange;
use common::SizeOf;

#[allow(clippy::type_complexity)]
#[derive(Debug)]
pub(super) enum KeyedState {
    Single(BTreeMap<DataType, Vec<Row>>),
    Double(BTreeMap<(DataType, DataType), Vec<Row>>),
    Tri(BTreeMap<(DataType, DataType, DataType), Vec<Row>>),
    Quad(BTreeMap<(DataType, DataType, DataType, DataType), Vec<Row>>),
    Quin(BTreeMap<(DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
    Sex(BTreeMap<(DataType, DataType, DataType, DataType, DataType, DataType), Vec<Row>>),
}

impl KeyedState {
    pub(super) fn lookup<'a>(&'a self, key: &KeyType) -> Option<RecordResult<'a>> {
        match (self, key) {
            (&KeyedState::Single(ref m), &KeyType::Single(k)) => {
                match m.get(k) {
                    Some(k) => Some(RecordResult::Borrowed(&k[..])),
                    None => None,
                }
            }
            (&KeyedState::Double(ref m), &KeyType::Double(ref k)) => {
                match m.get(k) {
                    Some(k) => Some(RecordResult::Borrowed(&k[..])),
                    None => None,
                }
            }
            (&KeyedState::Tri(ref m), &KeyType::Tri(ref k)) => {
                match m.get(k) {
                    Some(k) => Some(RecordResult::Borrowed(&k[..])),
                    None => None,
                }
            }
            (&KeyedState::Quad(ref m), &KeyType::Quad(ref k)) => {
                match m.get(k) {
                    Some(k) => Some(RecordResult::Borrowed(&k[..])),
                    None => None,
                }
            }
            (&KeyedState::Quin(ref m), &KeyType::Quin(ref k)) => {
                match m.get(k) {
                    Some(k) => Some(RecordResult::Borrowed(&k[..])),
                    None => None,
                }
            }
            (&KeyedState::Sex(ref m), &KeyType::Sex(ref k)) => {
                match m.get(k) {
                    Some(k) => Some(RecordResult::Borrowed(&k[..])),
                    None => None,
                }
            }
            (&KeyedState::Single(ref m), &KeyType::RangeSingle(ref k)) => {
                let rs : Vec<Vec<DataType>> = m.range(k.clone()).flat_map(|(_, row)| row).map(|row| (**row).clone()).collect();
                if rs.is_empty() { None } else { Some(RecordResult::Owned(rs)) }
            }
            (&KeyedState::Double(ref m), &KeyType::RangeDouble(ref k)) => {
                let rs : Vec<Vec<DataType>> = m.range(k.clone()).flat_map(|(_, row)| row).map(|row| (**row).clone()).collect();
                if rs.is_empty() { None } else { Some(RecordResult::Owned(rs)) }
            }
            (&KeyedState::Tri(ref m), &KeyType::RangeTri(ref k)) => {
                let rs : Vec<Vec<DataType>> = m.range(k.clone()).flat_map(|(_, row)| row).map(|row| (**row).clone()).collect();
                if rs.is_empty() { None } else { Some(RecordResult::Owned(rs)) }
            }
            (&KeyedState::Quad(ref m), &KeyType::RangeQuad(ref k)) => {
                let rs : Vec<Vec<DataType>> = m.range(k.clone()).flat_map(|(_, row)| row).map(|row| (**row).clone()).collect();
                if rs.is_empty() { None } else { Some(RecordResult::Owned(rs)) }      
            }
            (&KeyedState::Quin(ref m), &KeyType::RangeQuin(ref k)) => {
                let rs : Vec<Vec<DataType>> = m.range(k.clone()).flat_map(|(_, row)| row).map(|row| (**row).clone()).collect();
                if rs.is_empty() { None } else { Some(RecordResult::Owned(rs)) }
            }
            (&KeyedState::Sex(ref m), &KeyType::RangeSex(ref k)) => {
                let rs : Vec<Vec<DataType>> = m.range(k.clone()).flat_map(|(_, row)| row).map(|row| (**row).clone()).collect();
                if rs.is_empty() { None } else { Some(RecordResult::Owned(rs)) }
            }
            _ => unreachable!(),
        }
    }

    /// Remove all rows for a randomly chosen key seeded by `seed`, returning that key along with
    /// the number of bytes freed. Returns `None` if map is empty.
    pub(super) fn evict_with_seed(&mut self, seed: usize) -> Option<(u64, KeyRange)> {
        // TODO(jonathangb): implement?
        unimplemented!()
        // let (rs, key) = match *self {
        //     KeyedState::Single(ref mut m) => {
        //         let index = seed % m.len();
        //         m.swap_remove_index(index).map(|(k, rs)| (rs, KeyRange::Point(vec![k])))
        //     }
        //     KeyedState::Double(ref mut m) => {
        //         let index = seed % m.len();
        //         m.swap_remove_index(index)
        //             .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1])))
        //     }
        //     KeyedState::Tri(ref mut m) => {
        //         let index = seed % m.len();
        //         m.swap_remove_index(index)
        //             .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2])))
        //     }
        //     KeyedState::Quad(ref mut m) => {
        //         let index = seed % m.len();
        //         m.swap_remove_index(index)
        //             .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2, k.3])))
        //     }
        //     KeyedState::Quin(ref mut m) => {
        //         let index = seed % m.len();
        //         m.swap_remove_index(index)
        //             .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2, k.3, k.4])))
        //     }
        //     KeyedState::Sex(ref mut m) => {
        //         let index = seed % m.len();
        //         m.swap_remove_index(index)
        //             .map(|(k, rs)| (rs, KeyRange::Point(vec![k.0, k.1, k.2, k.3, k.4, k.5])))
        //     }
        // }?;
        // Some((
        //     rs.iter()
        //         .filter(|r| Rc::strong_count(&r.0) == 1)
        //         .map(SizeOf::deep_size_of)
        //         .sum(),
        //     key,
        // ))
    }

    /// Remove all rows for the given key, returning the number of bytes freed.
    pub(super) fn evict(&mut self, key: &KeyRange) -> u64 {
        // TODO(jonathangb): implement?
        unimplemented!()

        // match *self {
        //     KeyedState::Single(ref mut m) => m.swap_remove(&(key.get_ref_key_point()[0])),
        //     KeyedState::Double(ref mut m) => {
        //         m.swap_remove::<(DataType, _)>(&MakeKey::from_key(key.get_ref_key_point()))
        //     }
        //     KeyedState::Tri(ref mut m) => {
        //         m.swap_remove::<(DataType, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
        //     }
        //     KeyedState::Quad(ref mut m) => {
        //         m.swap_remove::<(DataType, _, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
        //     }
        //     KeyedState::Quin(ref mut m) => {
        //         m.swap_remove::<(DataType, _, _, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
        //     }
        //     KeyedState::Sex(ref mut m) => {
        //         m.swap_remove::<(DataType, _, _, _, _, _)>(&MakeKey::from_key(key.get_ref_key_point()))
        //     }
        // }
        // .map(|rows| {
        //     rows.iter()
        //         .filter(|r| Rc::strong_count(&r.0) == 1)
        //         .map(SizeOf::deep_size_of)
        //         .sum()
        // })
        // .unwrap_or(0)
    }
}

impl<'a> Into<KeyedState> for &'a [usize] {
    fn into(self) -> KeyedState {
        match self.len() {
            0 => unreachable!(),
            1 => KeyedState::Single(BTreeMap::default()),
            2 => KeyedState::Double(BTreeMap::default()),
            3 => KeyedState::Tri(BTreeMap::default()),
            4 => KeyedState::Quad(BTreeMap::default()),
            5 => KeyedState::Quin(BTreeMap::default()),
            6 => KeyedState::Sex(BTreeMap::default()),
            x => panic!("invalid compound key of length: {}", x),
        }
    }
}
