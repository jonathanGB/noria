use crate::backlog::ReaderLookup;
use common::DataType;
use evmap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

#[derive(Clone)]
pub(super) enum Handle {
    Single(evmap::ReadHandle<DataType, Vec<DataType>, i64>),
    Double(evmap::ReadHandle<(DataType, DataType), Vec<DataType>, i64>),
    Many(evmap::ReadHandle<Vec<DataType>, Vec<DataType>, i64>),
}

impl Handle {
    pub(super) fn len(&self) -> usize {
        match *self {
            Handle::Single(ref h) => h.len(),
            Handle::Double(ref h) => h.len(),
            Handle::Many(ref h) => h.len(),
        }
    }

    pub(super) fn meta_get_and<F, T>(&self, key: Vec<DataType>, then: F) -> ReaderLookup<T>
    where
        F: FnOnce(&[Vec<DataType>]) -> T,
    {
        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                match h.meta_get_and(&key[0], then) {
                    None => ReaderLookup::Err,
                    Some((None, _)) => ReaderLookup::MissPoint(key),
                    Some((Some(res), metadata)) => ReaderLookup::Ok(vec![res], metadata),
                }
                
            }
            Handle::Double(ref h) => {
                assert_eq!(key.len(), 2);
                // we want to transmute &[T; 2] to &(T, T), but that's not actually safe
                // we're not guaranteed that they have the same memory layout
                // we *could* just clone DataType, but that would mean dealing with string refcounts
                // so instead, we play a trick where we memcopy onto the stack and then forget!
                //
                // h/t https://gist.github.com/mitsuhiko/f6478a0dd1ef174b33c63d905babc89a
                use std::mem;
                use std::ptr;
                unsafe {
                    let mut stack_key: (mem::MaybeUninit<DataType>, mem::MaybeUninit<DataType>) =
                        (mem::MaybeUninit::uninit(), mem::MaybeUninit::uninit());
                    ptr::copy_nonoverlapping(
                        &key[0] as *const DataType,
                        stack_key.0.as_mut_ptr(),
                        1,
                    );
                    ptr::copy_nonoverlapping(
                        &key[1] as *const DataType,
                        stack_key.1.as_mut_ptr(),
                        1,
                    );
                    let stack_key = mem::transmute::<_, &(DataType, DataType)>(&stack_key);
                    match h.meta_get_and(&stack_key, then) {
                        None => ReaderLookup::Err,
                        Some((None, _)) => ReaderLookup::MissPoint(key),
                        Some((Some(res), metadata)) => ReaderLookup::Ok(vec![res], metadata),
                    }
                }
            }
            Handle::Many(ref h) => {
                match h.meta_get_and(&key, then) {
                    None => ReaderLookup::Err,
                    Some((None, _)) => ReaderLookup::MissPoint(key),
                    Some((Some(res), metadata)) => ReaderLookup::Ok(vec![res], metadata),
                }
            }
        }
    }

    pub(super) fn meta_get_range_and<F, T, R>(&self, range: R, then: F) -> ReaderLookup<T>
    where
        F: Fn(&[Vec<DataType>]) -> T,
        R: RangeBounds<Vec<DataType>>,
    {
        match *self {
            Handle::Single(ref h) => {
                println!("Single!");
                let start = range.start_bound();
                let end = range.end_bound();

                let start = match start {
                    Included(r) => Included(r[0].to_owned()),
                    Excluded(r) => Excluded(r[0].to_owned()),
                    Unbounded => Unbounded,
                };
                let end = match end {
                    Included(r) => Included(r[0].to_owned()),
                    Excluded(r) => Excluded(r[0].to_owned()),
                    Unbounded => Unbounded,           
                };

                let range = (start, end);

                match h.meta_get_range_and(range, then) {
                    evmap::RangeLookup::Err => ReaderLookup::Err,
                    evmap::RangeLookup::Ok(res, metadata) => ReaderLookup::Ok(res, metadata),
                    evmap::RangeLookup::Miss(miss) => ReaderLookup::MissRangeSingle(miss),
                }
            }
            Handle::Double(ref h) => {
                println!("Double!");
                let start = range.start_bound();
                let end = range.end_bound();

                let start = match start {
                    Included(ref r) => Included((r[0].to_owned(), r[1].to_owned())),
                    Excluded(ref r) => Excluded((r[0].to_owned(), r[1].to_owned())),
                    Unbounded => unreachable!(),
                };

                let end = match end {
                    Included(r) => Included((r[0].to_owned(), r[1].to_owned())),
                    Excluded(r) => Excluded((r[0].to_owned(), r[1].to_owned())),
                    Unbounded => unreachable!(),
                };

                let range = (start, end);
                match h.meta_get_range_and(range, then) {
                    evmap::RangeLookup::Err => ReaderLookup::Err,
                    evmap::RangeLookup::Ok(res, metadata) => ReaderLookup::Ok(res, metadata),
                    evmap::RangeLookup::Miss(miss) => ReaderLookup::MissRangeDouble(miss),
                }
            }
            Handle::Many(ref h) => {
                println!("Many!");
                assert!(range.start_bound() != Unbounded);
                assert!(range.end_bound() != Unbounded);
 
                match h.meta_get_range_and(range, then) {
                    evmap::RangeLookup::Err => ReaderLookup::Err,
                    evmap::RangeLookup::Ok(res, metadata) => ReaderLookup::Ok(res, metadata),
                    evmap::RangeLookup::Miss(miss) => ReaderLookup::MissRangeMany(miss),
                }
            }
        }
    }
}
