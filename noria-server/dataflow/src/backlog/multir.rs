use backlog::RangeLookupMiss;
use common::DataType;
use evmap;
use unbounded_interval_tree::IntervalTree;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;

#[derive(Clone)]
pub(super) enum Handle {
    Single(evmap::ReadHandle<DataType, Vec<DataType>, i64>, IntervalTree<DataType>),
    Double(evmap::ReadHandle<(DataType, DataType), Vec<DataType>, i64>, IntervalTree<(DataType, DataType)>),
    Many(evmap::ReadHandle<Vec<DataType>, Vec<DataType>, i64>, IntervalTree<Vec<DataType>>),
}

impl Handle {
    pub(super) fn len(&self) -> usize {
        match *self {
            Handle::Single(ref h, _) => h.len(),
            Handle::Double(ref h, _) => h.len(),
            Handle::Many(ref h, _) => h.len(),
        }
    }

    pub(super) fn meta_get_and<F, T>(&self, key: &[DataType], then: F) -> Option<(Option<T>, i64)>
    where
        F: FnOnce(&[Vec<DataType>]) -> T,
    {
        match *self {
            Handle::Single(ref h, _) => {
                assert_eq!(key.len(), 1);
                h.meta_get_and(&key[0], then)
            }
            Handle::Double(ref h, _) => {
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
                    let v = h.meta_get_and(&stack_key, then);
                    v
                }
            }
            Handle::Many(ref h, _) => h.meta_get_and(key, then),
        }
    }

    pub(super) fn meta_get_range_and<F, T, R>(&self, range: R, then: F) -> Result<(Option<Vec<T>>, i64), Option<RangeLookupMiss>>
    where
        F: Fn(&[Vec<DataType>]) -> T,
        R: RangeBounds<Vec<DataType>>,
    {

        match *self {
            Handle::Single(ref h, ref t) => {
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
                let diff = t.get_interval_difference(range.clone());
                println!("diff: {:?}", diff);
                if diff.is_empty() {
                    match h.meta_get_range_and(range, then) {
                        Some(res) => Ok(res),
                        None => Err(None),
                    }
                } else {
                    Err(Some(RangeLookupMiss::Single(diff)))
                }
            },
            Handle::Double(ref h, ref t) => {
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
                let diff = t.get_interval_difference(range.clone());
                println!("diff: {:?}", diff);
                if diff.is_empty() {
                    match h.meta_get_range_and(range, then) {
                        Some(res) => Ok(res),
                        None => Err(None),
                    }
                } else {
                    Err(Some(RangeLookupMiss::Double(diff)))
                }
            },
            Handle::Many(ref h, ref t) => {
                println!("Many!");
                assert!(range.start_bound() != Unbounded);
                assert!(range.end_bound() != Unbounded);

                let range = (range.start_bound().cloned(), range.end_bound().cloned());
                let diff = t.get_interval_difference(range.clone());
                println!("diff: {:?}", diff);
                if diff.is_empty() {
                    match h.meta_get_range_and(range, then) {
                        Some(res) => Ok(res),
                        None => Err(None),
                    }
                } else {
                    Err(Some(RangeLookupMiss::Many(diff)))
                }
            },
        }
    }
}
