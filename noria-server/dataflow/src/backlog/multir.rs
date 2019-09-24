use common::DataType;
use evmap;
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

    pub(super) fn meta_get_and<F, T>(&self, key: &[DataType], then: F) -> Option<(Option<T>, i64)>
    where
        F: FnOnce(&[Vec<DataType>]) -> T,
    {
        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                h.meta_get_and(&key[0], then)
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
                    let v = h.meta_get_and(&stack_key, then);
                    v
                }
            }
            Handle::Many(ref h) => h.meta_get_and(key, then),
        }
    }

    pub(super) fn meta_get_range_and<F, T, R>(&self, range: R, then: F) -> Option<(Option<Vec<T>>, i64)>
    where
        F: Fn(&[Vec<DataType>]) -> T,
        R: RangeBounds<common::DataType>,
    {
        match *self {
            Handle::Single(ref h) => {
                h.meta_get_range_and(range, then)
            },
            // TODO(jonathangb): Consider handling other types of handle.
            _ => unimplemented!(),
        }
    }
}
