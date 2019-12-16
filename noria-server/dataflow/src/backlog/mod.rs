use crate::prelude::*;
use common::{ColumnIdentifier, SizeOf};
use rand::prelude::*;
use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;
use std::ops::RangeBounds;

/// Allocate a new end-user facing result table.
pub(crate) fn new(cols: usize, key: &[usize]) -> (SingleReadHandle, WriteHandle) {
    new_inner(cols, key, None)
}

/// Allocate a new partially materialized end-user facing result table.
///
/// Misses in this table will call `trigger` to populate the entry, and retry until successful.
pub(crate) fn new_partial<F>(
    cols: usize,
    key: &[usize],
    trigger: F,
) -> (SingleReadHandle, WriteHandle)
where
    F: Fn(&KeyRange) -> bool + 'static + Send + Sync,
{
    new_inner(cols, key, Some(Arc::new(trigger)))
}

fn new_inner(
    cols: usize,
    key: &[usize],
    trigger: Option<Arc<dyn Fn(&KeyRange) -> bool + Send + Sync>>,
) -> (SingleReadHandle, WriteHandle) {
    let contiguous = {
        let mut contiguous = true;
        let mut last = None;
        for &k in key {
            if let Some(last) = last {
                if k != last + 1 {
                    contiguous = false;
                    break;
                }
            }
            last = Some(k);
        }
        contiguous
    };

    macro_rules! make {
        ($variant:tt) => {{
            use evmap;

            let mut evmap_options = evmap::Options::default().with_meta(-1);
            // By default, the evmap ignores the interval tree (which is only
            // relevant for range-queries in a partially-materialized scenario).
            // Hence, if we detect a trigger, which implies that we have a
            // partially-materialized reader, we must set the constructor of the
            // evmap to not ignore the underlying interval tree. 
            if trigger.is_some() {
                evmap_options.set_ignore_interval_tree(false);
            }
            let (r, w) = evmap_options.construct();

            (multir::Handle::$variant(r), multiw::Handle::$variant(w))
        }};
    }

    let mut key = Vec::from(key);
    key.dedup();
    let (r, w) = match key.len() {
        0 => unreachable!(),
        1 => make!(Single),
        2 => make!(Double),
        _ => make!(Many),
    };

    let w = WriteHandle {
        partial: trigger.is_some(),
        handle: w,
        key: key.clone(),
        cols,
        contiguous,
        mem_size: 0,
    };
    let r = SingleReadHandle {
        handle: r,
        trigger,
        key: key.clone(),
        operators: vec![],
    };

    (r, w)
}

mod multir;
mod multiw;

fn key_to_single(k: Key) -> Cow<DataType> {
    assert!(k.is_point());
    assert_eq!(k.get_ref_key_point().len(), 1);

    match k {
        Cow::Owned(mut k) => Cow::Owned(k.get_mut_key_point().swap_remove(0)),
        Cow::Borrowed(k) => Cow::Borrowed(&k.get_ref_key_point()[0]),
    }
}

fn key_to_double(k: Key) -> Cow<(DataType, DataType)> {
    assert!(k.is_point());
    assert_eq!(k.get_ref_key_point().len(), 2);

    match k {
        Cow::Owned(k) => {
            let mut k = k.get_key_point().into_iter();
            let k1 = k.next().unwrap();
            let k2 = k.next().unwrap();
            Cow::Owned((k1, k2))
        }
        Cow::Borrowed(k) => Cow::Owned((k.get_ref_key_point()[0].clone(),
                                        k.get_ref_key_point()[1].clone())),
    }
}

pub(crate) struct WriteHandle {
    handle: multiw::Handle,
    partial: bool,
    cols: usize,
    key: Vec<usize>,
    contiguous: bool,
    mem_size: usize,
}

// TODO(jonathangb): ConcreteKey necessary?
type ConcreteKey<'a> = Cow<'a, [DataType]>;
type Key<'a> = Cow<'a, KeyRange>;
pub(crate) struct MutWriteHandleEntry<'a> {
    handle: &'a mut WriteHandle,
    key: Key<'a>,
}
pub(crate) struct WriteHandleEntry<'a> {
    handle: &'a WriteHandle,
    key: Key<'a>,
}

impl<'a> MutWriteHandleEntry<'a> {
    pub(crate) fn mark_filled(self) {
        if self.key.is_point() {
            if self
                .handle
                .handle
                .meta_get_and(Cow::Borrowed(&*self.key), |rs| rs.is_empty())
                .is_miss()
            {
                self.handle.handle.clear(self.key)
            } else {
                unreachable!("attempted to fill already-filled key")
            } 
        } else {
            // TODO(jonathangb): clear with range?
            // if self
            //     .handle
            //     .handle
            //     .meta_get_range_and(Cow::Borrowed(&*self.key), |rs| rs.is_empty())
            //     .is_miss()
            // {
            //     self.handle.handle.clear(self.key)
            // } else {
            //     unreachable!("attempted to fill already-filled key")
            // }
        }


    }

    pub(crate) fn mark_hole(self) {
        if !self.key.is_point() {
            unimplemented!("Mark filled for ranges not implemented...")
        }

        let results = self.handle
                            .handle
                            .meta_get_and(Cow::Borrowed(&*self.key), |rs| {
                                rs.iter().map(SizeOf::deep_size_of).sum::<u64>()
                            });
        let size = if let ReaderLookup::Ok(sizes, _) = results { 
            assert_eq!(sizes.len(), 1);
            sizes[0] 
        } else { 0 };

        self.handle.mem_size = self.handle.mem_size.checked_sub(size as usize).unwrap();
        self.handle.handle.empty(self.key)
    }
}

impl<'a> WriteHandleEntry<'a> {
    pub(crate) fn try_find_and<F, T>(self, mut then: F) -> ReaderLookup<T>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        self.handle.handle.meta_get_and(self.key, &mut then)
    }

    pub(crate) fn try_find_range_and<F, T>(self, then: F) -> ReaderLookup<T>
    where
        F: Fn(&[Vec<DataType>]) -> T,
        {
            self.handle.handle.meta_get_range_and(self.key, &then)
        }
}

fn key_from_record<'a, R>(key: &[usize], contiguous: bool, record: R) -> Key<'a> // TODO(jonathangb): concrete key return?
where
    R: Into<Cow<'a, [DataType]>>,
{
    match record.into() {
        Cow::Owned(mut record) => {
            let mut i = 0;
            let mut keep = key.iter().peekable();
            record.retain(|_| {
                i += 1;
                if let Some(&&next) = keep.peek() {
                    if next != i - 1 {
                        return false;
                    }
                } else {
                    return false;
                }

                assert_eq!(*keep.next().unwrap(), i - 1);
                true
            });
            Cow::Owned(KeyRange::Point(record))
        }
        Cow::Borrowed(record) if contiguous => Cow::Owned(KeyRange::Point(record[key[0]..(key[0] + key.len())].to_vec())),
        Cow::Borrowed(record) => Cow::Owned(KeyRange::Point(key.iter().map(|&i| &record[i]).cloned().collect())),
    }
}

impl WriteHandle {
    pub(crate) fn mut_with_key<'a, K>(&'a mut self, key: K) -> MutWriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        MutWriteHandleEntry {
            handle: self,
            key: key.into(),
        }
    }

    pub(crate) fn with_key<'a, K>(&'a self, key: K) -> WriteHandleEntry<'a>
    where
        K: Into<Key<'a>>,
    {
        WriteHandleEntry {
            handle: self,
            key: key.into(),
        }
    }

    #[allow(dead_code)]
    fn mut_entry_from_record<'a, R>(&'a mut self, record: R) -> MutWriteHandleEntry<'a>
    where
        R: Into<Cow<'a, [DataType]>>,
    {
        let key = key_from_record(&self.key[..], self.contiguous, record);
        self.mut_with_key(key)
    }

    pub(crate) fn entry_from_record<'a, R>(&'a self, record: R) -> WriteHandleEntry<'a>
    where
        R: Into<Cow<'a, [DataType]>>,
    {
        let key = key_from_record(&self.key[..], self.contiguous, record);
        self.with_key(key)
    }

    pub(crate) fn swap(&mut self) {
        self.handle.refresh();
    }

    /// Add a new set of records to the backlog.
    ///
    /// These will be made visible to readers after the next call to `swap()`.
    pub(crate) fn add<I>(&mut self, rs: I)
    where
        I: IntoIterator<Item = Record>,
    {
        let mem_delta = self.handle.add(&self.key[..], self.cols, rs);
        if mem_delta > 0 {
            self.mem_size += mem_delta as usize;
        } else if mem_delta < 0 {
            self.mem_size = self
                .mem_size
                .checked_sub(mem_delta.checked_abs().unwrap() as usize)
                .unwrap();
        }
    }

    pub(crate) fn add_range<I>(&mut self, rs: I, ranges: HashSet<KeyRange>)
    where
        I: IntoIterator<Item = Record>,
    {
        let mem_delta = self.handle.add_range(&self.key[..], self.cols, rs, ranges);
        if mem_delta > 0 {
            self.mem_size += mem_delta as usize;
        } else if mem_delta < 0 {
            self.mem_size = self
                .mem_size
                .checked_sub(mem_delta.checked_abs().unwrap() as usize)
                .unwrap();
        }
    }

    pub(crate) fn is_partial(&self) -> bool {
        self.partial
    }

    /// Evict `count` randomly selected keys from state and return them along with the number of
    /// bytes that will be freed once the underlying `evmap` applies the operation.
    pub(crate) fn evict_random_key(&mut self, rng: &mut ThreadRng) -> u64 {
        let mut bytes_to_be_freed = 0;
        if self.mem_size > 0 {
            if self.handle.is_empty() {
                unreachable!("mem size is {}, but map is empty", self.mem_size);
            }

            // TODO(jonathangb): Find an alternative to `empty_at_index`.
            // match self.handle.empty_at_index(rng.gen()) {
            //     None => (),
            //     Some(vs) => {
            //         let size: u64 = vs.iter().map(|r| r.deep_size_of() as u64).sum();
            //         bytes_to_be_freed += size;
            //     }
            // }
            self.mem_size = self
                .mem_size
                .checked_sub(bytes_to_be_freed as usize)
                .unwrap();
        }
        bytes_to_be_freed
    }
}

impl SizeOf for WriteHandle {
    fn size_of(&self) -> u64 {
        use std::mem::size_of;

        size_of::<Self>() as u64
    }

    fn deep_size_of(&self) -> u64 {
        self.mem_size as u64
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum KeyRange {
    Point(Vec<DataType>),
    RangeSingle(Bound<DataType>, Bound<DataType>),
    RangeDouble(Bound<(DataType, DataType)>, Bound<(DataType, DataType)>),
    RangeMany(Bound<Vec<DataType>>, Bound<Vec<DataType>>),
}

impl KeyRange {
    pub fn is_point(&self) -> bool {
        match self {
            Self::Point(_) => true,
            _ => false,
        }
    }

    pub fn is_single(&self) -> bool {
        match self {
            Self::RangeSingle(..) => true,
            _ => false,
        }
    }

    pub fn is_double(&self) -> bool {
        match self {
            Self::RangeDouble(..) => true,
            _ => false,
        }
    }

    pub fn is_many(&self) -> bool {
        match self {
            Self::RangeMany(..) => true,
            _ => false,
        }
    }

    pub fn to_key_type<'a>(&'a self) -> common::KeyType<'a> {
        use std::ops::Bound::{Included, Excluded, Unbounded};

        match self {
            Self::Point(range) => {
                match range.len() {
                    1 => common::KeyType::Single(&range[0]),
                    2 => common::KeyType::Double((range[0].clone(), range[1].clone())),
                    3 => common::KeyType::Tri((range[0].clone(), range[1].clone(), range[2].clone())),
                    4 => common::KeyType::Quad((range[0].clone(), range[1].clone(), range[2].clone(), range[3].clone())),
                    5 => common::KeyType::Quin((range[0].clone(), range[1].clone(), range[2].clone(), range[3].clone(), range[4].clone())),
                    6 => common::KeyType::Sex((range[0].clone(), range[1].clone(), range[2].clone(), range[3].clone(), range[4].clone(), range[5].clone())),
                    _ => unreachable!(),
                }
            }
            Self::RangeSingle(start, end) => common::KeyType::RangeSingle((start.clone(), end.clone())),
            Self::RangeDouble(start, end) => common::KeyType::RangeDouble((start.clone(), end.clone())),
            Self::RangeMany(start, end) => {
                let len = match start {
                    Included(start) => Some(start.len()),
                    Excluded(start) => Some(start.len()),  
                    Unbounded => None, 
                };

                // Make sure end bound corresponds to the length of the start bound.
                match end {
                    Included(end) => assert_eq!(len.unwrap(), end.len()),
                    Excluded(end) => assert_eq!(len.unwrap(), end.len()),
                    Unbounded if len.is_none() => unreachable!("Both start and end bounds can't be Unbounded"),
                    Unbounded => {}
                };

                match len.unwrap() {
                    3 => {
                        let start = match start {
                            Included(start) => Included((start[0].clone(), start[1].clone(), start[2].clone())),
                            Excluded(start) => Excluded((start[0].clone(), start[1].clone(), start[2].clone())),
                            Unbounded => Unbounded,
                        };
                        let end = match end {
                            Included(end) => Included((end[0].clone(), end[1].clone(), end[2].clone())),
                            Excluded(end) => Excluded((end[0].clone(), end[1].clone(), end[2].clone())),
                            Unbounded => Unbounded,
                        };
                        common::KeyType::RangeTri((start, end))
                    }
                    4 => {
                        let start = match start {
                            Included(start) => Included((start[0].clone(), start[1].clone(), start[2].clone(), start[3].clone())),
                            Excluded(start) => Excluded((start[0].clone(), start[1].clone(), start[2].clone(), start[3].clone())),
                            Unbounded => Unbounded,
                        };
                        let end = match end {
                            Included(end) => Included((end[0].clone(), end[1].clone(), end[2].clone(), end[3].clone())),
                            Excluded(end) => Excluded((end[0].clone(), end[1].clone(), end[2].clone(), end[3].clone())),
                            Unbounded => Unbounded,
                        };
                        common::KeyType::RangeQuad((start, end))
                    }
                    5 => {
                        let start = match start {
                            Included(start) => Included((start[0].clone(), start[1].clone(), start[2].clone(), start[3].clone(), start[4].clone())),
                            Excluded(start) => Excluded((start[0].clone(), start[1].clone(), start[2].clone(), start[3].clone(), start[4].clone())),
                            Unbounded => Unbounded,
                        };
                        let end = match end {
                            Included(end) => Included((end[0].clone(), end[1].clone(), end[2].clone(), end[3].clone(), end[4].clone())),
                            Excluded(end) => Excluded((end[0].clone(), end[1].clone(), end[2].clone(), end[3].clone(), end[4].clone())),
                            Unbounded => Unbounded,
                        };
                        common::KeyType::RangeQuin((start, end))
                    }
                    6 => {
                        let start = match start {
                            Included(start) => Included((start[0].clone(), start[1].clone(), start[2].clone(), start[3].clone(), start[4].clone(), start[5].clone())),
                            Excluded(start) => Excluded((start[0].clone(), start[1].clone(), start[2].clone(), start[3].clone(), start[4].clone(), start[5].clone())),
                            Unbounded => Unbounded,
                        };
                        let end = match end {
                            Included(end) => Included((end[0].clone(), end[1].clone(), end[2].clone(), end[3].clone(), end[4].clone(), end[5].clone())),
                            Excluded(end) => Excluded((end[0].clone(), end[1].clone(), end[2].clone(), end[3].clone(), end[4].clone(), end[5].clone())),
                            Unbounded => Unbounded,
                        };
                        common::KeyType::RangeSex((start, end))
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    pub fn get_ref_key_point(&self) -> &Vec<DataType> {
        match self {
            Self::Point(ref key) => key,
            _ => unreachable!("Must be a point"),
        }
    }

    pub fn get_mut_key_point(&mut self) -> &mut Vec<DataType> {
        match self {
            Self::Point(ref mut key) => key,
            _ => unreachable!("Must be a point"),
        }
    }

    pub fn get_key_point(self) -> Vec<DataType> {
        match self {
            Self::Point(key) => key,
            _ => unreachable!("Must be a point"),
        }
    }

    pub fn get_single_key(&self) -> Option<&DataType> {
        match self {
            Self::Point(ref key) => {
                if key.len() == 1 { Some(&key[0]) } else { None }
            }
            _ => None,
        }
    }

    pub fn get_vec_range(self) -> (Bound<Vec<DataType>>, Bound<Vec<DataType>>) {
        use std::ops::Bound::*;

        match self {
            Self::Point(key) => (Included(key.clone()), Included(key)),
            Self::RangeSingle(start, end) => {
                let start = match start {
                    Included(start) => Included(vec![start]),
                    Excluded(start) => Excluded(vec![start]),
                    Unbounded => Unbounded, 
                };
                let end = match end {
                    Included(end) => Included(vec![end]),
                    Excluded(end) => Excluded(vec![end]),
                    Unbounded => Unbounded,
                };
                (start, end)
            }
            Self::RangeDouble(start, end) => {
                let start = match start {
                    Included(start) => Included(vec![start.0, start.1]),
                    Excluded(start) => Excluded(vec![start.0, start.1]),
                    Unbounded => Unbounded,
                };
                let end = match end {
                    Included(end) => Included(vec![end.0, end.1]),
                    Excluded(end) => Excluded(vec![end.0, end.1]),
                    Unbounded => Unbounded,
                };
                (start, end)
            }
            Self::RangeMany(start, end) => (start, end)
        }
    }
}

/// Contains the result of an equality or range lookup to the reader node.
//#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Hash)]
#[derive(Debug, Eq, PartialEq)]
pub enum ReaderLookup<T> {
    Err,
    MissPoint(Vec<DataType>),
    MissRangeSingle(Vec<(Bound<DataType>, Bound<DataType>)>),
    MissRangeDouble(Vec<(Bound<(DataType, DataType)>, Bound<(DataType, DataType)>)>),
    MissRangeMany(Vec<(Bound<Vec<DataType>>, Bound<Vec<DataType>>)>),
    Ok(Vec<T>, i64),
}

impl<T> ReaderLookup<T> {
    pub fn is_miss(&self) -> bool {
        match self {
            Self::MissPoint(_) |
            Self::MissRangeSingle(_) |
            Self::MissRangeDouble(_) |
            Self::MissRangeMany(_) => true,
            _ => false,
        }
    }

    pub fn is_ok(&self) -> bool {
        match self {
            Self::Ok(..) => true,
            _ => false,
        }
    }

    pub fn unwrap(self) -> (Vec<T>, i64) {
        match self {
            Self::Ok(rs, meta) => (rs, meta),
            _ => unreachable!("unwrap must be called on an `Ok` type"),
        }
    }
}

/// Handle to get the state of a single shard of a reader.
#[derive(Clone)]
pub struct SingleReadHandle {
    handle: multir::Handle,
    trigger: Option<Arc<dyn Fn(&KeyRange) -> bool + Send + Sync>>,
    key: Vec<usize>,
    operators: Vec<(ColumnIdentifier, nom_sql::Operator)>,
}

impl SingleReadHandle {
    /// Trigger a replay of a missing key from a partially materialized view.
    pub fn trigger(&self, key: &KeyRange) -> bool {
        assert!(
            self.trigger.is_some(),
            "tried to trigger a replay for a fully materialized view"
        );

        // trigger a replay to populate
        (*self.trigger.as_ref().unwrap())(key)
    }

    pub fn get_operators(&self) -> (&[(ColumnIdentifier, nom_sql::Operator)]) {
        &self.operators
    }

    pub fn set_operators(&mut self, operators: &[(ColumnIdentifier, nom_sql::Operator)]) {
        println!("set_operators called");
        self.operators = operators.to_vec();
    }

    /// Find all entries that matched the given conditions.
    ///
    /// Returned records are passed to `then` before being returned.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// swapped in by the writer.
    ///
    /// Holes in partially materialized state are returned as `Ok((None, _))`.
    pub fn try_find_and<F, T>(&self, key: &[DataType], mut then: F) -> ReaderLookup<T>
    where
        F: FnMut(&[Vec<DataType>]) -> T,
    {
        self.handle.meta_get_and(key, &mut then)
    }

    pub fn try_find_range_and<F, T, R>(&self, range: R, then: F) -> ReaderLookup<T>
    where
        F: Fn(&[Vec<DataType>]) -> T,
        R: RangeBounds<Vec<DataType>>,
        {
            self.handle.meta_get_range_and(range, &then)
        }

    pub fn len(&self) -> usize {
        self.handle.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handle.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO(jonathangb): Fix all broken tests.
    #[test]
    fn store_works() {
        let a = vec![1.into(), "a".into()];

        let (r, mut w) = new(2, &[0]);

        // initially, store is uninitialized
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), ReaderLookup::Err);

        w.swap();

        // after first swap, it is empty, but ready
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), ReaderLookup::Ok(vec![0], -1));

        w.add(vec![Record::Positive(a.clone())]);

        // it is empty even after an add (we haven't swapped yet)
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()), ReaderLookup::Ok(vec![0], -1));

        w.swap();

        // but after the swap, the record is there!
        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![1]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
    }

    #[test]
    fn busybusybusy() {
        use std::thread;

        let n = 1_000;
        let (r, mut w) = new(1, &[0]);
        thread::spawn(move || {
            for i in 0..n {
                w.add(vec![Record::Positive(vec![i.into()])]);
                w.swap();
            }
        });

        for i in 0..n {
            let i = &[i.into()];
            loop {
                match r.try_find_and(i, |rs| rs.len()) {
                    ReaderLookup::MissPoint(_) => continue,
                    ReaderLookup::Ok(rs, _) if rs.first() == Some(&1) => break,
                    ReaderLookup::Ok(rs, _) => assert_ne!(rs.first(), Some(&1)),
                    ReaderLookup::Err => continue,
                    _ => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn minimal_query() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.swap();
        w.add(vec![Record::Positive(b.clone())]);

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![1]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
    }

    #[test]
    fn non_minimal_query() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let c = vec![1.into(), "c".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Positive(c.clone())]);

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![2]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
    }

    #[test]
    fn absorb_negative_immediate() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![1]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
    }

    #[test]
    fn absorb_negative_later() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![Record::Positive(a.clone())]);
        w.add(vec![Record::Positive(b.clone())]);
        w.swap();
        w.add(vec![Record::Negative(a.clone())]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![1]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
    }

    #[test]
    fn absorb_multi() {
        let a = vec![1.into(), "a".into()];
        let b = vec![1.into(), "b".into()];
        let c = vec![1.into(), "c".into()];

        let (r, mut w) = new(2, &[0]);
        w.add(vec![
            Record::Positive(a.clone()),
            Record::Positive(b.clone()),
        ]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![2]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == a[0] && r[1] == a[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .first()
            .unwrap());

        w.add(vec![
            Record::Negative(a.clone()),
            Record::Positive(c.clone()),
            Record::Negative(c.clone()),
        ]);
        w.swap();

        assert_eq!(r.try_find_and(&a[0..1], |rs| rs.len()).unwrap().0, vec![1]);
        assert!(r
            .try_find_and(&a[0..1], |rs| rs
                .iter()
                .any(|r| r[0] == b[0] && r[1] == b[1]))
            .unwrap()
            .0
            .first()
            .unwrap());
    }
}
