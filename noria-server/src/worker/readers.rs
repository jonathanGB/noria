use async_bincode::AsyncBincodeStream;
use dataflow::prelude::DataType;
use dataflow::prelude::*;
use dataflow::KeyRange;
use dataflow::ReaderLookup;
use dataflow::Readers;
use dataflow::SingleReadHandle;
use futures_util::{
    future, future::Either, future::FutureExt, ready, try_future::TryFutureExt,
    try_stream::TryStreamExt,
};
use noria::{ReadQuery, ReadReply, Tagged};
use pin_project::pin_project;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::time;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use stream_cancel::Valve;
use tokio::prelude::*;
use tokio_tower::multiplex::server;
use tower::service_fn;

/// Retry reads every this often.
const RETRY_TIMEOUT_US: u64 = 200;

/// If a blocking reader finds itself waiting this long for a backfill to complete, it will
/// re-issue the replay request. To avoid the system falling over if replays are slow for a little
/// while, waiting readers will use exponential backoff on this delay if they continue to miss.
const TRIGGER_TIMEOUT_US: u64 = 50_000;

thread_local! {
    static READERS: RefCell<HashMap<
        (NodeIndex, usize),
        SingleReadHandle,
    >> = Default::default();
}

pub(super) fn listen(
    valve: &Valve,
    ioh: &tokio_io_pool::Handle,
    on: tokio::net::TcpListener,
    readers: Readers,
) -> impl Future<Output = ()> {
    ioh.spawn_all(
        valve
            .wrap(on.incoming())
            .into_stream()
            .filter_map(|c| {
                // io error from client: just ignore it
                async move { c.ok() }
            })
            .map(Ok)
            .map_ok(move |stream| {
                let readers = readers.clone();
                stream.set_nodelay(true).expect("could not set TCP_NODELAY");
                server::Server::new(
                    AsyncBincodeStream::from(stream).for_async(),
                    service_fn(move |req| handle_message(req, &readers)),
                )
                .map_err(|e| {
                    match e {
                        server::Error::Service(()) => {
                            // server is shutting down -- no need to report this error
                            return;
                        }
                        server::Error::BrokenTransportRecv(ref e)
                        | server::Error::BrokenTransportSend(ref e) => {
                            if let bincode::ErrorKind::Io(ref e) = **e {
                                if e.kind() == std::io::ErrorKind::BrokenPipe
                                    || e.kind() == std::io::ErrorKind::ConnectionReset
                                {
                                    // client went away
                                    return;
                                }
                            }
                        }
                    }
                    eprintln!("!!! reader client protocol error: {:?}", e);
                })
                .map(|_| ())
            }),
    )
    .map_err(|e: tokio_io_pool::StreamSpawnError<()>| {
        eprintln!(
            "io pool is shutting down, so can't handle more reads: {:?}",
            e
        );
    })
    .map(|_| ())
}

fn dup(rs: &[Vec<DataType>]) -> Vec<Vec<DataType>> {
    let mut outer = Vec::with_capacity(rs.len());
    for r in rs {
        let mut inner = Vec::with_capacity(r.len());
        for v in r {
            inner.push(v.deep_clone())
        }
        outer.push(inner);
    }
    outer
}

fn handle_message(
    m: Tagged<ReadQuery>,
    s: &Readers,
) -> impl Future<Output = Result<Tagged<ReadReply>, ()>> + Send {
    let tag = m.tag;
    match m.v {
        ReadQuery::Normal {
            target,
            mut keys,
            block,
        } => {
            let immediate = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                let mut ret = Vec::with_capacity(keys.len());
                ret.resize(keys.len(), Vec::new());

                use nom_sql::Operator;
                let (cols, operators) = reader.get_operators();
                println!("operators: {:?}", operators);
                let found : Vec<_> = if operators.iter().all(|op| *op == Operator::Equal) {
                    keys
                        .into_iter()
                        .map(|key| reader.try_find_and(key, dup))
                        .enumerate()
                        .collect()
                } else if operators.len() == 1 && operators[0] != Operator::Equal { 
                    keys
                        .iter_mut()
                        .map(|key| {
                            let range = match operators[0] {
                                Operator::Greater => (Excluded(vec![key[0].clone()]), Unbounded),
                                Operator::GreaterOrEqual => (Included(vec![key[0].clone()]), Unbounded),
                                Operator::Less => (Unbounded, Excluded(vec![key[0].clone()])),
                                Operator::LessOrEqual => (Unbounded, Included(vec![key[0].clone()])),
                                _ => unimplemented!(),
                            };
                            reader.try_find_range_and(range, dup)
                            // let rs = match reader.try_find_range_and(range, dup) {
                            //     RangeLookup::Ok(res, _) => {
                            //         let flattened_res : Vec<Vec<_>> = res.into_iter().flatten().collect();
                            //         Ok(Some(flattened_res))
                            //     },
                            //     Ok(None) => Ok(None),
                            //     Err(None) => Err(()),
                            //     Err(Some(miss)) => {
                            //         missing_ranges.push(miss);
                            //         Ok(None)
                            //     }
                            // };
                            // (key, rs)
                        })
                        .enumerate()
                        .collect()
                } else if operators.len() > 1 {
                    assert!(operators.iter().is_partitioned(|op| *op == Operator::Equal),
                    "We currently support multi-equalities at first, and then inequalities");
                    keys
                        .iter_mut()
                        .map(|key| {
                            let mut inequality_col = None;
                            let mut equality_keys = vec![];
                            let mut lower_bound = Unbounded;
                            let mut higher_bound = Unbounded;
                            for (i, op) in operators.iter().enumerate() {
                                let keyi = key[i].clone();

                                if *op == Operator::Equal {
                                    equality_keys.push(keyi);
                                    continue;
                                }

                                match inequality_col {
                                    // TODO(jonathangb): check inequality on same col.
                                    Some(_) => {
                                        match op {
                                            Operator::Less => higher_bound = Excluded(keyi),
                                            Operator::LessOrEqual => higher_bound = Included(keyi),
                                            Operator::Greater => lower_bound = Excluded(keyi),
                                            Operator::GreaterOrEqual => lower_bound = Included(keyi),
                                            _ => {},
                                        };
                                    },
                                    None => {
                                        inequality_col = Some(cols[i]);
                                        match op {
                                            Operator::Less => higher_bound = Excluded(keyi),
                                            Operator::LessOrEqual => higher_bound = Included(keyi),
                                            Operator::Greater => lower_bound = Excluded(keyi),
                                            Operator::GreaterOrEqual => lower_bound = Included(keyi),
                                            _ => {},
                                        };
                                    },
                                }
                            }

                            let mut first_bound = equality_keys.clone();
                            let first_bound = match &lower_bound {
                                Included(bound) => {
                                    first_bound.push(bound.clone());
                                    Included(first_bound)
                                },
                                Excluded(bound) => {
                                    first_bound.push(bound.clone());
                                    Excluded(first_bound)
                                },
                                Unbounded => {
                                    let higher_bound = match &higher_bound {
                                        Included(bound) => bound,
                                        Excluded(bound) => bound,
                                        Unbounded => unreachable!(), // We can't have both lower and higher unbounded at once.
                                    };
                                    first_bound.push(common::DataType::min_value(higher_bound));
                                    Included(first_bound)
                                },
                            };
                            let mut second_bound = equality_keys.clone();
                            let second_bound = match higher_bound {
                                Included(bound) => {
                                    second_bound.push(bound);
                                    Included(second_bound)
                                },
                                Excluded(bound) => {
                                    second_bound.push(bound);
                                    Excluded(second_bound)
                                },
                                Unbounded => {
                                    let lower_bound = match lower_bound {
                                        Included(ref bound) => bound,
                                        Excluded(ref bound) => bound,
                                        Unbounded => unreachable!(), // We can't have both lower and higher unbounded at once.
                                    };
                                    second_bound.push(common::DataType::max_value(lower_bound));
                                    Included(second_bound)
                                }
                            };

                            reader.try_find_range_and((first_bound, second_bound), dup)
                            // let rs = reader.try_find_range_and((first_bound, second_bound), dup).map(|r| r.0);
                            // let rs = match rs {
                            //     Ok(Some(res)) => {
                            //         let flattened_res : Vec<Vec<_>> = res.into_iter().flatten().collect();
                            //         Ok(Some(flattened_res))
                            //     }
                            //     Ok(None) => Ok(None),
                            //     Err(None) => Err(()),
                            //     Err(Some(miss)) => {
                            //         missing_ranges.push(miss);
                            //         Ok(None)
                            //     }
                            // };
                            // (key, rs)
                        })
                        .enumerate()
                        .collect()
                } else {
                    unimplemented!()
                };

                let mut ready = true;
                let mut missing_ranges = Vec::new();
                for (i, res) in found {
                    println!("Res: {:?}", res);

                    match res {
                        ReaderLookup::Ok(rs, _) => {
                            // immediate hit!
                            ret[i] = rs.into_iter().flatten().collect();
                        }
                        ReaderLookup::Err => {
                            // map not yet ready
                            ready = false;
                            break;
                        }
                        ReaderLookup::MissPoint(p) => {
                            // Miss a point (during non-range query).
                            missing_ranges.push((true, i, KeyRange::Point(p)));
                        }
                        ReaderLookup::MissRangeSingle(misses) => {
                            // Miss one or many "single" ranges.
                            for (start, end) in misses {
                                missing_ranges.push((true, i, KeyRange::RangeSingle(start, end)));
                            }
                        }
                        ReaderLookup::MissRangeDouble(misses) => {
                            // Miss one or many "double" ranges.
                            for (start, end) in misses {
                                missing_ranges.push((true, i, KeyRange::RangeDouble(start, end)));
                            }
                        }
                        ReaderLookup::MissRangeMany(misses) => {
                            // Miss one or many "many" ranges.
                            for (start, end) in misses {
                                missing_ranges.push((true, i, KeyRange::RangeMany(start, end)));
                            }
                        }
                    }
                }

                if !ready {
                    return Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Err(())),
                    });
                }

                if missing_ranges.is_empty() {
                    // we hit on all the keys!
                    return Ok(Tagged {
                        tag,
                        v: ReadReply::Normal(Ok(ret)),
                    });
                }

                // trigger backfills for all the keys we missed on for later
                for (_, _, missing_range) in &missing_ranges {
                    reader.trigger(missing_range);
                }

                Err((ret, missing_ranges))
            });

            match immediate {
                Ok(reply) => Either::Left(Either::Left(future::ready(Ok(reply)))),
                Err((ret, missing_ranges)) => {
                    if !block {
                        Either::Left(Either::Left(future::ready(Ok(Tagged {
                            tag,
                            v: ReadReply::Normal(Ok(ret)),
                        }))))
                    } else {
                        let trigger = time::Duration::from_micros(TRIGGER_TIMEOUT_US);
                        let retry = time::Duration::from_micros(RETRY_TIMEOUT_US);
                        let now = time::Instant::now();
                        Either::Left(Either::Right(BlockingRead {
                            tag,
                            target,
                            read: ret,
                            truth: s.clone(),
                            retry: async_timer::interval(retry),
                            trigger_timeout: trigger,
                            next_trigger: now,
                            missing_ranges,
                        }))
                    }
                }
            }
        }
        ReadQuery::Size { target } => {
            let size = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let reader = readers_cache.entry(target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(&target).unwrap().clone()
                });

                reader.len()
            });

            Either::Right(future::ready(Ok(Tagged {
                tag,
                v: ReadReply::Size(size),
            })))
        }
    }
}

#[pin_project]
struct BlockingRead {
    tag: u32,
    read: Vec<Vec<Vec<DataType>>>,
    target: (NodeIndex, usize),
    truth: Readers,

    #[pin]
    retry: async_timer::Interval<async_timer::oneshot::Timer>,

    trigger_timeout: time::Duration,
    next_trigger: time::Instant,
    missing_ranges: Vec<(bool, usize, KeyRange)>,
}

impl Future for BlockingRead {
    type Output = Result<Tagged<ReadReply>, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            ready!(this.retry.as_mut().poll_next(cx));

            let missing = READERS.with(|readers_cache| {
                let mut readers_cache = readers_cache.borrow_mut();
                let s = &this.truth;
                let target = &this.target;
                let reader = readers_cache.entry(*this.target).or_insert_with(|| {
                    let readers = s.lock().unwrap();
                    readers.get(target).unwrap().clone()
                });

                let mut triggered = false;
                let mut missing = false;
                let now = time::Instant::now();
                for (still_missing, i, missed_range) in this.missing_ranges.iter_mut() {
                    if !*still_missing {
                        continue;
                    }

                    assert!(missed_range.is_point()); // TODO(jonathangb): handle non-point ranges
                    let key = missed_range.get_ref_key_point().clone();
                    match reader.try_find_and(key, dup) {
                        ReaderLookup::Err => {
                            // map has been deleted, so server is shutting down
                            return Err(());
                        }
                        ReaderLookup::Ok(rs, _) => {
                            this.read[*i] = rs.into_iter().flatten().collect();
                            *still_missing = false;
                        }
                        ReaderLookup::MissPoint(miss) => {
                            if now > *this.next_trigger {
                                if !reader.trigger(&KeyRange::Point(miss)) {
                                    return Err(());
                                }

                                triggered = true;
                            }

                            missing = true;
                        }
                        _ => unimplemented!(),
                    }
                }
                /*for (i, key) in this.keys.iter_mut().enumerate() {
                    if key.is_empty() {
                        // already have this value
                    } else { // TODO(jonathangb): Add range partial.
                        // note that this *does* mean we'll trigger replay multiple times for things
                        // that miss and aren't replayed in time, which is a little sad. but at the
                        // same time, that replay trigger will just be ignored by the target domain.
                        match reader.try_find_and(key, dup).map(|r| r.0) {
                            Ok(Some(rs)) => {
                                this.read[i] = rs;
                                key.clear();
                            }
                            Err(()) => {
                                // map has been deleted, so server is shutting down
                                return Err(());
                            }
                            Ok(None) => {
                                if now > *this.next_trigger {
                                    // maybe the key was filled but then evicted, and we missed it?
                                    if !reader.trigger(key) {
                                        // server is shutting down and won't do the backfill
                                        return Err(());
                                    }
                                    triggered = true;
                                }
                                missing = true;
                            }
                        }
                    }
                }*/

                if triggered {
                    *this.trigger_timeout *= 2;
                    *this.next_trigger = now + *this.trigger_timeout;
                }

                Ok(missing)
            })?;

            if !missing {
                return Poll::Ready(Ok(Tagged {
                    tag: *this.tag,
                    v: ReadReply::Normal(Ok(mem::replace(&mut this.read, Vec::new()))),
                }));
            }
        }
    }
}
