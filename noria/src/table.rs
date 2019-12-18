use crate::channel::CONNECTION_FROM_BASE;
use crate::data::*;
use crate::debug::trace::Tracer;
use crate::internal::*;
use crate::LocalOrNot;
use crate::{Tagged, Tagger};
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use futures_util::{
    future, ready, stream::futures_unordered::FuturesUnordered, try_future::TryFutureExt,
    try_stream::TryStreamExt,
};
use nom_sql::CreateTableStatement;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::{fmt, io};
use tokio::prelude::*;
use tokio_tower::multiplex;
use tower_balance::pool::{self, Pool};
use tower_buffer::Buffer;
use tower_service::Service;
use vec_map::VecMap;

type Transport = AsyncBincodeStream<
    tokio::net::tcp::TcpStream,
    Tagged<()>,
    Tagged<LocalOrNot<Input>>,
    AsyncDestination,
>;

#[derive(Debug)]
#[doc(hidden)]
// only pub because we use it to figure out the error type for TableError
pub struct TableEndpoint(SocketAddr);

impl Service<()> for TableEndpoint {
    type Response = multiplex::MultiplexTransport<Transport, Tagger>;
    type Error = tokio::io::Error;
    // have to repeat types because https://github.com/rust-lang/rust/issues/57807
    type Future = impl Future<
        Output = Result<multiplex::MultiplexTransport<Transport, Tagger>, tokio::io::Error>,
    >;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let f = tokio::net::TcpStream::connect(self.0);
        async move {
            let mut s = f.await?;
            s.set_nodelay(true)?;
            s.write_all(&[CONNECTION_FROM_BASE]).await.unwrap();
            s.flush().await.unwrap();
            let s = AsyncBincodeStream::from(s).for_async();
            Ok(multiplex::MultiplexTransport::new(s, Tagger::default()))
        }
    }
}

pub(crate) type TableRpc = Buffer<
    Pool<
        multiplex::client::Maker<TableEndpoint, Tagged<LocalOrNot<Input>>>,
        (),
        Tagged<LocalOrNot<Input>>,
    >,
    Tagged<LocalOrNot<Input>>,
>;

/// A failed [`SyncTable`] operation.
#[derive(Debug, Fail)]
pub enum TableError {
    /// The wrong number of columns was given when inserting a row.
    #[fail(
        display = "wrong number of columns specified: expected {}, got {}",
        _0, _1
    )]
    WrongColumnCount(usize, usize),

    /// The wrong number of key columns was given when modifying a row.
    #[fail(
        display = "wrong number of key columns used: expected {}, got {}",
        _0, _1
    )]
    WrongKeyColumnCount(usize, usize),

    /// The underlying connection to Noria produced an error.
    #[fail(display = "{}", _0)]
    TransportError(#[cause] failure::Error),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for TableError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        TableError::TransportError(failure::Error::from_boxed_compat(e))
    }
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct Input {
    pub dst: LocalNodeIndex,
    pub data: Vec<TableOperation>,
    pub tracer: Tracer,
}

impl fmt::Debug for Input {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Input")
            .field("dst", &self.dst)
            .field("data", &self.data)
            .field("tracer", &"_")
            .finish()
    }
}

#[doc(hidden)]
#[derive(Clone, Serialize, Deserialize)]
pub struct TableBuilder {
    pub txs: Vec<SocketAddr>,
    pub ni: NodeIndex,
    pub addr: LocalNodeIndex,
    pub key_is_primary: bool,
    pub key: Vec<usize>,
    pub dropped: VecMap<DataType>,

    pub table_name: String,
    pub columns: Vec<String>,
    pub schema: Option<CreateTableStatement>,
}

impl TableBuilder {
    pub(crate) fn build(
        self,
        rpcs: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    ) -> Result<Table, io::Error> {
        let mut addrs = Vec::with_capacity(self.txs.len());
        let mut conns = Vec::with_capacity(self.txs.len());
        for (shardi, &addr) in self.txs.iter().enumerate() {
            use std::collections::hash_map::Entry;

            addrs.push(addr);

            // one entry per shard so that we can send sharded requests in parallel even if
            // they happen to be targeting the same machine.
            let mut rpcs = rpcs.lock().unwrap();
            let s = match rpcs.entry((addr, shardi)) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(h) => {
                    // TODO: maybe always use the same local port?
                    let c = Buffer::new(
                        pool::Builder::new()
                            .urgency(0.01)
                            .loaded_above(0.2)
                            .underutilized_below(0.000000001)
                            .max_services(Some(32))
                            .build(multiplex::client::Maker::new(TableEndpoint(addr)), ()),
                        50,
                    );
                    h.insert(c.clone());
                    c
                }
            };
            conns.push(s);
        }

        let dispatch = tracing::dispatcher::get_default(|d| d.clone());
        Ok(Table {
            ni: self.ni,
            node: self.addr,
            key: self.key,
            key_is_primary: self.key_is_primary,
            columns: self.columns,
            dropped: self.dropped,
            tracer: None,
            table_name: self.table_name,
            schema: self.schema,
            dst_is_local: false,

            shard_addrs: addrs,
            shards: conns,

            dispatch,
        })
    }
}

/// A `Table` is used to perform writes, deletes, and other operations to data in base tables.
///
/// If you create multiple `Table` handles from a single `ControllerHandle`, they may share
/// connections to the Soup workers. For this reason, `Table` is *not* `Send` or `Sync`. To get a
/// handle that can be sent to a different thread (i.e., one with its own dedicated connections),
/// call `Table::into_exclusive`.
#[derive(Clone)]
pub struct Table {
    ni: NodeIndex,
    node: LocalNodeIndex,
    key_is_primary: bool,
    key: Vec<usize>,
    columns: Vec<String>,
    dropped: VecMap<DataType>,
    tracer: Tracer,
    table_name: String,
    schema: Option<CreateTableStatement>,
    dst_is_local: bool,

    shards: Vec<TableRpc>,
    shard_addrs: Vec<SocketAddr>,

    dispatch: tracing::Dispatch,
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Table")
            .field("ni", &self.ni)
            .field("node", &self.node)
            .field("key_is_primary", &self.key_is_primary)
            .field("key", &self.key)
            .field("columns", &self.columns)
            .field("dropped", &self.dropped)
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("dst_is_local", &self.dst_is_local)
            .field("shard_addrs", &self.shard_addrs)
            .finish()
    }
}

impl Service<Input> for Table {
    type Error = TableError;
    type Response = <TableRpc as Service<Tagged<LocalOrNot<Input>>>>::Response;
    // have to repeat types because https://github.com/rust-lang/rust/issues/57807
    type Future = impl Future<Output = Result<Tagged<()>, TableError>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        for s in &mut self.shards {
            ready!(s.poll_ready(cx)).map_err(TableError::from)?;
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut i: Input) -> Self::Future {
        let span = if crate::trace_next_op() {
            Some(tracing::trace_span!(
                "table-request",
                base = self.ni.index()
            ))
        } else {
            None
        };

        i.tracer = self.tracer.take();

        // TODO: check each row's .len() against self.columns.len() -> WrongColumnCount

        if self.shards.len() == 1 {
            let request = Tagged::from(if self.dst_is_local {
                unsafe { LocalOrNot::for_local_transfer(i) }
            } else {
                LocalOrNot::new(i)
            });

            let _guard = span.as_ref().map(tracing::Span::enter);
            tracing::trace!("submit request");
            future::Either::Left(self.shards[0].call(request).map_err(TableError::from))
        } else {
            if self.key.is_empty() {
                unreachable!("sharded base without a key?");
            }
            if self.key.len() != 1 {
                // base sharded by complex key
                unimplemented!();
            }
            let key_col = self.key[0];

            let _guard = span.as_ref().map(tracing::Span::enter);
            tracing::trace!("shard request");
            let mut shard_writes = vec![Vec::new(); self.shards.len()];
            for r in i.data.drain(..) {
                let shard = {
                    let key = match r {
                        TableOperation::Insert(ref r) => &r[key_col],
                        TableOperation::Delete { ref key } => &key[0],
                        TableOperation::Update { ref key, .. } => &key[0],
                        TableOperation::InsertOrUpdate { ref row, .. } => &row[key_col],
                    };
                    crate::shard_by(key, self.shards.len())
                };
                shard_writes[shard].push(r);
            }

            let mut wait_for = FuturesUnordered::new();
            for (s, rs) in shard_writes.drain(..).enumerate() {
                if !rs.is_empty() {
                    let p = if self.dst_is_local {
                        unsafe {
                            LocalOrNot::for_local_transfer(Input {
                                dst: i.dst,
                                tracer: i.tracer.clone(),
                                data: rs,
                            })
                        }
                    } else {
                        LocalOrNot::new(Input {
                            dst: i.dst,
                            tracer: i.tracer.clone(),
                            data: rs,
                        })
                    };
                    let request = Tagged::from(p);

                    // make a span per shard
                    let span = if span.is_some() {
                        Some(tracing::trace_span!("table-shard", s))
                    } else {
                        None
                    };
                    let _guard = span.as_ref().map(tracing::Span::enter);
                    tracing::trace!("submit request shard");

                    wait_for.push(self.shards[s].call(request));
                } else {
                    // poll_ready reserves a sender slot which we have to release
                    // we do that by dropping the old handle and replacing it with a clone
                    // https://github.com/tokio-rs/tokio/issues/898
                    self.shards[s] = self.shards[s].clone()
                }
            }

            future::Either::Right(
                wait_for
                    .try_for_each(|_| async { Ok(()) })
                    .map_err(TableError::from)
                    .map_ok(Tagged::from),
            )
        }
    }
}

impl Service<TableOperation> for Table {
    type Error = TableError;
    type Response = <Table as Service<Input>>::Response;
    type Future = <Table as Service<Input>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Table as Service<Input>>::poll_ready(self, cx)
    }

    fn call(&mut self, op: TableOperation) -> Self::Future {
        let i = self.prep_records(vec![op]);
        <Table as Service<Input>>::call(self, i)
    }
}

impl Service<Vec<TableOperation>> for Table {
    type Error = TableError;
    type Response = <Table as Service<Input>>::Response;
    type Future = <Table as Service<Input>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Table as Service<Input>>::poll_ready(self, cx)
    }

    fn call(&mut self, ops: Vec<TableOperation>) -> Self::Future {
        let i = self.prep_records(ops);
        <Table as Service<Input>>::call(self, i)
    }
}

impl Table {
    /// Get the name of this base table.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[doc(hidden)]
    pub fn i_promise_dst_is_same_process(&mut self) {
        self.dst_is_local = true;
    }

    /// Get the list of columns in this base table.
    ///
    /// Note that this will *not* be updated if the underlying recipe changes and adds or removes
    /// columns!
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get the schema that was used to create this base table.
    ///
    /// Note that this will *not* be updated if the underlying recipe changes and adds or removes
    /// columns!
    pub fn schema(&self) -> Option<&CreateTableStatement> {
        self.schema.as_ref()
    }

    fn inject_dropped_cols(&self, r: &mut TableOperation) {
        use std::mem;
        let ndropped = self.dropped.len();
        if ndropped != 0 {
            // inject defaults for dropped columns
            let dropped = self.dropped.iter().rev();

            // get a handle to the underlying data vector
            let r = match *r {
                TableOperation::Insert(ref mut row)
                | TableOperation::InsertOrUpdate { ref mut row, .. } => row,
                _ => unimplemented!("we need to shift the update/delete cols!"),
            };

            // we want to be a bit careful here to avoid shifting elements multiple times. we
            // do this by moving from the back, and swapping the tail element to the end of the
            // vector until we hit each index.

            // make room in the record
            r.reserve(ndropped);
            let mut free = r.len() + ndropped;
            let mut last_unmoved = r.len() - 1;
            unsafe { r.set_len(free) };
            // *technically* we should set all the extra elements just in case we get a panic
            // below, because otherwise we might call drop() on uninitialized memory. we would
            // do that like this (note the ::forget() on the swapped-out value):
            //
            //   for i in (free - ndropped)..r.len() {
            //       mem::forget(mem::replace(r.get_mut(i).unwrap(), DataType::None));
            //   }
            //
            // but for efficiency we dont' do that, and just make sure that the code below
            // doesn't panic. what's the worst that could happen, right?

            // keep trying to insert the next dropped column
            for (next_insert, default) in dropped {
                // think of this being at the bottom of the loop
                // we just hoist it here to avoid underflow if we ever insert at 0
                free -= 1;

                // shift elements until the next free slot is the one we want to insert into
                while free > next_insert {
                    // shift another element so we the free slot is at a lower index
                    r.swap(last_unmoved, free);
                    free -= 1;

                    if last_unmoved == 0 {
                        // avoid underflow
                        debug_assert_eq!(next_insert, free);
                        break;
                    }
                    last_unmoved -= 1;
                }

                // we're at the right index -- insert the dropped value
                let current = &mut r[next_insert];
                let old = mem::replace(current, default.clone());
                // the old value is uninitialized memory!
                // (remember how we called set_len above?)
                mem::forget(old);

                // here, I'll help:
                // free -= 1;
            }
        }
    }

    fn prep_records(&self, mut ops: Vec<TableOperation>) -> Input {
        for r in &mut ops {
            self.inject_dropped_cols(r);
        }

        Input {
            dst: self.node,
            data: ops,
            tracer: None,
        }
    }

    async fn quick_n_dirty<Request, R>(
        &mut self,
        r: Request,
    ) -> Result<R, <Self as Service<Request>>::Error>
    where
        Request: Send + 'static,
        Self: Service<Request, Response = Tagged<R>>,
    {
        future::poll_fn(|cx| self.poll_ready(cx)).await?;
        Ok(self.call(r).await?.v)
    }

    /// Insert a single row of data into this base table.
    pub async fn insert<V>(&mut self, u: V) -> Result<(), TableError>
    where
        V: Into<Vec<DataType>>,
    {
        self.quick_n_dirty(TableOperation::Insert(u.into())).await
    }

    /// Perform multiple operation on this base table.
    pub async fn perform_all<I, V>(&mut self, i: I) -> Result<(), TableError>
    where
        I: IntoIterator<Item = V>,
        V: Into<TableOperation>,
    {
        self.quick_n_dirty(i.into_iter().map(Into::into).collect::<Vec<_>>())
            .await
    }

    /// Delete the row with the given key from this base table.
    pub async fn delete<I>(&mut self, key: I) -> Result<(), TableError>
    where
        I: Into<Vec<DataType>>,
    {
        self.quick_n_dirty(TableOperation::Delete { key: key.into() })
            .await
    }

    /// Update the row with the given key in this base table.
    ///
    /// `u` is a set of column-modification pairs, where for each pair `(i, m)`, the modification
    /// `m` will be applied to column `i` of the record with key `key`.
    pub async fn update<V>(&mut self, key: Vec<DataType>, u: V) -> Result<(), TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if key.len() != self.key.len() {
            return Err(TableError::WrongKeyColumnCount(self.key.len(), key.len()));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in u {
            if coli >= self.columns.len() {
                return Err(TableError::WrongColumnCount(self.columns.len(), coli + 1));
            }
            set[coli] = m;
        }

        self.quick_n_dirty(TableOperation::Update { key, set })
            .await
    }

    /// Perform a insert-or-update on this base table.
    ///
    /// If a row already exists for the key in `insert`, the existing row will instead be updated
    /// with the modifications in `u` (as documented in `Table::update`).
    pub async fn insert_or_update<V>(
        &mut self,
        insert: Vec<DataType>,
        update: V,
    ) -> Result<(), TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        assert!(
            !self.key.is_empty() && self.key_is_primary,
            "update operations can only be applied to base nodes with key columns"
        );

        if insert.len() != self.columns.len() {
            return Err(TableError::WrongColumnCount(
                self.columns.len(),
                insert.len(),
            ));
        }

        let mut set = vec![Modification::None; self.columns.len()];
        for (coli, m) in update {
            if coli >= self.columns.len() {
                return Err(TableError::WrongColumnCount(self.columns.len(), coli + 1));
            }
            set[coli] = m;
        }

        self.quick_n_dirty(TableOperation::InsertOrUpdate {
            row: insert,
            update: set,
        })
        .await
    }

    /// Trace the next modification to this base table.
    ///
    /// When an input is traced, events are triggered as it flows through the dataflow, and are
    /// communicated back to the controller. Currently, you can only receive trace events if you
    /// build a local controller, specifically by calling `create_debug_channel` before adding any
    /// operators to the dataflow.
    ///
    /// Traced events are sent on the debug channel, and are tagged with the given `tag`.
    pub fn trace_next(&mut self, tag: u64) {
        self.tracer = Some((tag, None));
    }

    /// Switch to a synchronous interface for this table.
    pub fn into_sync(self) -> SyncTable {
        SyncTable(self)
    }
}

/// A synchronous wrapper around [`Table`] where all methods block (using `wait`) for the operation
/// to complete before returning.
#[derive(Clone, Debug)]
pub struct SyncTable(Table);

macro_rules! sync {
    ($self:ident.$method:ident($($args:expr),*)) => {{
        let table = &mut $self.0;
        let tracer = std::mem::replace(&mut table.dispatch, tracing::Dispatch::none());
        let res = tracing::dispatcher::with_default(&tracer, || {
            tokio_executor::current_thread::block_on_all(table.$method($($args),*))
        });
        std::mem::replace(&mut table.dispatch, tracer);
        res
    }};
}

use std::ops::{Deref, DerefMut};
impl Deref for SyncTable {
    type Target = Table;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SyncTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl SyncTable {
    /// See [`Table::insert`].
    pub fn insert<V>(&mut self, u: V) -> Result<(), TableError>
    where
        V: Into<Vec<DataType>>,
    {
        sync!(self.insert(u))
    }

    /// See [`Table::perform_all`].
    pub fn perform_all<I, V>(&mut self, i: I) -> Result<(), TableError>
    where
        I: IntoIterator<Item = V>,
        V: Into<TableOperation>,
    {
        sync!(self.perform_all(i))
    }

    /// See [`Table::delete`].
    pub fn delete<I>(&mut self, key: I) -> Result<(), TableError>
    where
        I: Into<Vec<DataType>>,
    {
        sync!(self.delete(key))
    }

    /// See [`Table::update`].
    pub fn update<V>(&mut self, key: Vec<DataType>, u: V) -> Result<(), TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        sync!(self.update(key, u))
    }

    /// See [`Table::insert_or_update`].
    pub fn insert_or_update<V>(
        &mut self,
        insert: Vec<DataType>,
        update: V,
    ) -> Result<(), TableError>
    where
        V: IntoIterator<Item = (usize, Modification)>,
    {
        sync!(self.insert_or_update(insert, update))
    }

    /// Switch back to an asynchronous interface for this table.
    pub fn into_async(self) -> Table {
        self.0
    }
}
