// K07 local patch (see PATCHES.md): owned return deadline, gated release,
// raw-before-size close ownership.
use std::fmt::{self, Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures_core::future::BoxFuture;

use crate::sync::AsyncSemaphoreReleaser;

use crate::connection::Connection;
use crate::database::Database;
use crate::error::Error;

use super::inner::{is_beyond_max_lifetime, DecrementSizeGuard, PoolInner};
use crate::pool::options::PoolConnectionMetadata;
use std::future::Future;

const CLOSE_ON_DROP_TIMEOUT: Duration = Duration::from_secs(5);

/// K07: absolute budget of the owned future that returns a dropped connection to the pool,
/// covering `after_release`, the release ping, a graceful close and the `min_connections`
/// tail that this return starts. When it expires the connection is dropped, never re-pooled.
pub(super) const RETURN_TO_POOL_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection managed by a [`Pool`][crate::pool::Pool].
///
/// Will be returned to the pool on-drop.
pub struct PoolConnection<DB: Database> {
    live: Option<Live<DB>>,
    close_on_drop: bool,
    pub(crate) pool: Arc<PoolInner<DB>>,
}

pub(super) struct Live<DB: Database> {
    pub(super) raw: DB::Connection,
    pub(super) created_at: Instant,
}

pub(super) struct Idle<DB: Database> {
    pub(super) live: Live<DB>,
    pub(super) idle_since: Instant,
}

/// RAII wrapper for connections being handled by functions that may drop them
pub(super) struct Floating<DB: Database, C> {
    pub(super) inner: C,
    pub(super) guard: DecrementSizeGuard<DB>,
}

const EXPECT_MSG: &str = "BUG: inner connection already taken!";

impl<DB: Database> Debug for PoolConnection<DB> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO: Show the type name of the connection ?
        f.debug_struct("PoolConnection").finish()
    }
}

impl<DB: Database> Deref for PoolConnection<DB> {
    type Target = DB::Connection;

    fn deref(&self) -> &Self::Target {
        &self.live.as_ref().expect(EXPECT_MSG).raw
    }
}

impl<DB: Database> DerefMut for PoolConnection<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live.as_mut().expect(EXPECT_MSG).raw
    }
}

impl<DB: Database> AsRef<DB::Connection> for PoolConnection<DB> {
    fn as_ref(&self) -> &DB::Connection {
        self
    }
}

impl<DB: Database> AsMut<DB::Connection> for PoolConnection<DB> {
    fn as_mut(&mut self) -> &mut DB::Connection {
        self
    }
}

impl<DB: Database> PoolConnection<DB> {
    /// Close this connection, allowing the pool to open a replacement.
    ///
    /// Equivalent to calling [`.detach()`] then [`.close()`], but the connection permit is retained
    /// for the duration so that the pool may not exceed `max_connections`.
    ///
    /// [`.detach()`]: PoolConnection::detach
    /// [`.close()`]: Connection::close
    pub async fn close(mut self) -> Result<(), Error> {
        let floating = self.take_live().float(self.pool.clone());
        floating.inner.raw.close().await
    }

    /// Close this connection on-drop, instead of returning it to the pool.
    ///
    /// May be used in cases where waiting for the [`.close()`][Self::close] call
    /// to complete is unacceptable, but you still want the connection to be closed gracefully
    /// so that the server can clean up resources.
    #[inline(always)]
    pub fn close_on_drop(&mut self) {
        self.close_on_drop = true;
    }

    /// Detach this connection from the pool, allowing it to open a replacement.
    ///
    /// Note that if your application uses a single shared pool, this
    /// effectively lets the application exceed the [`max_connections`] setting.
    ///
    /// If [`min_connections`] is nonzero, a task will be spawned to replace this connection.
    ///
    /// If you want the pool to treat this connection as permanently checked-out,
    /// use [`.leak()`][Self::leak] instead.
    ///
    /// [`max_connections`]: crate::pool::PoolOptions::max_connections
    /// [`min_connections`]: crate::pool::PoolOptions::min_connections
    pub fn detach(mut self) -> DB::Connection {
        self.take_live().float(self.pool.clone()).detach()
    }

    /// Detach this connection from the pool, treating it as permanently checked-out.
    ///
    /// This effectively will reduce the maximum capacity of the pool by 1 every time it is used.
    ///
    /// If you don't want to impact the pool's capacity, use [`.detach()`][Self::detach] instead.
    pub fn leak(mut self) -> DB::Connection {
        self.take_live().raw
    }

    fn take_live(&mut self) -> Live<DB> {
        self.live.take().expect(EXPECT_MSG)
    }

    /// Test the connection to make sure it is still live before returning it to the pool.
    ///
    /// This effectively runs the drop handler eagerly instead of spawning a task to do it.
    #[doc(hidden)]
    pub fn return_to_pool(&mut self) -> impl Future<Output = ()> + Send + 'static {
        // float the connection in the pool before we move into the task
        // in case the returned `Future` isn't executed, like if it's spawned into a dying runtime
        // https://github.com/launchbadge/sqlx/issues/1396
        // Type hints seem to be broken by `Option` combinators in IntelliJ Rust right now (6/22).
        let floating: Option<Floating<DB, Live<DB>>> =
            self.live.take().map(|live| live.float(self.pool.clone()));

        let pool = self.pool.clone();

        // K07: the deadline is fixed when the owned future is created, not per await.
        let deadline = Instant::now() + RETURN_TO_POOL_TIMEOUT;

        owned_until(deadline, async move {
            let returned_to_pool = if let Some(floating) = floating {
                floating.return_to_pool().await
            } else {
                false
            };

            if !returned_to_pool {
                pool.min_connections_maintenance(Some(deadline)).await;
            }
        })
    }

    fn take_and_close(&mut self) -> impl Future<Output = ()> + Send + 'static {
        // float the connection in the pool before we move into the task
        // in case the returned `Future` isn't executed, like if it's spawned into a dying runtime
        // https://github.com/launchbadge/sqlx/issues/1396
        // Type hints seem to be broken by `Option` combinators in IntelliJ Rust right now (6/22).
        let floating = self.live.take().map(|live| live.float(self.pool.clone()));

        let pool = self.pool.clone();

        // K07: the graceful close and the maintenance tail share one budget.
        let deadline = Instant::now() + CLOSE_ON_DROP_TIMEOUT;

        owned_until(deadline, async move {
            if let Some(floating) = floating {
                // Don't hold the connection forever if it hangs while trying to close
                floating.close().await;
            }

            pool.min_connections_maintenance(Some(deadline)).await;
        })
    }
}

/// K07: run `fut` until `deadline`. On expiry, or if `deadline` has already passed at the first
/// poll, `fut` is dropped together with every connection and size guard it owns; nothing is
/// detached or handed to another task.
async fn owned_until<F: Future<Output = ()>>(deadline: Instant, fut: F) {
    match deadline.checked_duration_since(Instant::now()) {
        Some(remaining) if !remaining.is_zero() => {
            let _ = crate::rt::timeout(remaining, fut).await;
        }
        _ => drop(fut),
    }
}

/// K07: a driver `close()` future owned together with the size guard of the connection it
/// consumes. Field order drops the close future (and the raw connection inside it) before the
/// guard publishes the size decrement, whether the close completes, times out or is cancelled.
pub(super) struct OwnedClose<DB: Database> {
    close: BoxFuture<'static, Result<(), Error>>,
    _guard: DecrementSizeGuard<DB>,
}

impl<DB: Database> OwnedClose<DB> {
    pub(super) fn new(raw: DB::Connection, guard: DecrementSizeGuard<DB>) -> Self {
        Self {
            close: raw.close(),
            _guard: guard,
        }
    }
}

impl<DB: Database> Future for OwnedClose<DB> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.close.as_mut().poll(cx) {
            Poll::Ready(Err(error)) => {
                tracing::debug!(%error, "error occurred while closing the pool connection");
                Poll::Ready(())
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'c, DB: Database> crate::acquire::Acquire<'c> for &'c mut PoolConnection<DB> {
    type Database = DB;

    type Connection = &'c mut <DB as Database>::Connection;

    #[inline]
    fn acquire(self) -> futures_core::future::BoxFuture<'c, Result<Self::Connection, Error>> {
        Box::pin(futures_util::future::ok(&mut **self))
    }

    #[inline]
    fn begin(
        self,
    ) -> futures_core::future::BoxFuture<'c, Result<crate::transaction::Transaction<'c, DB>, Error>>
    {
        crate::transaction::Transaction::begin(&mut **self, None)
    }
}

/// Returns the connection to the [`Pool`][crate::pool::Pool] it was checked-out from.
impl<DB: Database> Drop for PoolConnection<DB> {
    fn drop(&mut self) {
        if self.close_on_drop {
            crate::rt::spawn(self.take_and_close());
            return;
        }

        // We still need to spawn a task to maintain `min_connections`.
        if self.live.is_some() || self.pool.options.min_connections > 0 {
            crate::rt::spawn(self.return_to_pool());
        }
    }
}

impl<DB: Database> Live<DB> {
    pub fn float(self, pool: Arc<PoolInner<DB>>) -> Floating<DB, Self> {
        Floating {
            inner: self,
            // create a new guard from a previously leaked permit
            guard: DecrementSizeGuard::new_permit(pool),
        }
    }

    pub fn into_idle(self) -> Idle<DB> {
        Idle {
            live: self,
            idle_since: Instant::now(),
        }
    }
}

impl<DB: Database> Deref for Idle<DB> {
    type Target = Live<DB>;

    fn deref(&self) -> &Self::Target {
        &self.live
    }
}

impl<DB: Database> DerefMut for Idle<DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live
    }
}

impl<DB: Database> Floating<DB, Live<DB>> {
    pub fn new_live(conn: DB::Connection, guard: DecrementSizeGuard<DB>) -> Self {
        Self {
            inner: Live {
                raw: conn,
                created_at: Instant::now(),
            },
            guard,
        }
    }

    pub fn reattach(self) -> PoolConnection<DB> {
        let Floating { inner, guard } = self;

        let pool = Arc::clone(&guard.pool);

        guard.cancel();
        PoolConnection {
            live: Some(inner),
            close_on_drop: false,
            pool,
        }
    }

    /// K07: returns the connection back when the pool is already closed.
    pub fn release(self) -> Result<(), Self> {
        self.guard.pool.clone().try_release(self)
    }

    /// Return the connection to the pool.
    ///
    /// Returns `true` if the connection was successfully returned, `false` if it was closed.
    async fn return_to_pool(mut self) -> bool {
        // Immediately close the connection.
        if self.guard.pool.is_closed() {
            self.close().await;
            return false;
        }

        // If the connection is beyond max lifetime, close the connection and
        // immediately create a new connection
        if is_beyond_max_lifetime(&self.inner, &self.guard.pool.options) {
            self.close().await;
            return false;
        }

        if let Some(test) = &self.guard.pool.options.after_release {
            let meta = self.metadata();
            match (test)(&mut self.inner.raw, meta).await {
                Ok(true) => (),
                Ok(false) => {
                    self.close().await;
                    return false;
                }
                Err(error) => {
                    tracing::warn!(%error, "error from `after_release`");
                    // Connection is broken, don't try to gracefully close as
                    // something weird might happen.
                    // K07: `close_hard()` can still await the transport, so drop it instead.
                    drop(self);
                    return false;
                }
            }
        }

        // test the connection on-release to ensure it is still viable,
        // and flush anything time-sensitive like transaction rollbacks
        // if an Executor future/stream is dropped during an `.await` call, the connection
        // is likely to be left in an inconsistent state, in which case it should not be
        // returned to the pool; also of course, if it was dropped due to an error
        // this is simply a band-aid as SQLx-next connections should be able
        // to recover from cancellations
        if let Err(error) = self.raw.ping().await {
            tracing::warn!(
                %error,
                "error occurred while testing the connection on-release",
            );

            // Connection is broken, don't try to gracefully close.
            // K07: `close_hard()` can still await the transport, so drop it instead.
            drop(self);
            false
        } else {
            // if the connection is still viable, release it to the pool
            match self.release() {
                Ok(()) => true,
                // K07: the pool closed while this connection was being tested.
                Err(rejected) => {
                    rejected.close().await;
                    false
                }
            }
        }
    }

    pub async fn close(self) {
        // This isn't used anywhere that we care about the return value
        let Floating { inner, guard } = self;

        // K07: the raw connection is gone before `guard` is dropped, even on cancellation.
        OwnedClose::new(inner.raw, guard).await;
    }

    pub fn detach(self) -> DB::Connection {
        self.inner.raw
    }

    pub fn into_idle(self) -> Floating<DB, Idle<DB>> {
        Floating {
            inner: self.inner.into_idle(),
            guard: self.guard,
        }
    }

    pub fn metadata(&self) -> PoolConnectionMetadata {
        PoolConnectionMetadata {
            age: self.created_at.elapsed(),
            idle_for: Duration::ZERO,
        }
    }
}

impl<DB: Database> Floating<DB, Idle<DB>> {
    pub fn from_idle(
        idle: Idle<DB>,
        pool: Arc<PoolInner<DB>>,
        permit: AsyncSemaphoreReleaser<'_>,
    ) -> Self {
        Self {
            inner: idle,
            guard: DecrementSizeGuard::from_permit(pool, permit),
        }
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        self.live.raw.ping().await
    }

    pub fn into_live(self) -> Floating<DB, Live<DB>> {
        Floating {
            inner: self.inner.live,
            guard: self.guard,
        }
    }

    pub async fn close(self) -> DecrementSizeGuard<DB> {
        if let Err(error) = self.inner.live.raw.close().await {
            tracing::debug!(%error, "error occurred while closing the pool connection");
        }
        self.guard
    }

    pub async fn close_hard(self) -> DecrementSizeGuard<DB> {
        let _ = self.inner.live.raw.close_hard().await;

        self.guard
    }

    pub fn metadata(&self) -> PoolConnectionMetadata {
        // Use a single `now` value for consistency.
        let now = Instant::now();

        PoolConnectionMetadata {
            // NOTE: the receiver is the later `Instant` and the arg is the earlier
            // https://github.com/launchbadge/sqlx/issues/1912
            age: now.saturating_duration_since(self.created_at),
            idle_for: now.saturating_duration_since(self.idle_since),
        }
    }
}

impl<DB: Database, C> Deref for Floating<DB, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<DB: Database, C> DerefMut for Floating<DB, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
