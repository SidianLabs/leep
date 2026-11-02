//! Serving connections. `axum::serve` builds hyper's connection builder
//! without a timer, so hyper's header-read timeout is never armed, and it has
//! no idle timeout, no bound on a stalled body and no connection cap. This
//! module serves the router with all four.
//!
//! - A connection must deliver a complete request head within the header
//!   timeout of opening, and (HTTP/1.1) of finishing its previous request.
//! - A connection with no request in progress for the idle timeout is closed
//!   gracefully (HTTP/2 `GOAWAY`; HTTP/1.1 is already closed by the header
//!   timeout when that is shorter).
//! - A request body that delivers nothing for the body idle timeout fails;
//!   a response body the client reads nothing of for that long is dropped,
//!   which releases the permits it holds (ADR 0020, ADR 0060), and a socket
//!   whose writes make no progress for that long is closed.
//! - At most `max_connections` connections are served at once; further ones
//!   wait in the listen backlog.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::ConnectInfo;
use axum::http::Request;
use axum::Router;
use http_body::{Frame, SizeHint};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::time::{sleep_until, Instant, Sleep};
use tower::ServiceExt;
use tower_http::timeout::TimeoutBody;

/// Default time a client has to send a complete request head.
pub const DEFAULT_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Default time a connection may sit with no request in progress.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// Default time a request or response body may make no progress.
pub const DEFAULT_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Default cap on connections served at once.
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// HTTP/2 keep-alive: a ping this often, and the connection closes when one
/// is not answered within [`H2_PING_TIMEOUT`].
const H2_PING_INTERVAL: Duration = Duration::from_secs(20);
const H2_PING_TIMEOUT: Duration = Duration::from_secs(20);

/// Responses of at most this many bytes, of known length, fit in the
/// buffers between server and client, so they are not watched.
const UNWATCHED_BODY_BYTES: u64 = 64 * 1024;

/// How connections are served. `None` turns a bound off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServeOptions {
    pub header_read_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub body_idle_timeout: Option<Duration>,
    pub max_connections: Option<usize>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        ServeOptions {
            header_read_timeout: Some(DEFAULT_HEADER_READ_TIMEOUT),
            idle_timeout: Some(DEFAULT_IDLE_TIMEOUT),
            body_idle_timeout: Some(DEFAULT_BODY_IDLE_TIMEOUT),
            max_connections: Some(DEFAULT_MAX_CONNECTIONS),
        }
    }
}

/// Serve `app` on `listener` with `opts` until `shutdown` resolves, then
/// stop accepting, let connections finish their requests, and return.
pub async fn serve_with(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
    opts: ServeOptions,
) -> io::Result<()> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(opts.header_read_timeout)
        .keep_alive(true);
    builder
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(Some(H2_PING_INTERVAL))
        .keep_alive_timeout(H2_PING_TIMEOUT);
    let builder = Arc::new(builder);
    let slots = opts.max_connections.map(|n| Arc::new(Semaphore::new(n)));
    let (stop_tx, stop_rx) = watch::channel(false);
    let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let slot = match &slots {
            Some(s) => tokio::select! {
                p = s.clone().acquire_owned() => Some(p.expect("the semaphore is never closed")),
                _ = &mut shutdown => break,
            },
            None => None,
        };
        let (stream, addr) = tokio::select! {
            r = listener.accept() => match r {
                Ok(c) => c,
                Err(e) => {
                    accept_failed(e).await;
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };
        let _ = stream.set_nodelay(true);
        let conn = Connection {
            builder: builder.clone(),
            app: app.clone(),
            addr,
            opts,
            stop: stop_rx.clone(),
            _done: done_tx.clone(),
        };
        tokio::spawn(async move {
            let _slot = slot;
            conn.run(stream).await;
        });
    }
    drop(listener);
    let _ = stop_tx.send(true);
    drop(done_tx);
    // Every connection task holds a sender; `None` once they have all ended.
    let _ = done_rx.recv().await;
    Ok(())
}

/// After a failed `accept`: errors about one connection are skipped; others
/// (out of file descriptors) are waited out briefly, as `axum::serve` does.
async fn accept_failed(e: io::Error) {
    use io::ErrorKind::*;
    if matches!(
        e.kind(),
        ConnectionRefused | ConnectionAborted | ConnectionReset
    ) {
        return;
    }
    tracing::error!(error = %e, "accepting a connection failed");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

struct Connection {
    builder: Arc<auto::Builder<TokioExecutor>>,
    app: Router,
    addr: SocketAddr,
    opts: ServeOptions,
    stop: watch::Receiver<bool>,
    _done: mpsc::Sender<()>,
}

/// Requests of one connection, for its idle and first-request deadlines.
#[derive(Clone, Copy, Debug)]
struct Activity {
    in_flight: usize,
    started: u64,
    /// When `in_flight` last fell to zero (or the connection opened).
    idle_since: Instant,
}

/// Held by a request from its start until its response body is dropped.
struct InFlight(watch::Sender<Activity>);

impl InFlight {
    fn enter(tx: &watch::Sender<Activity>) -> InFlight {
        tx.send_modify(|a| {
            a.in_flight += 1;
            a.started += 1;
        });
        InFlight(tx.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.send_modify(|a| {
            a.in_flight -= 1;
            if a.in_flight == 0 {
                a.idle_since = Instant::now();
            }
        });
    }
}

impl Connection {
    async fn run(self, stream: TcpStream) {
        let Connection {
            builder,
            app,
            addr,
            opts,
            mut stop,
            _done,
        } = self;
        let (activity, mut watch_activity) = watch::channel(Activity {
            in_flight: 0,
            started: 0,
            idle_since: Instant::now(),
        });
        let body_idle = opts.body_idle_timeout;
        let service = hyper::service::service_fn(move |req: Request<Incoming>| {
            let app = app.clone();
            let held = InFlight::enter(&activity);
            async move {
                let mut req = match body_idle {
                    Some(t) => req.map(|b| Body::new(TimeoutBody::new(t, b))),
                    None => req.map(Body::new),
                };
                // The peer address keys anonymous request limits (ADR 0020).
                req.extensions_mut().insert(ConnectInfo(addr));
                let resp = match app.oneshot(req).await {
                    Ok(r) => r,
                    Err(e) => match e {},
                };
                Ok::<_, Infallible>(resp.map(|b| ServeBody::new(b, held, body_idle)))
            }
        });
        let io = TokioIo::new(StallIo::new(stream, body_idle));
        let conn = builder.serve_connection(io, service);
        let mut conn = std::pin::pin!(conn);
        let mut closing = false;
        loop {
            tokio::select! {
                r = conn.as_mut() => {
                    if let Err(e) = r {
                        tracing::debug!(peer = %addr, error = %e, "connection ended");
                    }
                    return;
                }
                _ = stop.wait_for(|s| *s), if !closing => {
                    closing = true;
                    conn.as_mut().graceful_shutdown();
                }
                first = idle_deadline(&mut watch_activity, opts), if !closing => {
                    if first {
                        // No request head in time: nothing to answer.
                        tracing::debug!(peer = %addr, "closing a connection that sent no request in time");
                        return;
                    }
                    closing = true;
                    conn.as_mut().graceful_shutdown();
                }
            }
        }
    }
}

/// Resolves when the connection has had no request in progress for too
/// long: `true` when it never started one within the header timeout,
/// `false` when it has been idle for the idle timeout. Never resolves while
/// a request is in progress or when the bound that applies is off.
async fn idle_deadline(rx: &mut watch::Receiver<Activity>, opts: ServeOptions) -> bool {
    loop {
        let a = *rx.borrow_and_update();
        let first = a.started == 0;
        let limit = if first {
            opts.header_read_timeout
        } else {
            opts.idle_timeout
        };
        let deadline = limit.filter(|_| a.in_flight == 0).map(|l| a.idle_since + l);
        match deadline {
            Some(d) => tokio::select! {
                _ = sleep_until(d) => return first,
                r = rx.changed() => if r.is_err() { std::future::pending::<()>().await },
            },
            None => {
                if rx.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// A socket whose writes fail with `TimedOut` once they have been pending,
/// with no byte accepted, for `timeout`: a client that stops reading cannot
/// hold its connection, and the response it holds, forever.
struct StallIo {
    inner: TcpStream,
    timeout: Option<Duration>,
    stalled: Option<Pin<Box<Sleep>>>,
}

impl StallIo {
    fn new(inner: TcpStream, timeout: Option<Duration>) -> Self {
        StallIo {
            inner,
            timeout,
            stalled: None,
        }
    }

    /// After a write returned `Pending`: start the stall clock, or fail once
    /// it has run out.
    fn pending<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        let Some(timeout) = self.timeout else {
            return Poll::Pending;
        };
        let sleep = self
            .stalled
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        if sleep.as_mut().poll(cx).is_ready() {
            self.stalled = None;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the client read nothing for the body idle timeout",
            )));
        }
        Poll::Pending
    }

    fn progressed<T>(
        &mut self,
        r: Poll<io::Result<T>>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<T>> {
        match r {
            Poll::Pending => self.pending(cx),
            ready => {
                self.stalled = None;
                ready
            }
        }
    }
}

impl AsyncRead for StallIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for StallIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.progressed(r, cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.progressed(r, cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let r = Pin::new(&mut self.inner).poll_flush(cx);
        self.progressed(r, cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let r = Pin::new(&mut self.inner).poll_shutdown(cx);
        self.progressed(r, cx)
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A response body as served: it keeps its request in flight until dropped
/// and, unless it is small, is dropped by a watchdog once the client has
/// read nothing of it for the body idle timeout. Dropping it drops whatever
/// the inner body holds, such as a read-class permit or a token's
/// concurrency slot. HTTP/2 needs this on top of [`StallIo`]: a stream
/// whose flow-control window is used up stalls without stalling the socket.
pub(crate) struct ServeBody {
    inner: Inner,
    _held: InFlight,
}

enum Inner {
    Plain(Body),
    Watched(Arc<Watched>),
}

struct Watched(Mutex<WatchState>);

struct WatchState {
    /// `None` once finished or cut.
    body: Option<Body>,
    /// Set when the last poll returned a frame: from then on the body waits
    /// for the client. Cleared while the body itself is not ready.
    waiting_since: Option<Instant>,
    /// Cut by the watchdog; the next poll reports it.
    cut: bool,
}

impl Watched {
    fn lock(&self) -> MutexGuard<'_, WatchState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl ServeBody {
    fn new(body: Body, held: InFlight, idle: Option<Duration>) -> Self {
        let small = http_body::Body::size_hint(&body)
            .exact()
            .is_some_and(|n| n <= UNWATCHED_BODY_BYTES);
        let inner = match idle {
            Some(idle) if !small && !http_body::Body::is_end_stream(&body) => {
                let watched = Arc::new(Watched(Mutex::new(WatchState {
                    body: Some(body),
                    waiting_since: Some(Instant::now()),
                    cut: false,
                })));
                tokio::spawn(watchdog(Arc::downgrade(&watched), idle));
                Inner::Watched(watched)
            }
            _ => Inner::Plain(body),
        };
        ServeBody { inner, _held: held }
    }
}

/// Drops the body of `watched` once it has waited `idle` for the client.
async fn watchdog(watched: Weak<Watched>, idle: Duration) {
    loop {
        let deadline = {
            let Some(w) = watched.upgrade() else { return };
            let s = w.lock();
            if s.body.is_none() {
                return;
            }
            s.waiting_since.unwrap_or_else(Instant::now) + idle
        };
        sleep_until(deadline).await;
        let Some(w) = watched.upgrade() else { return };
        let mut s = w.lock();
        if s.waiting_since.is_some_and(|t| t.elapsed() >= idle) {
            let body = s.body.take();
            s.cut = true;
            drop(s);
            drop(body);
            metrics::counter!("leep_http_response_body_idle_total").increment(1);
            tracing::debug!("dropped a response the client stopped reading");
            return;
        }
    }
}

#[derive(Debug)]
struct IdleCut;

impl std::fmt::Display for IdleCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the client read nothing of the response for the body idle timeout")
    }
}

impl std::error::Error for IdleCut {}

impl http_body::Body for ServeBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        let w = match &mut this.inner {
            Inner::Plain(b) => return Pin::new(b).poll_frame(cx).map_err(Into::into),
            Inner::Watched(w) => w,
        };
        let mut s = w.lock();
        let Some(body) = s.body.as_mut() else {
            if std::mem::take(&mut s.cut) {
                return Poll::Ready(Some(Err(Box::new(IdleCut))));
            }
            return Poll::Ready(None);
        };
        match Pin::new(body).poll_frame(cx) {
            Poll::Ready(Some(r)) => {
                s.waiting_since = Some(Instant::now());
                Poll::Ready(Some(r.map_err(Into::into)))
            }
            Poll::Ready(None) => {
                s.body = None;
                Poll::Ready(None)
            }
            Poll::Pending => {
                s.waiting_since = None;
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Inner::Plain(b) => b.is_end_stream(),
            Inner::Watched(w) => {
                let s = w.lock();
                s.body.as_ref().map_or(!s.cut, |b| b.is_end_stream())
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Inner::Plain(b) => b.size_hint(),
            Inner::Watched(w) => w
                .lock()
                .body
                .as_ref()
                .map_or_else(SizeHint::default, |b| b.size_hint()),
        }
    }
}
