//! Which share of the metadata store's connections a task draws on (ADR
//! 0067). Land leaders run in [`Lane::Land`] so that request handlers
//! filling the shared pool can never queue the one task every land on a ref
//! waits for. Stores without separate pools ignore it.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A share of a store's connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Lane {
    /// The shared pool every request handler draws on.
    #[default]
    Shared,
    /// The pool reserved for land leaders and their ref moves.
    Land,
}

thread_local! {
    /// Set only while a [`Scoped`] future is being polled on this thread,
    /// which makes it task-local without depending on an async runtime.
    static LANE: Cell<Lane> = const { Cell::new(Lane::Shared) };
}

/// The lane of the running task: [`Lane::Shared`] outside any [`scope`].
pub fn current() -> Lane {
    LANE.with(Cell::get)
}

/// Run `f` in `lane`. Task-local: a task spawned inside it is in
/// [`Lane::Shared`] unless wrapped with [`inherit`].
pub fn scope<F: Future>(lane: Lane, f: F) -> Scoped<F> {
    Scoped {
        lane,
        inner: Box::pin(f),
    }
}

/// `f` in the caller's lane, for a future about to be spawned on its own
/// task.
pub fn inherit<F: Future>(f: F) -> Scoped<F> {
    scope(current(), f)
}

/// A future polled in a lane; see [`scope`].
pub struct Scoped<F> {
    lane: Lane,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        struct Restore(Lane);
        impl Drop for Restore {
            fn drop(&mut self) {
                LANE.with(|l| l.set(self.0));
            }
        }
        let this = self.get_mut();
        let _restore = Restore(LANE.with(|l| l.replace(this.lane)));
        this.inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scopes_and_inherits() {
        assert_eq!(current(), Lane::Shared);
        scope(Lane::Land, async {
            assert_eq!(current(), Lane::Land);
            let plain = tokio::spawn(async { current() }).await.unwrap();
            assert_eq!(plain, Lane::Shared, "spawning leaves the lane");
            let kept = tokio::spawn(inherit(async { current() })).await.unwrap();
            assert_eq!(kept, Lane::Land);
            let inner = scope(Lane::Shared, async {
                tokio::task::yield_now().await;
                current()
            })
            .await;
            assert_eq!(inner, Lane::Shared, "scopes nest");
            tokio::task::yield_now().await;
            assert_eq!(current(), Lane::Land, "and restore across polls");
        })
        .await;
        assert_eq!(current(), Lane::Shared);
    }
}
