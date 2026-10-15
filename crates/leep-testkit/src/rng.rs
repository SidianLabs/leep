use std::future::Future;
use std::pin::pin;
use std::task::Poll;

/// Small deterministic generator (xorshift64*), so test seeds do not depend on
/// an external crate's stream staying stable.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // SplitMix64 step: spreads small seeds and never leaves the state zero.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Rng((z ^ (z >> 31)) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `0..n`; `n` must be positive.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// True with probability `per_mille / 1000`.
    pub fn chance(&mut self, per_mille: u32) -> bool {
        self.next_u64() % 1000 < u64::from(per_mille)
    }
}

/// Poll `fut` until it finishes or has suspended `polls` times, then drop it.
/// `None` means it was cancelled mid-flight: what a request timeout or a
/// client disconnect does to a handler, at a reproducible point.
pub async fn cancel_after_polls<F: Future>(fut: F, polls: usize) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut suspended = 0;
    std::future::poll_fn(move |cx| {
        if suspended >= polls {
            return Poll::Ready(None);
        }
        match fut.as_mut().poll(cx) {
            Poll::Ready(v) => Poll::Ready(Some(v)),
            Poll::Pending => {
                suspended += 1;
                Poll::Pending
            }
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let (mut a, mut b) = (Rng::new(7), Rng::new(7));
        let xs: Vec<_> = (0..8).map(|_| a.next_u64()).collect();
        assert_eq!(xs, (0..8).map(|_| b.next_u64()).collect::<Vec<_>>());
        assert_ne!(xs[0], Rng::new(8).next_u64());
        let mut r = Rng::new(0);
        assert!((0..100).all(|_| r.below(3) < 3));
        assert!(!(0..100).any(|_| r.chance(0)));
        assert!((0..100).all(|_| r.chance(1000)));
    }

    #[tokio::test]
    async fn cancels_at_the_requested_suspension() {
        let slow = || async {
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            5
        };
        assert_eq!(cancel_after_polls(slow(), 0).await, None);
        assert_eq!(cancel_after_polls(slow(), 1).await, None);
        assert_eq!(cancel_after_polls(slow(), 2).await, None);
        assert_eq!(cancel_after_polls(slow(), 3).await, Some(5));
    }
}
