use crate::Result;

mod mpmc;
mod native;
mod rayon;
mod shared_pool;

pub use self::native::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;
pub use self::shared_pool::SharedQueueThreadPool;

pub trait ThreadPool: Send + 'static {
    /// Creates a new thread pool, immediately spawning the specified number of
    /// threads.
    ///
    /// Returns an error if any thread fails to spawn. All previously-spawned threads
    /// are terminated.
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// Spawn a function into the threadpool.
    ///
    /// Spawning always succeeds, but if the function panics the threadpool continues
    /// to operate with the same number of threads &mdash; the thread count is not
    /// reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}
