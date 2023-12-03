
use crate::Result;

mod native;
mod shared_pool;
mod rayon;

pub use self::native::NaiveThreadPool;
pub use self::shared_pool::SharedQueueThreadPool;
pub use self::rayon::RayonThreadPool;

pub trait ThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of
    /// threads.
    ///
    /// Returns an error if any thread fails to spawn. All previously-spawned threads
    /// are terminated.
    fn new(threads: u32) -> Result<Self>
    where Self: Sized;

    /// Spawn a function into the threadpool.
    /// 
    /// Spawning always succeeds, but if the function panics the threadpool continues
    /// to operate with the same number of threads &mdash; the thread count is not
    /// reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}