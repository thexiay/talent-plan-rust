use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    thread::{sleep, spawn},
    time::Duration,
};

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use log::error;

use super::ThreadPool;

pub struct SharedQueueThreadPool {
    // total threads cap
    threads: u64,

    // a sender to start task
    spawner: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::Result<Self>
    where
        Self: Sized,
    {
        // lanuch `threads` nums thread with zero buffer
        let (tx, rx) = bounded(0);
        (0..threads).for_each(|_| {
            let each_rx = rx.clone();
            spawn(move || run(each_rx));
        });
        Ok(SharedQueueThreadPool {
            threads: threads as u64,
            spawner: tx,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.spawner
            .send(Box::new(job))
            .expect("Thread pool has no thread left")
    }
}

fn run(rx: Receiver<Box<dyn FnOnce() + Send + 'static>>) {
    loop {
        match rx.try_recv() {
            Ok(f) => {
                if let Err(cause) = catch_unwind(AssertUnwindSafe(|| f())) {
                    error!("user task panic catch: \n{:#?}", cause);
                }
            }
            Err(TryRecvError::Empty) => sleep(Duration::from_millis(100)),
            Err(TryRecvError::Disconnected) => {
                error!("thread pool is be destoryed.");
                break;
            }
        }
    }
}
