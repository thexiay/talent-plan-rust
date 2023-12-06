use std::{thread::{spawn, sleep}, time::Duration, panic::{catch_unwind, AssertUnwindSafe}};

use log::error;

use crate::thread_pool::mpmc::channel;

use super::{ThreadPool, mpmc::{Sender, Receiver}};


pub struct SharedQueueThreadPool {
    // total threads cap
    threads: u64,

    // a sender to start task
    spawner: Sender<Box<dyn FnOnce() + Send + 'static>>, 

}

impl ThreadPool for SharedQueueThreadPool  {
    fn new(threads: u32) -> crate::Result<Self>
    where 
        Self: Sized 
    {
        let (tx, rx) = channel();
        (0..threads).for_each(|_| {
            let each_rx = rx.clone();
            spawn(move || {
                // 实现一个一直跑的逻辑，需要捕获rx
                run(each_rx)
            });
        });
        Ok(SharedQueueThreadPool {
            threads: threads as u64,
            spawner: tx,
        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.spawner.send(Box::new(job))
    }
}

fn run(rx: Receiver<Box<dyn FnOnce() + Send + 'static>>) {

    loop {
        if let Some(f) = rx.receive() {
            if let Err(cause) = catch_unwind(AssertUnwindSafe(|| {
                f()
            })) {
                error!("user task panic catch: \n{:#?}", cause);
            }
        }

        // for every user task or idle rece, just sleep 0.1s
        sleep(Duration::from_millis(100))    
    }
}