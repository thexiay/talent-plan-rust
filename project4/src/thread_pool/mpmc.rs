use std::{collections::VecDeque, sync::{Mutex, Arc}, any::Any};


pub struct Sender<T> {
    tx: Arc<Mutex<VecDeque<T>>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, t: T) {
        self.tx.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(t)
    }
}

pub struct Receiver<T> {
    rx: Arc<Mutex<VecDeque<T>>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self { rx: self.rx.clone() }
    }
}

impl<T> Receiver<T> {
    pub fn receive(&self) -> Option<T> {
        self.rx.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
    }
}

/// 1.拥塞控制
/// 2.高性能，无锁实现
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    // default for limitless cap
    let queue = Arc::new(
        Mutex::new(
            VecDeque::new()
        )
    );
    (Sender{ tx: queue.clone() }, Receiver{ rx: queue.clone() })
}