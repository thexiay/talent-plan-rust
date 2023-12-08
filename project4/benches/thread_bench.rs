
use std::env;
use std::fmt::Display;
/// 测试多线程下的写入和读取性能
/// 1. 写入，n个客户端，在每个单独的线程中写入10条数据，服务端有n(cpu cores)个服务线程工作
/// 2. 读取
/// 
use std::iter;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::net::ToSocketAddrs;
use std::ops::Range;
use std::thread::spawn;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};
use kvs::KvClient;
use kvs::KvServer;
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::error::Result;
use kvs::thread_pool::SharedQueueThreadPool;
use kvs::thread_pool::ThreadPool;
use log::info;
use tempfile::TempDir;

fn from_elem(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    info!("begin bench");

    let mut group = c.benchmark_group("from_elem");
    for data_set in [
        DataGenerater::new(0..100, 1), 
        DataGenerater::new(0..100, 2), 
        DataGenerater::new(0..100, 3)
    ].iter() {
        let client_thread_pool = SharedQueueThreadPool::new(10).unwrap();
        let server_thread_pool = SharedQueueThreadPool::new(10).unwrap();
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 5005);
        let engine = KvStore::open(temp_dir.path()).unwrap();
        spawn(move || KvServer::serve_with_engine(engine, server_thread_pool, addr).unwrap());
        group.bench_with_input(
            BenchmarkId::new("Test write bench", data_set), 
            data_set,
         |b, data_set| {
                b.iter(|| write_queued_kvstore(data_set.gen_set(), &client_thread_pool, addr))
        });
    }
    group.finish();
}

struct DataGenerater {
    range: Range<u64>,
    nums: u64
}

impl Display for DataGenerater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} nums in ({},{}))", self.nums, self.range.start, self.range.end)
    }
}


impl DataGenerater {
    fn new(range: Range<u64>, nums: u64) -> DataGenerater {
        DataGenerater { range, nums }
    }
    
    fn gen_set(&self) -> impl Iterator<Item = (String, String)> {
        let le = (&self.range.start).clone();
        let len = self.range.size_hint().0;
        (0..self.nums).map(move |i| {
            let index = i % len as u64 + le;
            (format!("key{}", index), format!("value{}", index))
        })
    }

    fn gen_get(&self) -> impl Iterator<Item = String> {
        let le = (&self.range.start).clone();
        let len = self.range.size_hint().0;
        (0..self.nums).map(move |i| 
            format!("key{}", i % len as u64 + le)
        )
    }
}


fn write_queued_kvstore<Iter, P, Addr>(input: Iter, thread_pool: &P, addr: Addr) 
where
    Iter: Iterator<Item = (String, String)>, 
    P: ThreadPool,
    Addr: ToSocketAddrs + Send + Clone + 'static,
{
    input.for_each(|(key, value)| {
        let add = addr.clone(); 
        thread_pool.spawn(|| {
            let mut client = KvClient::new(add).unwrap();
            client.set(key, value).unwrap();
            client.shutdown().unwrap();
        })
    })
    
}

fn read_queued_kvstore() {

}


criterion_group!(benches, from_elem);
criterion_main!(benches);