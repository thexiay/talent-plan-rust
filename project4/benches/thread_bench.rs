
#[macro_use]
extern crate lazy_static;

use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::thread::spawn;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use crossbeam_utils::sync::WaitGroup;
use kvs::KvClient;
use kvs::KvServer;
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::SledStore;
use kvs::ThreadHandle;
use kvs::thread_pool;
use kvs::thread_pool::RayonThreadPool;
use kvs::thread_pool::SharedQueueThreadPool;
use kvs::thread_pool::ThreadPool;
use log::info;
use tempfile::TempDir;

static A: i32 = 3;

lazy_static! {
    static ref SERVER_ADDR: SocketAddr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 5005).into();
}

fn write_group<SetUp>(c: &mut Criterion, setup: SetUp)
where
    SetUp: Fn(&TempDir, u32) -> ThreadHandle,
{
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    info!("begin bench");

    // init common tools
    let temp_dir = TempDir::new().unwrap();
    let mut group = c.benchmark_group("write_group");
    let num_cpus = num_cpus::get() as u32;
    let pool = RayonThreadPool::new(num_cpus * 2).unwrap();

    for threads in [ 1, 2, 4, 8, num_cpus, num_cpus * 2 ].iter() {
        let handle = setup(&temp_dir, *threads);
        group.bench_with_input(
            BenchmarkId::new("Test write bench", threads), 
            threads,
        |b, _| {
                b.iter(|| write(&pool))
        });
        // when exit scope pool and server exit.
        teardown_with_check(handle);
    }
    group.finish();
}

fn write_rayon_sledkvengine(c: &mut Criterion) {
    write_group(c, startup_with_rayon_sled);
}

fn write_queued_kvstore(c: &mut Criterion) {
    write_group(c,startup_with_shared);
}

fn teardown_with_check(handle: ThreadHandle) {
    // for 1000 inputs
    let mut client = KvClient::new(*SERVER_ADDR).unwrap();
    (0..1000).for_each(|i| {
        assert_eq!(
            client.get(format!("key{}", i)).unwrap().unwrap(),
            format!("value{}", i)
        );
    });
    handle.shutdown().unwrap();
}

/// startup with different thread pool and different server
fn startup_with_shared(
    temp_dir: &TempDir, 
    threads: u32, 
) -> ThreadHandle {
    let thread_pool = SharedQueueThreadPool::new(threads).unwrap();
    let engine = KvStore::open(temp_dir.path()).unwrap();
    KvServer::serve(engine, thread_pool, *SERVER_ADDR).unwrap()
}

fn startup_with_rayon(
    temp_dir: &TempDir, 
    threads: u32, 
) -> ThreadHandle {
    let thread_pool = RayonThreadPool::new(threads).unwrap();
    let engine = KvStore::open(temp_dir.path()).unwrap();
    KvServer::serve(engine, thread_pool, *SERVER_ADDR).unwrap()
}

fn startup_with_rayon_sled(
    temp_dir: &TempDir, 
    threads: u32, 
) -> ThreadHandle {
    let thread_pool = RayonThreadPool::new(threads).unwrap();
    let engine = SledStore::open(temp_dir.path()).unwrap();
    KvServer::serve(engine, thread_pool, *SERVER_ADDR).unwrap()
}


fn write<P: ThreadPool>(thread_pool: &P) {
    // for 1000 inputs write
    let wg = WaitGroup::new();
    (0..1000).for_each(|i| {
        let wg = wg.clone();
        thread_pool.spawn(move || {
            let mut client = KvClient::new(*SERVER_ADDR).unwrap();
            client.set(format!("key{}", i), format!("value{}", i)).unwrap();
            client.shutdown().unwrap();
            drop(wg);
        });
    });
    wg.wait();
}


fn read_queued_kvstore() {

}


criterion_group!(benches, write_queued_kvstore, write_rayon_sledkvengine);
criterion_main!(benches);