
/// 测试多线程下的写入和读取性能
/// 1. 写入，100个客户端，每个客户端写入10个数据，服务端，有n(cpu cores)个服务线程工作
/// 
/// 
use std::iter;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main};

fn from_elem(c: &mut Criterion) {
    static KB: usize = 1024;

    let mut group = c.benchmark_group("from_elem");
    for size in [KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| iter::repeat(0u8).take(size).collect::<Vec<_>>());
        });
    }
    group.finish();
}

fn 


criterion_group!(benches, from_elem);
criterion_main!(benches);