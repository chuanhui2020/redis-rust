use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redis_rust::storage::StorageEngine;
use redis_rust::protocol::{RespParser, RespValue};
use bytes::Bytes;

fn bench_storage_set(c: &mut Criterion) {
    let storage = StorageEngine::new();
    let value = Bytes::from_static(b"hello_world_value_12345");
    let mut i = 0u64;
    c.bench_function("storage_set", |b| b.iter(|| {
        i = i.wrapping_add(1);
        let key = format!("key_{:08}", i % 10000);
        black_box(storage.set(key, value.clone()).unwrap());
    }));
}

fn bench_storage_get(c: &mut Criterion) {
    let storage = StorageEngine::new();
    let value = Bytes::from_static(b"hello_world_value_12345");
    for i in 0..10000 {
        storage.set(format!("key_{:08}", i), value.clone()).unwrap();
    }
    let mut i = 0u64;
    c.bench_function("storage_get", |b| b.iter(|| {
        i = i.wrapping_add(1);
        let key = format!("key_{:08}", i % 10000);
        black_box(storage.get(&key).unwrap());
    }));
}

fn bench_storage_setget(c: &mut Criterion) {
    let storage = StorageEngine::new();
    let value = Bytes::from_static(b"hello_world_value_12345");
    let mut i = 0u64;
    c.bench_function("storage_setget", |b| b.iter(|| {
        i = i.wrapping_add(1);
        let key = format!("key_{:08}", i % 10000);
        black_box(storage.set(key.clone(), value.clone()).unwrap());
        black_box(storage.get(&key).unwrap());
    }));
}

fn bench_encode_bulkstring(c: &mut Criterion) {
    let parser = RespParser::new();
    let resp = RespValue::BulkString(Some(Bytes::from_static(b"value_12345")));
    let mut buf = Vec::with_capacity(256);
    c.bench_function("encode_bulkstring", |b| b.iter(|| {
        buf.clear();
        parser.encode_append(&resp, &mut buf);
        black_box(&buf);
    }));
}

fn bench_encode_ok(c: &mut Criterion) {
    let parser = RespParser::new();
    let resp = RespValue::SimpleString(Bytes::from_static(b"OK"));
    let mut buf = Vec::with_capacity(256);
    c.bench_function("encode_ok", |b| b.iter(|| {
        buf.clear();
        parser.encode_append(&resp, &mut buf);
        black_box(&buf);
    }));
}

criterion_group!(benches, bench_storage_set, bench_storage_get, bench_storage_setget, bench_encode_bulkstring, bench_encode_ok);
criterion_main!(benches);
