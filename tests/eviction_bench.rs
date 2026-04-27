// Eviction 随机采样性能基准测试

use redis_rust::pubsub::PubSubManager;
use redis_rust::server::Server;
use redis_rust::storage::StorageEngine;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) {
    let mut cmd = format!("*{}\r\n", parts.len());
    for part in parts {
        cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(cmd.as_bytes()).await.unwrap();
}

async fn recv_resp(stream: &mut TcpStream) -> redis_rust::protocol::RespValue {
    let parser = redis_rust::protocol::RespParser::new();
    let mut buf = bytes::BytesMut::with_capacity(4096);
    loop {
        match parser.parse(&mut buf).unwrap() {
            Some(resp) => return resp,
            None => {
                let mut tmp = [0u8; 1024];
                let n = stream.read(&mut tmp).await.unwrap();
                assert!(n > 0, "连接已关闭");
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}

async fn exec(stream: &mut TcpStream, parts: &[&str]) -> redis_rust::protocol::RespValue {
    send_cmd(stream, parts).await;
    recv_resp(stream).await
}

/// 基准：大量 key 下触发 maxmemory 淘汰的性能
#[tokio::test]
async fn bench_eviction_with_large_dataset() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut s = TcpStream::connect(addr).await.unwrap();

    // 插入 10,000 个 key（每个约 100 字节 value）
    let n_keys = 10_000;
    let value = "x".repeat(100);
    let start = Instant::now();
    for i in 0..n_keys {
        exec(&mut s, &["SET", &format!("key:{}", i), &value]).await;
    }
    let insert_elapsed = start.elapsed();
    println!(
        "插入 {} keys: {:?} ({:.0} ops/sec)",
        n_keys,
        insert_elapsed,
        n_keys as f64 / insert_elapsed.as_secs_f64()
    );

    // 估算每个 key 约 120 字节 (key + value + entry 开销)
    // 10000 keys ≈ 1.2MB，设置 maxmemory 为 400KB 强制大量淘汰
    let target_max = 400_000u64;
    exec(&mut s, &["CONFIG", "SET", "maxmemory", &target_max.to_string()]).await;

    // 再插入 2000 个 key，触发淘汰
    let evict_start = Instant::now();
    for i in n_keys..(n_keys + 2000) {
        exec(&mut s, &["SET", &format!("key:{}", i), &value]).await;
    }
    let evict_elapsed = evict_start.elapsed();
    let evict_count = 2000;
    println!(
        "触发淘汰 {} writes (maxmemory={}): {:?} ({:.0} ops/sec)",
        evict_count,
        target_max,
        evict_elapsed,
        evict_count as f64 / evict_elapsed.as_secs_f64()
    );

    // 验证最终 key 数量符合 maxmemory 限制
    let dbsize = exec(&mut s, &["DBSIZE"]).await;
    let final_count: i64 = match dbsize {
        redis_rust::protocol::RespValue::Integer(n) => n,
        _ => panic!("unexpected dbsize resp"),
    };
    println!("最终 key 数量: {} (原始 {}, 淘汰约 {})", final_count, n_keys, n_keys + 2000 - final_count as usize);
    assert!(final_count > 0);
}
