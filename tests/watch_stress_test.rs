// WATCH 修复后的深度功能与性能测试

use bytes::BytesMut;
use redis_rust::protocol::{RespParser, RespValue};
use redis_rust::pubsub::PubSubManager;
use redis_rust::server::Server;
use redis_rust::storage::StorageEngine;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep, Instant};

async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) {
    let mut cmd = format!("*{}\r\n", parts.len());
    for part in parts {
        cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(cmd.as_bytes()).await.unwrap();
}

async fn recv_resp(stream: &mut TcpStream) -> RespValue {
    let parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(4096);
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

async fn exec(stream: &mut TcpStream, parts: &[&str]) -> RespValue {
    send_cmd(stream, parts).await;
    recv_resp(stream).await
}

// ========== 功能测试 ==========

/// 测试：WATCH 后 key 被 DEL，EXEC 应失败
#[tokio::test]
async fn test_watch_exec_after_del() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();

    exec(&mut a, &["SET", "w", "old"]).await;
    exec(&mut a, &["WATCH", "w"]).await;

    exec(&mut b, &["DEL", "w"]).await;

    exec(&mut a, &["MULTI"]).await;
    exec(&mut a, &["SET", "w", "new"]).await;
    let resp = exec(&mut a, &["EXEC"]).await;
    assert_eq!(resp, RespValue::BulkString(None), "WATCH 后 key 被删除，EXEC 应失败");
}

/// 测试：WATCH 后 key 被 RENAME，EXEC 应失败（原 key 不存在了）
#[tokio::test]
async fn test_watch_exec_after_rename() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();

    exec(&mut a, &["SET", "w", "old"]).await;
    exec(&mut a, &["WATCH", "w"]).await;

    exec(&mut b, &["RENAME", "w", "w2"]).await;

    exec(&mut a, &["MULTI"]).await;
    exec(&mut a, &["SET", "w", "new"]).await;
    let resp = exec(&mut a, &["EXEC"]).await;
    assert_eq!(resp, RespValue::BulkString(None), "WATCH 后 key 被 rename，EXEC 应失败");
}

/// 测试：WATCH 多个 key，只修改其中一个，EXEC 失败
#[tokio::test]
async fn test_watch_multi_key_one_changed() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();

    exec(&mut a, &["SET", "k1", "v1"]).await;
    exec(&mut a, &["SET", "k2", "v2"]).await;
    exec(&mut a, &["SET", "k3", "v3"]).await;

    exec(&mut a, &["WATCH", "k1", "k2", "k3"]).await;

    exec(&mut b, &["SET", "k2", "modified"]).await;

    exec(&mut a, &["MULTI"]).await;
    exec(&mut a, &["SET", "k1", "new1"]).await;
    exec(&mut a, &["SET", "k3", "new3"]).await;
    let resp = exec(&mut a, &["EXEC"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

/// 测试：并发多个连接同时 WATCH 同一个 key，只有一个 EXEC 能成功
#[tokio::test]
async fn test_watch_contention_single_key() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut init = TcpStream::connect(addr).await.unwrap();
    exec(&mut init, &["SET", "counter", "0"]).await;
    drop(init);

    let n = 20;
    let n_tx = 100;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _i in 0..n {
        let success_count = success_count.clone();
        let addr = addr;
        let h = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            for _ in 0..n_tx {
                exec(&mut stream, &["WATCH", "counter"]).await;
                exec(&mut stream, &["MULTI"]).await;
                exec(&mut stream, &["INCR", "counter"]).await;
                let resp = exec(&mut stream, &["EXEC"]).await;

                if let RespValue::Array(arr) = resp {
                    if !arr.is_empty() {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            drop(stream);
        });
        handles.push(h);
    }

    for h in handles {
        h.await.unwrap();
    }

    let success = success_count.load(Ordering::SeqCst);
    assert!(
        success > 0,
        "并发 WATCH 事务应至少部分成功（因为没有外部修改，只有自身竞争）"
    );
    println!("并发同一 key WATCH+EXEC 成功率: {}/{}", success, n * n_tx);

    let mut verify = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut verify, &["GET", "counter"]).await;
    let final_val: i64 = match resp {
        RespValue::BulkString(Some(b)) => std::str::from_utf8(&b).unwrap().parse().unwrap(),
        _ => panic!("unexpected resp"),
    };
    assert_eq!(final_val, success as i64, "最终值应等于成功事务数");
}

/// 测试：高并发读写竞争，一半连接 WATCH+EXEC，一半直接修改
#[tokio::test]
async fn test_watch_race_with_modifiers() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut init = TcpStream::connect(addr).await.unwrap();
    exec(&mut init, &["SET", "x", "0"]).await;
    drop(init);

    let n_watchers = 10;
    let n_modifiers = 10;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..n_watchers {
        let success_count = success_count.clone();
        let addr = addr;
        handles.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            exec(&mut s, &["WATCH", "x"]).await;
            sleep(Duration::from_millis(10)).await;
            exec(&mut s, &["MULTI"]).await;
            exec(&mut s, &["INCR", "x"]).await;
            let resp = exec(&mut s, &["EXEC"]).await;
            if let RespValue::Array(arr) = resp {
                if !arr.is_empty() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    for _ in 0..n_modifiers {
        let addr = addr;
        handles.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            sleep(Duration::from_millis(5)).await;
            exec(&mut s, &["INCR", "x"]).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let success = success_count.load(Ordering::SeqCst);
    println!("成功事务数: {}/{}", success, n_watchers);
    // 有 modifiers 竞争时，watcher 事务可能部分或全部失败
    assert!(success <= n_watchers);

    let mut verify = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut verify, &["GET", "x"]).await;
    let final_val: i64 = match resp {
        RespValue::BulkString(Some(b)) => std::str::from_utf8(&b).unwrap().parse().unwrap(),
        _ => panic!("unexpected resp"),
    };
    // 最终值 = modifiers 的 INCR 次数 + 成功的事务数
    assert_eq!(final_val, (n_modifiers + success) as i64, "最终值应等于 modifiers + 成功事务数");
}

/// 测试：WATCH 后 key 过期（使用 PX 设置短 TTL）
#[tokio::test]
async fn test_watch_exec_after_expire() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut a = TcpStream::connect(addr).await.unwrap();
    exec(&mut a, &["SET", "w", "val", "PX", "50"]).await;
    exec(&mut a, &["WATCH", "w"]).await;

    sleep(Duration::from_millis(80)).await;

    // 触发 lazy expire（GET 会删除过期 key）
    let resp = exec(&mut a, &["GET", "w"]).await;
    assert_eq!(resp, RespValue::BulkString(None), "key 应已过期");

    exec(&mut a, &["MULTI"]).await;
    exec(&mut a, &["SET", "w", "new"]).await;
    let resp = exec(&mut a, &["EXEC"]).await;
    assert_eq!(resp, RespValue::BulkString(None), "WATCH 后 key 过期，EXEC 应失败");
}

/// 测试：事务中多个命令混合，WATCH 检测正确
#[tokio::test]
async fn test_watch_mixed_transaction_commands() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();

    exec(&mut a, &["SET", "k", "v"]).await;
    exec(&mut a, &["WATCH", "k"]).await;

    exec(&mut b, &["APPEND", "k", "_extra"]).await;

    exec(&mut a, &["MULTI"]).await;
    exec(&mut a, &["GET", "k"]).await;
    exec(&mut a, &["SET", "k", "updated"]).await;
    let resp = exec(&mut a, &["EXEC"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

// ========== 性能测试 ==========

/// 基准：单连接无 WATCH 的 SET 吞吐量
#[tokio::test]
async fn bench_set_without_watch() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut s = TcpStream::connect(addr).await.unwrap();
    let n = 5000;
    let start = Instant::now();
    for i in 0..n {
        exec(&mut s, &["SET", &format!("k{}", i % 100), &format!("v{}", i)]).await;
    }
    let elapsed = start.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("SET without WATCH: {} ops/sec ({} ops in {:?})", ops as u64, n, elapsed);
    assert!(ops > 1000.0, "SET 吞吐量应大于 1000 ops/sec");
}

/// 基准：单连接 WATCH + MULTI + EXEC 吞吐量（事务成功）
#[tokio::test]
async fn bench_watch_exec_success() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut s = TcpStream::connect(addr).await.unwrap();
    let n = 2000;
    let start = Instant::now();
    for i in 0..n {
        exec(&mut s, &["WATCH", "counter"]).await;
        exec(&mut s, &["MULTI"]).await;
        exec(&mut s, &["SET", "counter", &format!("{}", i)]).await;
        exec(&mut s, &["EXEC"]).await;
    }
    let elapsed = start.elapsed();
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("WATCH+EXEC (success): {} ops/sec ({} tx in {:?})", ops as u64, n, elapsed);
    assert!(ops > 500.0, "WATCH+EXEC 吞吐量应大于 500 tx/sec");
}

/// 基准：10 个并发连接同时 WATCH + EXEC（无竞争）
#[tokio::test]
async fn bench_concurrent_watch_exec_no_contention() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let n_conn = 10;
    let n_tx = 500;
    let start = Instant::now();
    let mut handles = vec![];

    for c in 0..n_conn {
        let addr = addr;
        handles.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            for i in 0..n_tx {
                let key = format!("k{}_{}", c, i % 10);
                exec(&mut s, &["WATCH", &key]).await;
                exec(&mut s, &["MULTI"]).await;
                exec(&mut s, &["SET", &key, &format!("v{}", i)]).await;
                exec(&mut s, &["EXEC"]).await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_tx = n_conn * n_tx;
    let ops = total_tx as f64 / elapsed.as_secs_f64();
    println!("并发 WATCH+EXEC (无竞争, {} conn): {} tx/sec ({} tx in {:?})", n_conn, ops as u64, total_tx, elapsed);
    assert!(ops > 2000.0, "并发 WATCH+EXEC 应大于 2000 tx/sec");
}

/// 基准：10 个并发连接同时 WATCH 同一个 key + EXEC（高竞争）
#[tokio::test]
async fn bench_concurrent_watch_exec_high_contention() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut init = TcpStream::connect(addr).await.unwrap();
    exec(&mut init, &["SET", "shared", "0"]).await;
    drop(init);

    let n_conn = 10;
    let n_tx = 200;
    let start = Instant::now();
    let mut handles = vec![];

    for _ in 0..n_conn {
        let addr = addr;
        handles.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            for _ in 0..n_tx {
                exec(&mut s, &["WATCH", "shared"]).await;
                exec(&mut s, &["MULTI"]).await;
                exec(&mut s, &["INCR", "shared"]).await;
                exec(&mut s, &["EXEC"]).await;
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_tx = n_conn * n_tx;
    let ops = total_tx as f64 / elapsed.as_secs_f64();
    println!("并发 WATCH+EXEC (高竞争, {} conn, 同一 key): {} tx/sec ({} tx in {:?})", n_conn, ops as u64, total_tx, elapsed);
    // 高竞争下吞吐量会下降，但不应对数
    assert!(ops > 100.0, "高竞争 WATCH+EXEC 应大于 100 tx/sec");
}

/// 基准：对比 watch_count>0 时 SET 与 watch_count=0 时 SET 的性能差异
#[tokio::test]
async fn bench_set_with_watch_enabled_vs_disabled() {
    // 方案 A: 不使用 WATCH（watch_count = 0）
    let storage_a = StorageEngine::new();
    let server_a = Server::new("127.0.0.1:0", storage_a, None, PubSubManager::new(), None);
    let (addr_a, _handle_a) = server_a.start().await.unwrap();

    // 方案 B: 使用 WATCH（watch_count > 0）
    let storage_b = StorageEngine::new();
    let server_b = Server::new("127.0.0.1:0", storage_b, None, PubSubManager::new(), None);
    let (addr_b, _handle_b) = server_b.start().await.unwrap();

    let n = 5000;

    // A - 无 WATCH
    let mut s_a = TcpStream::connect(addr_a).await.unwrap();
    let start_a = Instant::now();
    for i in 0..n {
        exec(&mut s_a, &["SET", "k", &format!("v{}", i)]).await;
    }
    let elapsed_a = start_a.elapsed();
    let ops_a = n as f64 / elapsed_a.as_secs_f64();

    // B - 有 WATCH（先让另一个连接 WATCH 一个 key，使 watch_count > 0）
    let mut s_b_watcher = TcpStream::connect(addr_b).await.unwrap();
    exec(&mut s_b_watcher, &["WATCH", "some_key"]).await;
    let mut s_b = TcpStream::connect(addr_b).await.unwrap();
    let start_b = Instant::now();
    for i in 0..n {
        exec(&mut s_b, &["SET", "k", &format!("v{}", i)]).await;
    }
    let elapsed_b = start_b.elapsed();
    let ops_b = n as f64 / elapsed_b.as_secs_f64();

    println!("SET without WATCH overhead: {} ops/sec", ops_a as u64);
    println!("SET with WATCH enabled:     {} ops/sec", ops_b as u64);
    println!("性能差异: {:.1}%", (ops_a - ops_b) / ops_a * 100.0);

    // WATCH 开启后 SET 性能下降应在合理范围内（< 30%）
    assert!(ops_b > ops_a * 0.7, "WATCH 开启后 SET 性能下降不应超过 30%");
}
