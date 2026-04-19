// 集成测试：启动真实 TCP 服务器，通过 tokio TcpStream 发送 RESP 命令验证功能

use bytes::{Bytes, BytesMut};
use redis_rust::protocol::{RespParser, RespValue};
use redis_rust::pubsub::PubSubManager;
use redis_rust::server::Server;
use redis_rust::storage::StorageEngine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

/// 向 TCP 流发送 RESP 数组格式的命令
async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) {
    let mut cmd = format!("*{}\r\n", parts.len());
    for part in parts {
        cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(cmd.as_bytes()).await.unwrap();
}

/// 从 TCP 流读取并解析一个 RESP 响应
async fn recv_resp(stream: &mut TcpStream) -> RespValue {
    let parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        match parser.parse(&mut buf).unwrap() {
            Some(resp) => return resp,
            None => {
                let mut tmp = [0u8; 1024];
                let n = stream.read(&mut tmp).await.unwrap();
                assert!(n > 0, "连接已关闭，未收到完整响应");
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}

/// 发送命令并返回响应的便捷函数
async fn exec(stream: &mut TcpStream, parts: &[&str]) -> RespValue {
    send_cmd(stream, parts).await;
    recv_resp(stream).await
}

// ---------- 基础命令测试 ----------

#[tokio::test]
async fn test_ping() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut stream, &["PING"]).await;
    assert_eq!(resp, RespValue::SimpleString("PONG".to_string()));
}

#[tokio::test]
async fn test_ping_with_message() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut stream, &["PING", "hello"]).await;
    assert_eq!(resp, RespValue::SimpleString("hello".to_string()));
}

#[tokio::test]
async fn test_set_and_get() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let resp = exec(&mut stream, &["SET", "name", "redis"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", "name"]).await;
    assert_eq!(
        resp,
        RespValue::BulkString(Some(bytes::Bytes::from("redis")))
    );
}

#[tokio::test]
async fn test_set_and_del_and_get() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["SET", "key", "val"]).await;
    let resp = exec(&mut stream, &["DEL", "key"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    let resp = exec(&mut stream, &["GET", "key"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

#[tokio::test]
async fn test_exists() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let resp = exec(&mut stream, &["EXISTS", "key"]).await;
    assert_eq!(resp, RespValue::Integer(0));

    exec(&mut stream, &["SET", "key", "val"]).await;
    let resp = exec(&mut stream, &["EXISTS", "key"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    exec(&mut stream, &["DEL", "key"]).await;
    let resp = exec(&mut stream, &["EXISTS", "key"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}

// ---------- TTL 相关测试 ----------

#[tokio::test]
async fn test_set_with_ex_and_expire() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SET with EX 1（1 秒后过期）
    let resp = exec(&mut stream, &["SET", "temp", "data", "EX", "1"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", "temp"]).await;
    assert_eq!(
        resp,
        RespValue::BulkString(Some(bytes::Bytes::from("data")))
    );

    // 等待 1.5 秒确保过期
    sleep(Duration::from_millis(1500)).await;

    let resp = exec(&mut stream, &["GET", "temp"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

#[tokio::test]
async fn test_expire_and_ttl() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["SET", "key", "val"]).await;

    // EXPIRE 设置 10 秒
    let resp = exec(&mut stream, &["EXPIRE", "key", "10"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // TTL 应该在 9~10 之间
    let resp = exec(&mut stream, &["TTL", "key"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n >= 9 && n <= 10),
        other => panic!("期望 Integer，得到 {:?}", other),
    }

    // 对不存在的 key 返回 0
    let resp = exec(&mut stream, &["EXPIRE", "missing", "10"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_ttl_no_expire_and_missing() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["SET", "key", "val"]).await;

    // 无过期时间的 key 返回 -1
    let resp = exec(&mut stream, &["TTL", "key"]).await;
    assert_eq!(resp, RespValue::Integer(-1));

    // 不存在的 key 返回 -2
    let resp = exec(&mut stream, &["TTL", "missing"]).await;
    assert_eq!(resp, RespValue::Integer(-2));
}

// ---------- 边界测试 ----------

#[tokio::test]
async fn test_empty_key_and_value() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let resp = exec(&mut stream, &["SET", "", ""]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", ""]).await;
    assert_eq!(
        resp,
        RespValue::BulkString(Some(bytes::Bytes::from_static(b"")))
    );
}

#[tokio::test]
async fn test_long_key() {
    let long_key = "a".repeat(1000);
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let resp = exec(&mut stream, &["SET", &long_key, "val"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", &long_key]).await;
    assert_eq!(
        resp,
        RespValue::BulkString(Some(bytes::Bytes::from("val")))
    );
}

#[tokio::test]
async fn test_get_nonexistent() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut stream, &["GET", "nonexistent"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

#[tokio::test]
async fn test_del_nonexistent() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut stream, &["DEL", "nonexistent"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_invalid_command() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut stream, &["UNKNOWNCMD"]).await;
    assert_eq!(
        resp,
        RespValue::Error("ERR unknown command 'UNKNOWNCMD'".to_string())
    );
}

#[tokio::test]
async fn test_wrong_number_of_args() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SET 只有 key 没有 value
    let resp = exec(&mut stream, &["SET", "key"]).await;
    assert!(
        matches!(resp, RespValue::Error(ref e) if e.contains("SET")),
        "期望 SET 参数错误，得到 {:?}",
        resp
    );
}

// ---------- 并发测试 ----------

#[tokio::test]
async fn test_concurrent_clients() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut handles = vec![];
    for i in 0..10 {
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let key = format!("key{}", i);
            let value = format!("value{}", i);

            let resp = exec(&mut stream, &["SET", &key, &value]).await;
            assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

            let resp = exec(&mut stream, &["GET", &key]).await;
            assert_eq!(
                resp,
                RespValue::BulkString(Some(bytes::Bytes::from(value)))
            );
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_set_same_key() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut handles = vec![];
    for i in 0..20 {
        let val = format!("val{}", i);
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            exec(&mut stream, &["SET", "shared", &val]).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 验证最终值是其中之一
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut stream, &["GET", "shared"]).await;
    match resp {
        RespValue::BulkString(Some(data)) => {
            let s = String::from_utf8_lossy(&data);
            assert!(s.starts_with("val"), "最终值不以 val 开头: {}", s);
        }
        other => panic!("期望 BulkString(Some(...))，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_keys_scan_rename_type_persist_pexpire_pttl_dbsize_info() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 准备数据
    exec(&mut stream, &["SET", "hello", "v1"]).await;
    exec(&mut stream, &["SET", "hallo", "v2"]).await;
    exec(&mut stream, &["SET", "world", "v3"]).await;

    // KEYS
    let resp = exec(&mut stream, &["KEYS", "h*lo"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("期望 Array"),
    }

    // SCAN
    let resp = exec(&mut stream, &["SCAN", "0", "MATCH", "*", "COUNT", "10"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2); // cursor + keys
        }
        _ => panic!("期望 Array"),
    }

    // RENAME
    let resp = exec(&mut stream, &["RENAME", "hello", "hello2"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["GET", "hello2"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("v1"))));

    // TYPE
    let resp = exec(&mut stream, &["TYPE", "hello2"]).await;
    assert_eq!(resp, RespValue::SimpleString("string".to_string()));
    let resp = exec(&mut stream, &["TYPE", "missing"]).await;
    assert_eq!(resp, RespValue::SimpleString("none".to_string()));

    // PEXPIRE + PTTL + PERSIST
    exec(&mut stream, &["SET", "temp", "v"]).await;
    let resp = exec(&mut stream, &["PEXPIRE", "temp", "5000"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    let resp = exec(&mut stream, &["PTTL", "temp"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n > 0 && n <= 5000),
        _ => panic!("期望 Integer PTTL"),
    }

    let resp = exec(&mut stream, &["PERSIST", "temp"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    let resp = exec(&mut stream, &["PTTL", "temp"]).await;
    assert_eq!(resp, RespValue::Integer(-1));

    // DBSIZE
    let resp = exec(&mut stream, &["DBSIZE"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n >= 4, "DBSIZE 应 >= 4, 得到 {}", n),
        _ => panic!("期望 Integer DBSIZE"),
    }

    // INFO
    let resp = exec(&mut stream, &["INFO"]).await;
    match resp {
        RespValue::BulkString(Some(data)) => {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("redis_version"), "INFO 应包含 redis_version");
        }
        RespValue::SimpleString(s) => {
            assert!(s.contains("redis_version"), "INFO 应包含 redis_version");
        }
        _ => panic!("期望 BulkString 或 SimpleString INFO, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_pubsub() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    // 订阅者连接
    let mut sub_stream = TcpStream::connect(addr).await.unwrap();

    // 订阅频道 ch1
    send_cmd(&mut sub_stream, &["SUBSCRIBE", "ch1"]).await;
    let resp = recv_resp(&mut sub_stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("subscribe"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("ch1"))));
            assert_eq!(arr[2], RespValue::Integer(1));
        }
        _ => panic!("期望 subscribe 确认数组, 得到 {:?}", resp),
    }

    // 另一个连接发布消息
    let mut pub_stream = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut pub_stream, &["PUBLISH", "ch1", "hello"]).await;
    let resp = recv_resp(&mut pub_stream).await;
    assert_eq!(resp, RespValue::Integer(1));

    // 订阅者收到推送消息
    let resp = recv_resp(&mut sub_stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("message"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("ch1"))));
            assert_eq!(arr[2], RespValue::BulkString(Some(bytes::Bytes::from("hello"))));
        }
        _ => panic!("期望 message 推送数组, 得到 {:?}", resp),
    }

    // 取消订阅
    send_cmd(&mut sub_stream, &["UNSUBSCRIBE", "ch1"]).await;
    let resp = recv_resp(&mut sub_stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("unsubscribe"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("ch1"))));
            assert_eq!(arr[2], RespValue::Integer(0));
        }
        _ => panic!("期望 unsubscribe 确认数组, 得到 {:?}", resp),
    }

    // 再次发布，订阅者不应收到（连接已回到普通模式）
    send_cmd(&mut pub_stream, &["PUBLISH", "ch1", "world"]).await;
    let resp = recv_resp(&mut pub_stream).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_pubsub_psubscribe() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    // 模式订阅者
    let mut sub_stream = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut sub_stream, &["PSUBSCRIBE", "news.*"]).await;
    let resp = recv_resp(&mut sub_stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("psubscribe"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("news.*"))));
            assert_eq!(arr[2], RespValue::Integer(1));
        }
        _ => panic!("期望 psubscribe 确认数组, 得到 {:?}", resp),
    }

    // 发布到匹配频道
    let mut pub_stream = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut pub_stream, &["PUBLISH", "news.sport", "goal"]).await;
    let resp = recv_resp(&mut pub_stream).await;
    assert_eq!(resp, RespValue::Integer(1));

    // 订阅者收到 pmessage
    let resp = recv_resp(&mut sub_stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 4);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("pmessage"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("news.*"))));
            assert_eq!(arr[2], RespValue::BulkString(Some(bytes::Bytes::from("news.sport"))));
            assert_eq!(arr[3], RespValue::BulkString(Some(bytes::Bytes::from("goal"))));
        }
        _ => panic!("期望 pmessage 推送数组, 得到 {:?}", resp),
    }

    // 发布到不匹配频道
    send_cmd(&mut pub_stream, &["PUBLISH", "weather", "rain"]).await;
    let resp = recv_resp(&mut pub_stream).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_pubsub_ping_in_subscribed_mode() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 订阅后进入订阅模式
    send_cmd(&mut stream, &["SUBSCRIBE", "ch"]).await;
    let _ = recv_resp(&mut stream).await;

    // 订阅模式下 PING
    send_cmd(&mut stream, &["PING"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("PONG".to_string()));

    // 订阅模式下不允许普通命令
    send_cmd(&mut stream, &["GET", "k"]).await;
    let resp = recv_resp(&mut stream).await;
    match resp {
        RespValue::Error(msg) => {
            assert!(msg.contains("allowed in this context"), "错误信息应提示仅允许 pub/sub 命令");
        }
        _ => panic!("期望错误响应, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_multi_exec() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // MULTI
    send_cmd(&mut stream, &["MULTI"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // SET 入队
    send_cmd(&mut stream, &["SET", "a", "1"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("QUEUED".to_string()));

    // SET 入队
    send_cmd(&mut stream, &["SET", "b", "2"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("QUEUED".to_string()));

    // EXEC
    send_cmd(&mut stream, &["EXEC"]).await;
    let resp = recv_resp(&mut stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::SimpleString("OK".to_string()));
            assert_eq!(arr[1], RespValue::SimpleString("OK".to_string()));
        }
        _ => panic!("期望 EXEC 结果数组, 得到 {:?}", resp),
    }

    // 验证值已设置
    send_cmd(&mut stream, &["GET", "a"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("1"))));

    send_cmd(&mut stream, &["GET", "b"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("2"))));
}

#[tokio::test]
async fn test_multi_discard() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_cmd(&mut stream, &["MULTI"]).await;
    let _ = recv_resp(&mut stream).await;

    send_cmd(&mut stream, &["SET", "x", "1"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("QUEUED".to_string()));

    // DISCARD
    send_cmd(&mut stream, &["DISCARD"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 验证值未设置
    send_cmd(&mut stream, &["GET", "x"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

#[tokio::test]
async fn test_watch_exec_success() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 先设置初始值
    send_cmd(&mut stream, &["SET", "w", "old"]).await;
    let _ = recv_resp(&mut stream).await;

    // WATCH
    send_cmd(&mut stream, &["WATCH", "w"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // MULTI → SET → EXEC（无其他连接修改，应成功）
    send_cmd(&mut stream, &["MULTI"]).await;
    let _ = recv_resp(&mut stream).await;

    send_cmd(&mut stream, &["SET", "w", "new"]).await;
    let _ = recv_resp(&mut stream).await;

    send_cmd(&mut stream, &["EXEC"]).await;
    let resp = recv_resp(&mut stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::SimpleString("OK".to_string()));
        }
        _ => panic!("期望 EXEC 成功, 得到 {:?}", resp),
    }

    send_cmd(&mut stream, &["GET", "w"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("new"))));
}

#[tokio::test]
async fn test_watch_exec_failure() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    // 连接 A：WATCH + MULTI + SET
    let mut stream_a = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut stream_a, &["SET", "w", "old"]).await;
    let _ = recv_resp(&mut stream_a).await;

    send_cmd(&mut stream_a, &["WATCH", "w"]).await;
    let _ = recv_resp(&mut stream_a).await;

    // 连接 B：在 A 的 WATCH 之后修改 key
    let mut stream_b = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut stream_b, &["SET", "w", "modified"]).await;
    let _ = recv_resp(&mut stream_b).await;

    // 连接 A：MULTI → SET → EXEC（应失败，返回 nil）
    send_cmd(&mut stream_a, &["MULTI"]).await;
    let _ = recv_resp(&mut stream_a).await;

    send_cmd(&mut stream_a, &["SET", "w", "new"]).await;
    let _ = recv_resp(&mut stream_a).await;

    send_cmd(&mut stream_a, &["EXEC"]).await;
    let resp = recv_resp(&mut stream_a).await;
    assert_eq!(resp, RespValue::BulkString(None), "WATCH 被修改后 EXEC 应返回 nil");

    // 验证值是连接 B 修改的
    send_cmd(&mut stream_a, &["GET", "w"]).await;
    let resp = recv_resp(&mut stream_a).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("modified"))));
}

#[tokio::test]
async fn test_nested_multi_error() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_cmd(&mut stream, &["MULTI"]).await;
    let _ = recv_resp(&mut stream).await;

    send_cmd(&mut stream, &["MULTI"]).await;
    let resp = recv_resp(&mut stream).await;
    match resp {
        RespValue::Error(msg) => {
            assert!(msg.contains("nested"), "嵌套 MULTI 应报错");
        }
        _ => panic!("期望错误响应, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_exec_without_multi_error() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_cmd(&mut stream, &["EXEC"]).await;
    let resp = recv_resp(&mut stream).await;
    match resp {
        RespValue::Error(msg) => {
            assert!(msg.contains("without MULTI"), "EXEC without MULTI 应报错");
        }
        _ => panic!("期望错误响应, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_config_maxmemory() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // CONFIG SET maxmemory
    send_cmd(&mut stream, &["CONFIG", "SET", "maxmemory", "100"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // CONFIG GET maxmemory
    send_cmd(&mut stream, &["CONFIG", "GET", "maxmemory"]).await;
    let resp = recv_resp(&mut stream).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("maxmemory"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("100"))));
        }
        _ => panic!("期望 CONFIG GET 返回数组, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_maxmemory_eviction_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 先设置上限，确保 touch() 会记录访问时间
    send_cmd(&mut stream, &["CONFIG", "SET", "maxmemory", "70"]).await;
    let _ = recv_resp(&mut stream).await;

    send_cmd(&mut stream, &["SET", "a", "va"]).await;
    let _ = recv_resp(&mut stream).await;

    send_cmd(&mut stream, &["SET", "b", "vb"]).await;
    let _ = recv_resp(&mut stream).await;

    // a 应该被淘汰（LRU 最久未访问）
    send_cmd(&mut stream, &["GET", "a"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::BulkString(None));

    // b 应该还在
    send_cmd(&mut stream, &["GET", "b"]).await;
    let resp = recv_resp(&mut stream).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("vb"))));
}

#[tokio::test]
async fn test_string_tail_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SETEX + GET
    let resp = exec(&mut stream, &["SETEX", "k1", "3600", "v1"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["GET", "k1"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("v1"))));

    // PSETEX + GET
    let resp = exec(&mut stream, &["PSETEX", "k2", "3600000", "v2"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["GET", "k2"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("v2"))));

    // GETSET
    let resp = exec(&mut stream, &["SET", "k3", "old"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["GETSET", "k3", "new"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("old"))));
    let resp = exec(&mut stream, &["GET", "k3"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("new"))));

    // GETDEL
    let resp = exec(&mut stream, &["SET", "k4", "v4"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["GETDEL", "k4"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("v4"))));
    let resp = exec(&mut stream, &["GET", "k4"]).await;
    assert_eq!(resp, RespValue::BulkString(None));

    // GETEX PERSIST
    let resp = exec(&mut stream, &["SETEX", "k5", "3600", "v5"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["GETEX", "k5", "PERSIST"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("v5"))));
    let resp = exec(&mut stream, &["TTL", "k5"]).await;
    assert_eq!(resp, RespValue::Integer(-1));

    // MSETNX
    let resp = exec(&mut stream, &["MSETNX", "m1", "1", "m2", "2"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["MGET", "m1", "m2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("1"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("2"))));
        }
        _ => panic!("期望 MGET 返回数组, 得到 {:?}", resp),
    }
    let resp = exec(&mut stream, &["MSETNX", "m1", "x", "m3", "3"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["GET", "m3"]).await;
    assert_eq!(resp, RespValue::BulkString(None));

    // INCRBYFLOAT
    let resp = exec(&mut stream, &["INCRBYFLOAT", "f", "0.5"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("0.5"))));
    let resp = exec(&mut stream, &["INCRBYFLOAT", "f", "0.25"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("0.75"))));

    // SETRANGE
    let resp = exec(&mut stream, &["SET", "s", "Hello World"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["SETRANGE", "s", "6", "Redis"]).await;
    assert_eq!(resp, RespValue::Integer(11));
    let resp = exec(&mut stream, &["GET", "s"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("Hello Redis"))));
}

#[tokio::test]
async fn test_list_tail_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // LSET
    let resp = exec(&mut stream, &["RPUSH", "list", "a", "b", "c"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["LSET", "list", "1", "x"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["LINDEX", "list", "1"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("x"))));

    // LINSERT
    let resp = exec(&mut stream, &["LINSERT", "list", "BEFORE", "x", "y"]).await;
    assert_eq!(resp, RespValue::Integer(4));
    let resp = exec(&mut stream, &["LINDEX", "list", "1"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("y"))));

    // LREM
    let resp = exec(&mut stream, &["RPUSH", "list2", "a", "b", "a", "c", "a"]).await;
    assert_eq!(resp, RespValue::Integer(5));
    let resp = exec(&mut stream, &["LREM", "list2", "2", "a"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = exec(&mut stream, &["LLEN", "list2"]).await;
    assert_eq!(resp, RespValue::Integer(3));

    // LTRIM
    let resp = exec(&mut stream, &["RPUSH", "list3", "a", "b", "c", "d", "e"]).await;
    assert_eq!(resp, RespValue::Integer(5));
    let resp = exec(&mut stream, &["LTRIM", "list3", "1", "3"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["LLEN", "list3"]).await;
    assert_eq!(resp, RespValue::Integer(3));

    // LPOS
    let resp = exec(&mut stream, &["RPUSH", "list4", "a", "b", "a", "c"]).await;
    assert_eq!(resp, RespValue::Integer(4));
    let resp = exec(&mut stream, &["LPOS", "list4", "a"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["LPOS", "list4", "a", "RANK", "2"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = exec(&mut stream, &["LPOS", "list4", "a", "COUNT", "2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::Integer(0));
            assert_eq!(arr[1], RespValue::Integer(2));
        }
        _ => panic!("期望 LPOS COUNT 返回数组, 得到 {:?}", resp),
    }

    // BLPOP immediate
    let resp = exec(&mut stream, &["RPUSH", "blist", "a", "b"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = exec(&mut stream, &["BLPOP", "blist", "1"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(bytes::Bytes::from("blist"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(bytes::Bytes::from("a"))));
        }
        _ => panic!("期望 BLPOP 返回数组, 得到 {:?}", resp),
    }

    // BLPOP timeout
    let resp = exec(&mut stream, &["BLPOP", "nobody", "0.1"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

#[tokio::test]
async fn test_hash_tail_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // HINCRBY
    let resp = exec(&mut stream, &["HINCRBY", "h", "f", "5"]).await;
    assert_eq!(resp, RespValue::Integer(5));
    let resp = exec(&mut stream, &["HINCRBY", "h", "f", "3"]).await;
    assert_eq!(resp, RespValue::Integer(8));

    // HINCRBYFLOAT
    let resp = exec(&mut stream, &["HINCRBYFLOAT", "h", "g", "0.5"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("0.5"))));

    // HKEYS / HVALS
    let resp = exec(&mut stream, &["HSET", "h2", "a", "1", "b", "2"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = exec(&mut stream, &["HKEYS", "h2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("期望 HKEYS 返回数组, 得到 {:?}", resp),
    }
    let resp = exec(&mut stream, &["HVALS", "h2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("期望 HVALS 返回数组, 得到 {:?}", resp),
    }

    // HSETNX
    let resp = exec(&mut stream, &["HSETNX", "h3", "f", "v1"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["HSETNX", "h3", "f", "v2"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["HGET", "h3", "f"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(bytes::Bytes::from("v1"))));

    // HSCAN
    let resp = exec(&mut stream, &["HSET", "h4", "a", "1", "b", "2", "c", "3"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["HSCAN", "h4", "0", "COUNT", "2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2); // cursor + fields array
            match &arr[1] {
                RespValue::Array(fields) => {
                    assert_eq!(fields.len(), 4); // 2 fields * 2
                }
                _ => panic!("期望 HSCAN 返回字段数组"),
            }
        }
        _ => panic!("期望 HSCAN 返回数组, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_set_tail_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SPOP
    let resp = exec(&mut stream, &["SADD", "s1", "a", "b", "c"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["SPOP", "s1", "2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("期望 SPOP 返回数组, 得到 {:?}", resp),
    }
    let resp = exec(&mut stream, &["SCARD", "s1"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // SRANDMEMBER
    let resp = exec(&mut stream, &["SADD", "s2", "a", "b", "c"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["SRANDMEMBER", "s2", "2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("期望 SRANDMEMBER 返回数组, 得到 {:?}", resp),
    }
    let resp = exec(&mut stream, &["SCARD", "s2"]).await;
    assert_eq!(resp, RespValue::Integer(3));

    // SMOVE
    let resp = exec(&mut stream, &["SADD", "s3", "a", "b"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = exec(&mut stream, &["SADD", "s4", "c"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["SMOVE", "s3", "s4", "a"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["SISMEMBER", "s4", "a"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // SINTERSTORE
    let resp = exec(&mut stream, &["SADD", "s5", "a", "b", "c"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["SADD", "s6", "b", "c", "d"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["SINTERSTORE", "dest", "s5", "s6"]).await;
    assert_eq!(resp, RespValue::Integer(2));

    // SUNIONSTORE
    let resp = exec(&mut stream, &["SUNIONSTORE", "dest2", "s5", "s6"]).await;
    assert_eq!(resp, RespValue::Integer(4));

    // SDIFFSTORE
    let resp = exec(&mut stream, &["SDIFFSTORE", "dest3", "s5", "s6"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // SSCAN
    let resp = exec(&mut stream, &["SADD", "s7", "a", "b", "c"]).await;
    assert_eq!(resp, RespValue::Integer(3));
    let resp = exec(&mut stream, &["SSCAN", "s7", "0", "COUNT", "2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[1] {
                RespValue::Array(members) => {
                    assert_eq!(members.len(), 2);
                }
                _ => panic!("期望 SSCAN 返回成员数组"),
            }
        }
        _ => panic!("期望 SSCAN 返回数组, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_zset_tail_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // ZADD 准备数据
    let resp = exec(&mut stream, &["ZADD", "z1", "10", "a", "20", "b", "30", "c"]).await;
    assert_eq!(resp, RespValue::Integer(3));

    // ZREVRANGE
    let resp = exec(&mut stream, &["ZREVRANGE", "z1", "0", "1"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("c"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("b"))));
        }
        _ => panic!("期望 ZREVRANGE 返回数组, 得到 {:?}", resp),
    }

    // ZREVRANK
    let resp = exec(&mut stream, &["ZREVRANK", "z1", "a"]).await;
    assert_eq!(resp, RespValue::Integer(2));

    // ZINCRBY
    let resp = exec(&mut stream, &["ZINCRBY", "z1", "5.5", "a"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("15.5"))));

    // ZCOUNT
    let resp = exec(&mut stream, &["ZCOUNT", "z1", "16", "25"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // ZPOPMIN
    let resp = exec(&mut stream, &["ZPOPMIN", "z1", "1"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("a"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("15.5"))));
        }
        _ => panic!("期望 ZPOPMIN 返回数组, 得到 {:?}", resp),
    }

    // ZPOPMAX
    let resp = exec(&mut stream, &["ZPOPMAX", "z1", "1"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("c"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("30"))));
        }
        _ => panic!("期望 ZPOPMAX 返回数组, 得到 {:?}", resp),
    }

    // ZUNIONSTORE
    let resp = exec(&mut stream, &["ZADD", "z2", "2", "b", "3", "d"]).await;
    assert_eq!(resp, RespValue::Integer(2));
    let resp = exec(&mut stream, &["ZUNIONSTORE", "z3", "2", "z1", "z2", "AGGREGATE", "SUM"]).await;
    assert_eq!(resp, RespValue::Integer(2));

    // ZINTERSTORE
    let resp = exec(&mut stream, &["ZINTERSTORE", "z4", "2", "z1", "z2"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // ZSCAN
    let resp = exec(&mut stream, &["ZSCAN", "z1", "0", "COUNT", "10"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
        }
        _ => panic!("期望 ZSCAN 返回数组, 得到 {:?}", resp),
    }

    // ZRANGEBYLEX
    let resp = exec(&mut stream, &["ZADD", "z5", "0", "a", "0", "b", "0", "c", "0", "d"]).await;
    assert_eq!(resp, RespValue::Integer(4));
    let resp = exec(&mut stream, &["ZRANGEBYLEX", "z5", "[b", "[c"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("b"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("c"))));
        }
        _ => panic!("期望 ZRANGEBYLEX 返回数组, 得到 {:?}", resp),
    }
}

#[tokio::test]
async fn test_bitmap_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SETBIT / GETBIT
    let resp = exec(&mut stream, &["SETBIT", "k", "7", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["GETBIT", "k", "7"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["GETBIT", "k", "6"]).await;
    assert_eq!(resp, RespValue::Integer(0));

    // BITCOUNT
    let resp = exec(&mut stream, &["BITCOUNT", "k"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // BITOP AND (用 SETBIT 构造位图)
    let resp = exec(&mut stream, &["SETBIT", "a", "0", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "a", "1", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "a", "2", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "a", "3", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    // a = 0b11110000
    let resp = exec(&mut stream, &["SETBIT", "b", "4", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "b", "5", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "b", "6", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "b", "7", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    // b = 0b00001111
    let resp = exec(&mut stream, &["BITOP", "AND", "dest", "a", "b"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["GETBIT", "dest", "0"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["GETBIT", "dest", "7"]).await;
    assert_eq!(resp, RespValue::Integer(0));

    // BITPOS (用 SETBIT 构造 0b10101010)
    let resp = exec(&mut stream, &["SETBIT", "k2", "0", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "k2", "2", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "k2", "4", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["SETBIT", "k2", "6", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["BITPOS", "k2", "1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
    let resp = exec(&mut stream, &["BITPOS", "k2", "0"]).await;
    assert_eq!(resp, RespValue::Integer(1));
}

#[tokio::test]
async fn test_hll_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // PFADD
    let resp = exec(&mut stream, &["PFADD", "hll", "a", "b", "c"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // PFCOUNT
    let resp = exec(&mut stream, &["PFCOUNT", "hll"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n >= 3),
        other => panic!("期望 Integer，得到 {:?}", other),
    }

    // PFADD 重复元素
    let resp = exec(&mut stream, &["PFADD", "hll", "a", "b"]).await;
    assert_eq!(resp, RespValue::Integer(0));

    // PFMERGE
    let resp = exec(&mut stream, &["PFADD", "hll2", "c", "d", "e"]).await;
    assert_eq!(resp, RespValue::Integer(1));
    let resp = exec(&mut stream, &["PFMERGE", "merged", "hll", "hll2"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["PFCOUNT", "merged"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n >= 5),
        other => panic!("期望 Integer，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_geo_commands_tcp() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // GEOADD
    let resp = exec(&mut stream, &["GEOADD", "cities", "116.4074", "39.9042", "北京", "121.4737", "31.2304", "上海"]).await;
    assert_eq!(resp, RespValue::Integer(2));

    // GEODIST
    let resp = exec(&mut stream, &["GEODIST", "cities", "北京", "上海", "km"]).await;
    match resp {
        RespValue::BulkString(Some(b)) => {
            let dist: f64 = String::from_utf8_lossy(&b).parse().unwrap();
            assert!((dist - 1067.0).abs() < 20.0);
        }
        other => panic!("期望 BulkString，得到 {:?}", other),
    }

    // GEOHASH
    let resp = exec(&mut stream, &["GEOHASH", "cities", "北京"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // GEOPOS
    let resp = exec(&mut stream, &["GEOPOS", "cities", "北京"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // GEOSEARCH BYRADIUS
    let resp = exec(&mut stream, &["GEOSEARCH", "cities", "FROMLONLAT", "116.4074", "39.9042", "BYRADIUS", "500", "km", "ASC"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty());
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // GEOSEARCHSTORE
    let resp = exec(&mut stream, &["GEOSEARCHSTORE", "near", "cities", "FROMLONLAT", "116.4074", "39.9042", "BYRADIUS", "2000", "km", "ASC"]).await;
    assert_eq!(resp, RespValue::Integer(2));
}


#[tokio::test]
async fn test_select_db_isolation() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 在 db 0 设置 key
    let resp = exec(&mut stream, &["SET", "mykey", "db0value"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 切换到 db 1
    let resp = exec(&mut stream, &["SELECT", "1"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // db 1 中不应该有 mykey
    let resp = exec(&mut stream, &["GET", "mykey"]).await;
    assert_eq!(resp, RespValue::BulkString(None));

    // 在 db 1 设置同名 key
    let resp = exec(&mut stream, &["SET", "mykey", "db1value"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", "mykey"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("db1value"))));

    // 切换回 db 0，验证数据隔离
    let resp = exec(&mut stream, &["SELECT", "0"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", "mykey"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("db0value"))));
}

#[tokio::test]
async fn test_auth_required() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), Some("secret".to_string()));
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 未认证时 GET 应该失败
    let resp = exec(&mut stream, &["GET", "key"]).await;
    assert_eq!(resp, RespValue::Error("NOAUTH Authentication required.".to_string()));

    // 未认证时 PING 应该成功
    let resp = exec(&mut stream, &["PING"]).await;
    assert_eq!(resp, RespValue::SimpleString("PONG".to_string()));

    // 错误密码
    let resp = exec(&mut stream, &["AUTH", "wrong"]).await;
    assert_eq!(resp, RespValue::Error("ERR invalid password".to_string()));

    // 正确密码
    let resp = exec(&mut stream, &["AUTH", "secret"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 认证后 GET 应该成功
    let resp = exec(&mut stream, &["SET", "key", "value"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["GET", "key"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("value"))));
}

#[tokio::test]
async fn test_client_commands() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // CLIENT ID 应该返回正整数
    let resp = exec(&mut stream, &["CLIENT", "ID"]).await;
    match resp {
        RespValue::Integer(id) => assert!(id > 0),
        other => panic!("期望 Integer，得到 {:?}", other),
    }

    // CLIENT SETNAME
    let resp = exec(&mut stream, &["CLIENT", "SETNAME", "my-connection"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // CLIENT GETNAME
    let resp = exec(&mut stream, &["CLIENT", "GETNAME"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("my-connection"))));

    // CLIENT LIST 应该包含当前连接
    let resp = exec(&mut stream, &["CLIENT", "LIST"]).await;
    match resp {
        RespValue::BulkString(Some(b)) => {
            let list = String::from_utf8_lossy(&b);
            assert!(list.contains("my-connection"));
            assert!(list.contains("id="));
        }
        other => panic!("期望 BulkString，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_quit_command() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // QUIT 应该返回 OK
    let resp = exec(&mut stream, &["QUIT"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
}

// ---------- 阶段 46 测试 ----------

#[tokio::test]
async fn test_memory_usage_doctor() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // MEMORY USAGE 不存在的 key
    let resp = exec(&mut stream, &["MEMORY", "USAGE", "nonexist"]).await;
    assert_eq!(resp, RespValue::BulkString(None));

    // 设置 key 后查询
    exec(&mut stream, &["SET", "mykey", "hello_world"]).await;
    let resp = exec(&mut stream, &["MEMORY", "USAGE", "mykey"]).await;
    match resp {
        RespValue::Integer(size) => assert!(size > 0),
        other => panic!("期望 Integer，得到 {:?}", other),
    }

    // MEMORY DOCTOR
    let resp = exec(&mut stream, &["MEMORY", "DOCTOR"]).await;
    match resp {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("redis-rust"));
            assert!(s.contains("内存诊断报告"));
        }
        other => panic!("期望 BulkString，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_latency_latest_history_reset() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 执行一些命令产生延迟记录
    for i in 0..5 {
        exec(&mut stream, &["SET", &format!("k{}", i), "v"]).await;
    }

    // LATENCY LATEST
    let resp = exec(&mut stream, &["LATENCY", "LATEST"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "LATENCY LATEST 应返回非空数组");
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // LATENCY HISTORY command
    let resp = exec(&mut stream, &["LATENCY", "HISTORY", "command"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty(), "LATENCY HISTORY 应返回非空数组");
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // LATENCY RESET
    let resp = exec(&mut stream, &["LATENCY", "RESET"]).await;
    match resp {
        RespValue::Integer(n) => assert!(n >= 0),
        other => panic!("期望 Integer，得到 {:?}", other),
    }

    // 重置后再查应为空或仅剩框架
    let resp = exec(&mut stream, &["LATENCY", "LATEST"]).await;
    match resp {
        RespValue::Array(arr) => {
            // 重置后应为空
            assert!(arr.is_empty(), "LATENCY LATEST 重置后应为空");
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_config_rewrite_resetstat() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // CONFIG REWRITE
    let resp = exec(&mut stream, &["CONFIG", "REWRITE"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // CONFIG RESETSTAT
    let resp = exec(&mut stream, &["CONFIG", "RESETSTAT"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
}

#[tokio::test]
async fn test_reset_command() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), Some("secret".to_string()));
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 认证
    let resp = exec(&mut stream, &["AUTH", "secret"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 设置客户端名称
    exec(&mut stream, &["CLIENT", "SETNAME", "test-client"]).await;

    // 选择数据库
    exec(&mut stream, &["SELECT", "1"]).await;

    // RESET
    let resp = exec(&mut stream, &["RESET"]).await;
    assert_eq!(resp, RespValue::SimpleString("RESET".to_string()));

    // RESET 后认证应被清除，需要重新认证
    let resp = exec(&mut stream, &["AUTH", "secret"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // RESET 后客户端名称应被清除
    let resp = exec(&mut stream, &["CLIENT", "GETNAME"]).await;
    assert_eq!(resp, RespValue::BulkString(None));
}

#[tokio::test]
async fn test_hello_command() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // HELLO 2
    let resp = exec(&mut stream, &["HELLO", "2"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty());
            // 检查是否包含 server 字段
            let has_server = arr.iter().any(|v| {
                if let RespValue::BulkString(Some(b)) = v {
                    &b[..] == b"server"
                } else {
                    false
                }
            });
            assert!(has_server, "HELLO 返回应包含 server 字段");
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // HELLO 2 SETNAME myconn
    let resp = exec(&mut stream, &["HELLO", "2", "SETNAME", "myconn"]).await;
    match resp {
        RespValue::Array(_) => {}
        other => panic!("期望 Array，得到 {:?}", other),
    }
    let resp = exec(&mut stream, &["CLIENT", "GETNAME"]).await;
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("myconn"))));
}

#[tokio::test]
async fn test_monitor_command() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    // monitor 客户端
    let mut monitor_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut monitor_stream, &["MONITOR"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 另一个客户端执行命令
    let mut cmd_stream = TcpStream::connect(addr).await.unwrap();
    exec(&mut cmd_stream, &["SET", "mon_key", "mon_val"]).await;

    // monitor 客户端应收到广播消息
    // 使用 recv_resp 读取下一条消息（MONITOR 模式下的实时推送）
    let parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(4096);
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        async {
            loop {
                match parser.parse(&mut buf).unwrap() {
                    Some(resp) => return resp,
                    None => {
                        let mut tmp = [0u8; 1024];
                        let n = monitor_stream.read(&mut tmp).await.unwrap();
                        assert!(n > 0, "monitor 连接已关闭");
                        buf.extend_from_slice(&tmp[..n]);
                    }
                }
            }
        }
    ).await.expect("monitor 等待超时");

    match resp {
        RespValue::SimpleString(s) => {
            assert!(s.contains("SET"), "MONITOR 消息应包含 SET 命令: {}", s);
            assert!(s.contains("mon_key"), "MONITOR 消息应包含 key: {}", s);
        }
        other => panic!("期望 SimpleString，得到 {:?}", other),
    }
}

// ---------- Keyspace 通知测试 ----------

#[tokio::test]
async fn test_config_keyspace_events() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 默认应为空
    let resp = exec(&mut stream, &["CONFIG", "GET", "notify-keyspace-events"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from(""))));
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // 设置配置
    let resp = exec(&mut stream, &["CONFIG", "SET", "notify-keyspace-events", "KEA"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 验证配置已生效
    let resp = exec(&mut stream, &["CONFIG", "GET", "notify-keyspace-events"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("KEA"))));
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_config_aof_use_rdb_preamble() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 默认应为 no
    let resp = exec(&mut stream, &["CONFIG", "GET", "aof-use-rdb-preamble"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("no"))));
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }

    // 设置为 yes
    let resp = exec(&mut stream, &["CONFIG", "SET", "aof-use-rdb-preamble", "yes"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["CONFIG", "GET", "aof-use-rdb-preamble"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("yes"))));
        }
        other => panic!("期望 Array，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_keyspace_notification_set() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    // 设置 notify-keyspace-events
    let mut config_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut config_stream, &["CONFIG", "SET", "notify-keyspace-events", "KE$"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 订阅 keyspace 频道
    let mut sub_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut sub_stream, &["SUBSCRIBE", "__keyspace@0__:mykey"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("subscribe"))));
        }
        other => panic!("期望 subscribe 响应，得到 {:?}", other),
    }

    // 另一个客户端执行 SET
    let mut cmd_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut cmd_stream, &["SET", "mykey", "myval"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 订阅客户端应收到消息
    let parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(4096);
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        async {
            loop {
                match parser.parse(&mut buf).unwrap() {
                    Some(resp) => return resp,
                    None => {
                        let mut tmp = [0u8; 1024];
                        let n = sub_stream.read(&mut tmp).await.unwrap();
                        assert!(n > 0, "订阅连接已关闭");
                        buf.extend_from_slice(&tmp[..n]);
                    }
                }
            }
        }
    ).await.expect("等待 keyspace 通知超时");

    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("message"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("__keyspace@0__:mykey"))));
            assert_eq!(arr[2], RespValue::BulkString(Some(Bytes::from("set"))));
        }
        other => panic!("期望 message 数组，得到 {:?}", other),
    }
}

#[tokio::test]
async fn test_keyspace_notification_keyevent() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();

    // 设置 notify-keyspace-events（仅 keyevent）
    let mut config_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut config_stream, &["CONFIG", "SET", "notify-keyspace-events", "E$"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 订阅 keyevent 频道
    let mut sub_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut sub_stream, &["SUBSCRIBE", "__keyevent@0__:set"]).await;
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("subscribe"))));
        }
        other => panic!("期望 subscribe 响应，得到 {:?}", other),
    }

    // 另一个客户端执行 SET
    let mut cmd_stream = TcpStream::connect(addr).await.unwrap();
    let resp = exec(&mut cmd_stream, &["SET", "testkey", "testval"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 订阅客户端应收到消息
    let parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(4096);
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        async {
            loop {
                match parser.parse(&mut buf).unwrap() {
                    Some(resp) => return resp,
                    None => {
                        let mut tmp = [0u8; 1024];
                        let n = sub_stream.read(&mut tmp).await.unwrap();
                        assert!(n > 0, "订阅连接已关闭");
                        buf.extend_from_slice(&tmp[..n]);
                    }
                }
            }
        }
    ).await.expect("等待 keyevent 通知超时");

    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("message"))));
            assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("__keyevent@0__:set"))));
            assert_eq!(arr[2], RespValue::BulkString(Some(Bytes::from("testkey"))));
        }
        other => panic!("期望 message 数组，得到 {:?}", other),
    }
}

// ---------- 混合持久化测试 ----------

#[tokio::test]
async fn test_aof_rdb_preamble_integration() {
    use std::sync::{Arc, Mutex};
    use redis_rust::aof::{AofWriter, AofReplayer};

    let path = "/tmp/test_aof_rdb_preamble_integration.aof";
    let _ = std::fs::remove_file(path);

    let storage = StorageEngine::new();
    let aof = Arc::new(Mutex::new(AofWriter::new(path).unwrap()));
    let server = Server::new("127.0.0.1:0", storage.clone(), Some(aof.clone()), PubSubManager::new(), None);
    let (addr, handle) = server.start().await.unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 写入数据
    let resp = exec(&mut stream, &["SET", "mix_key1", "val1"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));
    let resp = exec(&mut stream, &["LPUSH", "mix_list", "a", "b"]).await;
    assert_eq!(resp, RespValue::Integer(2));

    // 启用混合持久化并重写
    let resp = exec(&mut stream, &["CONFIG", "SET", "aof-use-rdb-preamble", "yes"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["BGREWRITEAOF"]).await;
    assert!(matches!(resp, RespValue::SimpleString(_)), "BGREWRITEAOF 应返回 SimpleString");

    // 关闭服务器
    drop(stream);
    drop(handle);

    // 验证文件为混合格式
    let content = std::fs::read(path).unwrap();
    assert!(content.starts_with(b"REDIS-RUST-AOF-PREAMBLE\n"), "AOF 文件应以 RDB preamble 开头");

    // 用新 storage 重放
    let new_storage = StorageEngine::new();
    AofReplayer::replay(path, new_storage.clone()).unwrap();

    assert_eq!(new_storage.get("mix_key1").unwrap(), Some(Bytes::from("val1")));
    let list = new_storage.lrange("mix_list", 0, -1).unwrap();
    assert_eq!(list.len(), 2);

    let _ = std::fs::remove_file(path);
}

// ---------- Stream 消费者组测试 ----------

#[tokio::test]
async fn test_xgroup_create_and_xreadgroup() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 创建 stream 并添加数据
    exec(&mut stream, &["XADD", "mystream", "1-0", "name", "alice"]).await;
    exec(&mut stream, &["XADD", "mystream", "2-0", "name", "bob"]).await;

    // 创建消费者组，从头开始读
    let resp = exec(&mut stream, &["XGROUP", "CREATE", "mystream", "grp1", "0"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // XREADGROUP 读取
    let resp = exec(&mut stream, &["XREADGROUP", "GROUP", "grp1", "consumer1", "COUNT", "10", "STREAMS", "mystream", ">"]).await;
    if let RespValue::Array(streams) = &resp {
        assert_eq!(streams.len(), 1);
        if let RespValue::Array(inner) = &streams[0] {
            if let RespValue::Array(entries) = &inner[1] {
                assert_eq!(entries.len(), 2);
            } else { panic!("期望 entries 数组"); }
        } else { panic!("期望 stream 数组"); }
    } else { panic!("期望数组响应"); }

    // 再次读取应该没有新消息
    let resp = exec(&mut stream, &["XREADGROUP", "GROUP", "grp1", "consumer1", "STREAMS", "mystream", ">"]).await;
    if let RespValue::Array(streams) = &resp {
        if streams.is_empty() {
            // 没有新消息，返回空数组
        } else if let RespValue::Array(inner) = &streams[0] {
            if let RespValue::Array(entries) = &inner[1] {
                assert_eq!(entries.len(), 0);
            } else { panic!("期望 entries 数组"); }
        } else { panic!("期望 stream 数组"); }
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xack() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s1", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s1", "g1", "0"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s1", ">"]).await;

    // ACK
    let resp = exec(&mut stream, &["XACK", "s1", "g1", "1-0"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // 重复 ACK 返回 0
    let resp = exec(&mut stream, &["XACK", "s1", "g1", "1-0"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_xpending_summary() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s2", "1-0", "a", "1"]).await;
    exec(&mut stream, &["XADD", "s2", "2-0", "b", "2"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s2", "g1", "0"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s2", ">"]).await;

    // XPENDING 摘要模式
    let resp = exec(&mut stream, &["XPENDING", "s2", "g1"]).await;
    if let RespValue::Array(arr) = &resp {
        assert_eq!(arr[0], RespValue::Integer(2));
    } else { panic!("期望数组响应"); }

    // XPENDING 详细模式
    let resp = exec(&mut stream, &["XPENDING", "s2", "g1", "-", "+", "10"]).await;
    if let RespValue::Array(arr) = &resp {
        assert_eq!(arr.len(), 2);
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xgroup_destroy_and_delconsumer() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s3", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s3", "g1", "0"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s3", ">"]).await;

    // DELCONSUMER 返回该消费者的 pending 数
    let resp = exec(&mut stream, &["XGROUP", "DELCONSUMER", "s3", "g1", "c1"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // DESTROY 返回 1
    let resp = exec(&mut stream, &["XGROUP", "DESTROY", "s3", "g1"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // 再次 DESTROY 返回 0
    let resp = exec(&mut stream, &["XGROUP", "DESTROY", "s3", "g1"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_xgroup_createconsumer() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s4", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s4", "g1", "0"]).await;

    let resp = exec(&mut stream, &["XGROUP", "CREATECONSUMER", "s4", "g1", "newc"]).await;
    assert_eq!(resp, RespValue::Integer(1));

    // 重复创建返回 0
    let resp = exec(&mut stream, &["XGROUP", "CREATECONSUMER", "s4", "g1", "newc"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}

#[tokio::test]
async fn test_xgroup_setid() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s5", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XADD", "s5", "2-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s5", "g1", "$"]).await;

    // SETID 回到 0，这样可以重新读取
    let resp = exec(&mut stream, &["XGROUP", "SETID", "s5", "g1", "0"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    let resp = exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s5", ">"]).await;
    if let RespValue::Array(streams) = &resp {
        if let RespValue::Array(inner) = &streams[0] {
            if let RespValue::Array(entries) = &inner[1] {
                assert_eq!(entries.len(), 2);
            } else { panic!("期望 entries 数组"); }
        } else { panic!("期望 stream 数组"); }
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xclaim() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s6", "1-0", "k", "v1"]).await;
    exec(&mut stream, &["XADD", "s6", "2-0", "k", "v2"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s6", "g1", "0"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s6", ">"]).await;

    // XCLAIM min-idle=0 把消息转给 c2
    let resp = exec(&mut stream, &["XCLAIM", "s6", "g1", "c2", "0", "1-0"]).await;
    if let RespValue::Array(arr) = &resp {
        assert_eq!(arr.len(), 1);
    } else { panic!("期望数组响应"); }

    // XCLAIM JUSTID
    let resp = exec(&mut stream, &["XCLAIM", "s6", "g1", "c2", "0", "2-0", "JUSTID"]).await;
    if let RespValue::Array(arr) = &resp {
        assert_eq!(arr.len(), 1);
        if let RespValue::BulkString(Some(id)) = &arr[0] {
            assert_eq!(id.as_ref(), b"2-0");
        } else { panic!("期望 BulkString"); }
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xautoclaim() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s7", "1-0", "k", "v1"]).await;
    exec(&mut stream, &["XADD", "s7", "2-0", "k", "v2"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s7", "g1", "0"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s7", ">"]).await;

    // XAUTOCLAIM min-idle=0
    let resp = exec(&mut stream, &["XAUTOCLAIM", "s7", "g1", "c2", "0", "0-0", "COUNT", "10"]).await;
    if let RespValue::Array(arr) = &resp {
        assert_eq!(arr.len(), 3); // next-id, entries, deleted-ids
        if let RespValue::Array(entries) = &arr[1] {
            assert_eq!(entries.len(), 2);
        } else { panic!("期望 entries 数组"); }
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xinfo_stream() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s8", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s8", "g1", "0"]).await;

    let resp = exec(&mut stream, &["XINFO", "STREAM", "s8"]).await;
    if let RespValue::Array(arr) = &resp {
        assert!(arr.len() >= 4);
        assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from("length"))));
        assert_eq!(arr[1], RespValue::Integer(1));
        assert_eq!(arr[2], RespValue::BulkString(Some(Bytes::from("groups"))));
        assert_eq!(arr[3], RespValue::Integer(1));
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xinfo_groups() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s9", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s9", "g1", "0"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s9", "g2", "0"]).await;

    let resp = exec(&mut stream, &["XINFO", "GROUPS", "s9"]).await;
    if let RespValue::Array(arr) = &resp {
        assert_eq!(arr.len(), 2);
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xinfo_consumers() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    exec(&mut stream, &["XADD", "s10", "1-0", "k", "v"]).await;
    exec(&mut stream, &["XGROUP", "CREATE", "s10", "g1", "0"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s10", ">"]).await;
    exec(&mut stream, &["XREADGROUP", "GROUP", "g1", "c2", "STREAMS", "s10", ">"]).await;

    let resp = exec(&mut stream, &["XINFO", "CONSUMERS", "s10", "g1"]).await;
    if let RespValue::Array(arr) = &resp {
        assert!(arr.len() >= 1); // 至少 c1 有 pending
    } else { panic!("期望数组响应"); }
}

#[tokio::test]
async fn test_xgroup_create_mkstream() {
    let storage = StorageEngine::new();
    let server = Server::new("127.0.0.1:0", storage, None, PubSubManager::new(), None);
    let (addr, _handle) = server.start().await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 不存在的 key 使用 MKSTREAM 创建
    let resp = exec(&mut stream, &["XGROUP", "CREATE", "newstream", "g1", "$", "MKSTREAM"]).await;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // 验证 stream 已创建
    let resp = exec(&mut stream, &["XLEN", "newstream"]).await;
    assert_eq!(resp, RespValue::Integer(0));
}
