//! Sentinel 间通信与发现模块

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{SentinelManager, SentinelPeer};

/// 启动 Sentinel 发现任务
/// 每 2 秒向所有被监控 master 的 __sentinel__:hello 频道发布自身信息
/// 同时订阅该频道获取其他 Sentinel 信息
pub fn start_discovery(sentinel: Arc<SentinelManager>, listen_port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));

        loop {
            interval.tick().await;

            let masters = sentinel.get_masters();
            let epoch = sentinel.get_current_epoch();
            for master in &masters {
                let ip = master.ip.clone();
                let port = master.port;
                let name = master.name.clone();
                let runid = sentinel.runid.clone();

                // 发布 hello 消息到 master 的 __sentinel__:hello 频道
                // 格式: "ip,port,runid,epoch,master_name"
                let hello_msg = format!(
                    "{},{},{},{},{}",
                    "127.0.0.1", listen_port, runid, epoch, name
                );

                if let Err(e) = publish_hello(&ip, port, &hello_msg).await {
                    log::debug!("发布 sentinel hello 到 {}:{} 失败: {}", ip, port, e);
                }
            }
        }
    })
}

/// 启动订阅 __sentinel__:hello 频道的任务
/// 从频道消息中发现其他 Sentinel
pub fn start_hello_subscriber(sentinel: Arc<SentinelManager>, master_ip: String, master_port: u16, master_name: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match subscribe_hello(&master_ip, master_port).await {
                Ok(mut reader) => {
                    loop {
                        let mut line = String::new();
                        match tokio::time::timeout(
                            Duration::from_secs(30),
                            reader.read_line(&mut line),
                        ).await {
                            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                            Ok(Ok(_)) => {
                                // 解析 hello 消息
                                let trimmed = line.trim();
                                if let Some(peer) = parse_hello_message(trimmed)
                                    && peer.runid != sentinel.runid {
                                        sentinel.update_sentinel_peer(&master_name, peer);
                                    }
                            }
                        }
                    }
                }
                Err(e) => {
                    log::debug!("订阅 __sentinel__:hello 失败 ({}:{}): {}", master_ip, master_port, e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    })
}

/// 向 master 发布 hello 消息
async fn publish_hello(ip: &str, port: u16, message: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, port);
    let timeout = Duration::from_millis(500);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();
    
    let channel = "__sentinel__:hello";
    let cmd = format!(
        "*3\r\n$7\r\nPUBLISH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        channel.len(), channel,
        message.len(), message
    );
    write_half.write_all(cmd.as_bytes()).await?;
    
    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;
    
    Ok(())
}

/// 订阅 master 的 __sentinel__:hello 频道
async fn subscribe_hello(ip: &str, port: u16) -> Result<tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, port);
    let stream = TcpStream::connect(&addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    
    let channel = "__sentinel__:hello";
    let cmd = format!(
        "*2\r\n$9\r\nSUBSCRIBE\r\n${}\r\n{}\r\n",
        channel.len(), channel
    );
    write_half.write_all(cmd.as_bytes()).await?;
    
    let mut reader = tokio::io::BufReader::new(read_half);
    // 读取订阅确认响应（跳过）
    for _ in 0..3 {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
    }
    
    Ok(reader)
}

/// 解析 hello 消息: "ip,port,runid,epoch,master_name"
fn parse_hello_message(msg: &str) -> Option<SentinelPeer> {
    let parts: Vec<&str> = msg.split(',').collect();
    if parts.len() < 5 {
        return None;
    }
    let ip = parts[0].to_string();
    let port: u16 = parts[1].parse().ok()?;
    let runid = parts[2].to_string();
    // parts[3] 是 epoch，当前不严格使用
    let _epoch: u64 = parts[3].parse().unwrap_or(0);
    
    Some(SentinelPeer {
        ip,
        port,
        runid,
        last_hello_time: Some(std::time::Instant::now()),
    })
}
