// Sentinel 监控模块：定期 PING master 并通过 INFO replication 发现 replica

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{SentinelManager, ReplicaInstance};

/// 启动 master 监控任务
pub fn start_monitor(sentinel: Arc<SentinelManager>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut info_interval_counter: u64 = 0;
        
        loop {
            interval.tick().await;
            info_interval_counter += 1;
            
            let masters = sentinel.get_masters();
            for master in &masters {
                let ip = master.ip.clone();
                let port = master.port;
                let name = master.name.clone();
                
                // 每秒发送 PING
                match ping_instance(&ip, port).await {
                    Ok(true) => {
                        sentinel.update_last_ping_reply(&name);
                    }
                    Ok(false) | Err(_) => {
                        log::debug!("PING {}:{} 失败", ip, port);
                    }
                }
                
                // 每 10 秒发送 INFO replication 发现 replica
                if info_interval_counter % 10 == 0 {
                    match get_info_replication(&ip, port).await {
                        Ok(info) => {
                            let replicas = parse_replicas_from_info(&info);
                            sentinel.update_replicas(&name, replicas);
                        }
                        Err(e) => {
                            log::debug!("INFO replication {}:{} 失败: {}", ip, port, e);
                        }
                    }
                }
            }
            
            // 检查 SDOWN 状态
            sentinel.check_sdown();
        }
    })
}

/// 向实例发送 PING，返回是否收到 PONG
async fn ping_instance(ip: &str, port: u16) -> Result<bool, Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", ip, port);
    let timeout = Duration::from_millis(500);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();
    
    write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    
    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;
    
    Ok(line.trim().starts_with("+PONG"))
}

/// 向实例发送 INFO replication，返回响应内容
async fn get_info_replication(ip: &str, port: u16) -> Result<String, Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", ip, port);
    let timeout = Duration::from_secs(2);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();
    
    write_half.write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n").await?;
    
    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    
    // 读取 bulk string 长度行
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;
    if !line.starts_with('$') {
        return Err(format!("期望 bulk string，收到: {}", line).into());
    }
    let len: usize = line.trim()[1..].parse()?;
    
    // 读取内容
    let mut buf = vec![0u8; len + 2]; // +2 for \r\n
    tokio::time::timeout(timeout, reader.read_exact(&mut buf)).await??;
    
    Ok(String::from_utf8_lossy(&buf[..len]).to_string())
}

/// 从 INFO replication 输出中解析 replica 列表
fn parse_replicas_from_info(info: &str) -> Vec<ReplicaInstance> {
    let mut replicas = Vec::new();
    for line in info.lines() {
        // 格式: slave0:ip=127.0.0.1,port=6380,state=online,offset=123,lag=0
        if !line.starts_with("slave") || !line.contains(':') {
            continue;
        }
        let parts_str = line.split(':').nth(1).unwrap_or("");
        let mut ip = String::new();
        let mut port: u16 = 0;
        let mut offset: i64 = 0;
        
        for part in parts_str.split(',') {
            let kv: Vec<&str> = part.split('=').collect();
            if kv.len() != 2 { continue; }
            match kv[0] {
                "ip" => ip = kv[1].to_string(),
                "port" => port = kv[1].parse().unwrap_or(0),
                "offset" => offset = kv[1].parse().unwrap_or(0),
                _ => {}
            }
        }
        
        if !ip.is_empty() && port > 0 {
            replicas.push(ReplicaInstance {
                ip,
                port,
                offset,
                last_ping_reply: None,
                sdown: false,
            });
        }
    }
    replicas
}
