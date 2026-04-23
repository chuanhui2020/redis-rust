// Sentinel ODOWN 判定与 Leader 选举模块

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::SentinelManager;

/// 启动 ODOWN 检查任务
pub fn start_odown_checker(sentinel: Arc<SentinelManager>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let masters = sentinel.get_masters();
            for master in &masters {
                if !master.sdown {
                    // 不是 SDOWN，清除 ODOWN
                    if master.odown {
                        sentinel.set_odown(&master.name, false);
                    }
                    continue;
                }

                // 已经是 SDOWN，检查是否达到 ODOWN
                let sentinel_count = master.sentinels.len() as u32;
                let total_sentinels = sentinel_count + 1; // 包括自己

                // 向其他 Sentinel 询问 master 是否下线
                let mut agree_count: u32 = 1; // 自己已经判定 SDOWN

                for peer in &master.sentinels {
                    match ask_sentinel_is_down(&peer.ip, peer.port, &master.ip, master.port).await {
                        Ok((true, _, _)) => {
                            agree_count += 1;
                        }
                        Ok((false, _, _)) => {}
                        Err(e) => {
                            log::debug!("询问 Sentinel {}:{} 失败: {}", peer.ip, peer.port, e);
                        }
                    }
                }

                let was_odown = master.odown;
                if agree_count >= master.quorum {
                    if !was_odown {
                        log::warn!(
                            "Sentinel: master {} ({}:{}) 进入客观下线 (ODOWN), 投票 {}/{}，quorum={}",
                            master.name, master.ip, master.port,
                            agree_count, total_sentinels, master.quorum
                        );
                        sentinel.set_odown(&master.name, true);

                        // 尝试成为 leader 并执行故障转移
                        let is_leader = try_become_leader(
                            &sentinel, &master.name, &master.ip, master.port, &master.sentinels,
                        ).await;

                        if is_leader {
                            log::info!(
                                "Sentinel: 当选为 leader，准备对 master {} 执行故障转移",
                                master.name
                            );
                            execute_failover(&sentinel, &master.name).await;
                        }
                    }
                } else if was_odown {
                    sentinel.set_odown(&master.name, false);
                    log::info!(
                        "Sentinel: master {} ({}:{}) 退出客观下线, 投票 {}/{}",
                        master.name, master.ip, master.port,
                        agree_count, total_sentinels
                    );
                }
            }
        }
    })
}

/// 向其他 Sentinel 询问 master 是否下线
/// 返回 (是否下线, leader_runid, leader_epoch)
async fn ask_sentinel_is_down(
    sentinel_ip: &str,
    sentinel_port: u16,
    master_ip: &str,
    master_port: u16,
) -> Result<(bool, String, u64), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", sentinel_ip, sentinel_port);
    let timeout = Duration::from_millis(500);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();

    // 发送 SENTINEL is-master-down-by-addr master_ip master_port 0 *
    let cmd = format!(
        "*6\r\n$8\r\nSENTINEL\r\n$25\r\nis-master-down-by-addr\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n0\r\n$1\r\n*\r\n",
        master_ip.len(),
        master_ip,
        master_port.to_string().len(),
        master_port
    );
    write_half.write_all(cmd.as_bytes()).await?;

    let mut reader = tokio::io::BufReader::new(read_half);

    // 读取 RESP 数组头
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;
    if !line.starts_with('*') {
        return Err(format!("期望 RESP 数组，收到: {}", line).into());
    }
    let array_len: usize = line.trim()[1..].parse()?;

    // 读取 down_state（整数）
    let mut down_line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut down_line)).await??;
    let down_state = down_line.trim().strip_prefix(':').unwrap_or("0").parse::<i64>().unwrap_or(0) == 1;

    // 读取 leader_runid（bulk string）
    let mut runid_line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut runid_line)).await??;
    let leader_runid = if runid_line.starts_with('$') {
        let len: usize = runid_line.trim()[1..].parse()?;
        if len > 0 {
            let mut buf = vec![0u8; len + 2]; // +2 for \r\n
            tokio::time::timeout(timeout, reader.read_exact(&mut buf)).await??;
            String::from_utf8_lossy(&buf[..len]).to_string()
        } else {
            let mut _crlf = [0u8; 2];
            tokio::time::timeout(timeout, reader.read_exact(&mut _crlf)).await??;
            String::new()
        }
    } else {
        String::new()
    };

    // 读取 leader_epoch（整数），如果数组长度 >= 3
    let mut leader_epoch = 0u64;
    if array_len >= 3 {
        let mut epoch_line = String::new();
        tokio::time::timeout(timeout, reader.read_line(&mut epoch_line)).await??;
        leader_epoch = epoch_line.trim().strip_prefix(':').unwrap_or("0").parse::<u64>().unwrap_or(0);
    }

    Ok((down_state, leader_runid, leader_epoch))
}

/// 尝试成为 leader（简化的 Raft-like 选举）
async fn try_become_leader(
    sentinel: &SentinelManager,
    master_name: &str,
    master_ip: &str,
    master_port: u16,
    peers: &[super::SentinelPeer],
) -> bool {
    // 递增 epoch
    let epoch = sentinel.incr_epoch();
    log::info!("Sentinel: 开始 leader 选举，master={}, epoch={}", master_name, epoch);

    if peers.is_empty() {
        // 没有其他 Sentinel，自己就是 leader
        log::info!("Sentinel: 没有其他 Sentinel，直接当选为 leader");
        return true;
    }

    let my_runid = &sentinel.runid;
    let mut votes: u32 = 1; // 投自己一票
    let total = peers.len() as u32 + 1;
    let majority = total / 2 + 1;

    for peer in peers {
        match request_vote(&peer.ip, peer.port, master_ip, master_port, epoch, my_runid).await {
            Ok(true) => {
                votes += 1;
            }
            Ok(false) => {
                log::debug!("Sentinel {}:{} 拒绝投票", peer.ip, peer.port);
            }
            Err(e) => {
                log::debug!("向 Sentinel {}:{} 请求投票失败: {}", peer.ip, peer.port, e);
            }
        }
    }

    let elected = votes >= majority;
    if elected {
        log::info!(
            "Sentinel: leader 选举成功，获得 {}/{} 票（需要 {}），epoch={}",
            votes, total, majority, epoch
        );
    } else {
        log::info!(
            "Sentinel: leader 选举失败，获得 {}/{} 票（需要 {}），epoch={}",
            votes, total, majority, epoch
        );
    }

    elected
}

/// 向其他 Sentinel 请求投票
async fn request_vote(
    sentinel_ip: &str,
    sentinel_port: u16,
    master_ip: &str,
    master_port: u16,
    epoch: u64,
    my_runid: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", sentinel_ip, sentinel_port);
    let timeout = Duration::from_millis(500);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();

    // 发送投票请求：SENTINEL is-master-down-by-addr master_ip master_port epoch runid
    let epoch_str = epoch.to_string();
    let cmd = format!(
        "*6\r\n$8\r\nSENTINEL\r\n$25\r\nis-master-down-by-addr\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        master_ip.len(), master_ip,
        master_port.to_string().len(), master_port,
        epoch_str.len(), epoch_str,
        my_runid.len(), my_runid
    );
    write_half.write_all(cmd.as_bytes()).await?;

    let mut reader = tokio::io::BufReader::new(read_half);

    // 读取 RESP 数组头
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;
    if !line.starts_with('*') {
        return Err(format!("期望 RESP 数组，收到: {}", line).into());
    }
    let array_len: usize = line.trim()[1..].parse()?;

    // 读取 down_state
    let mut down_line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut down_line)).await??;

    // 读取 leader_runid
    let mut runid_line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut runid_line)).await??;
    let voted_runid = if runid_line.starts_with('$') {
        let len: usize = runid_line.trim()[1..].parse()?;
        if len > 0 {
            let mut buf = vec![0u8; len + 2];
            tokio::time::timeout(timeout, reader.read_exact(&mut buf)).await??;
            String::from_utf8_lossy(&buf[..len]).to_string()
        } else {
            let mut _crlf = [0u8; 2];
            tokio::time::timeout(timeout, reader.read_exact(&mut _crlf)).await??;
            String::new()
        }
    } else {
        String::new()
    };

    // 读取 leader_epoch（如果有）
    if array_len >= 3 {
        let mut epoch_line = String::new();
        tokio::time::timeout(timeout, reader.read_line(&mut epoch_line)).await??;
    }

    // 如果响应中的 leader_runid 是我们的 runid，表示投票给我们
    Ok(voted_runid == my_runid)
}

/// 执行自动故障转移
pub async fn execute_failover(sentinel: &SentinelManager, master_name: &str) {
    let master = match sentinel.get_master(master_name) {
        Some(m) => m,
        None => {
            log::error!("故障转移失败：找不到 master {}", master_name);
            return;
        }
    };

    if master.replicas.is_empty() {
        log::error!("故障转移失败：master {} 没有可用的 replica", master_name);
        return;
    }

    // 选择最优 replica（offset 最大的）
    let best_replica = master.replicas.iter()
        .filter(|r| !r.sdown)
        .max_by_key(|r| r.offset);

    let replica = match best_replica {
        Some(r) => r.clone(),
        None => {
            log::error!("故障转移失败：master {} 没有健康的 replica", master_name);
            return;
        }
    };

    log::info!(
        "故障转移：选择 replica {}:{} (offset={}) 作为新 master",
        replica.ip, replica.port, replica.offset
    );

    // 1. 向选中的 replica 发送 REPLICAOF NO ONE
    match send_replicaof_no_one(&replica.ip, replica.port).await {
        Ok(()) => {
            log::info!("故障转移：已向 {}:{} 发送 REPLICAOF NO ONE", replica.ip, replica.port);
        }
        Err(e) => {
            log::error!("故障转移失败：向 {}:{} 发送 REPLICAOF NO ONE 失败: {}", replica.ip, replica.port, e);
            return;
        }
    }

    // 等待新 master 确认 role:master（超时 60 秒）
    let timeout = Duration::from_secs(60);
    let start = std::time::Instant::now();
    let mut confirmed_master = false;
    loop {
        if start.elapsed() > timeout {
            log::warn!("故障转移：等待 {}:{} 成为 master 超时", replica.ip, replica.port);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        match check_role_master(&replica.ip, replica.port).await {
            Ok(true) => {
                log::info!("故障转移：{}:{} 已确认 role=master", replica.ip, replica.port);
                confirmed_master = true;
                break;
            }
            Ok(false) => {}
            Err(e) => {
                log::debug!("故障转移：检查 {}:{} role 失败: {}", replica.ip, replica.port, e);
            }
        }
    }

    if !confirmed_master {
        log::warn!("故障转移：新 master {}:{} 未在超时内确认 role=master，继续执行", replica.ip, replica.port);
    }

    // 2. 向其他 replica 发送 REPLICAOF <new_master_ip> <new_master_port>
    for other in &master.replicas {
        if other.ip == replica.ip && other.port == replica.port {
            continue;
        }
        match send_replicaof(&other.ip, other.port, &replica.ip, replica.port).await {
            Ok(()) => {
                log::info!(
                    "故障转移：已通知 replica {}:{} 指向新 master {}:{}",
                    other.ip, other.port, replica.ip, replica.port
                );
            }
            Err(e) => {
                log::warn!(
                    "故障转移：通知 replica {}:{} 失败: {}",
                    other.ip, other.port, e
                );
            }
        }
    }

    // 等待其他 replica 确认 master_host 和 master_port 正确（每个超时 60 秒，并行检查较复杂，这里串行）
    for other in &master.replicas {
        if other.ip == replica.ip && other.port == replica.port {
            continue;
        }
        let replica_start = std::time::Instant::now();
        let mut confirmed = false;
        loop {
            if replica_start.elapsed() > timeout {
                log::warn!(
                    "故障转移：等待 replica {}:{} 同步超时",
                    other.ip, other.port
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            match check_replica_master(&other.ip, other.port, &replica.ip, replica.port).await {
                Ok(true) => {
                    log::info!(
                        "故障转移：replica {}:{} 已确认指向新 master",
                        other.ip, other.port
                    );
                    confirmed = true;
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    log::debug!(
                        "故障转移：检查 replica {}:{} 同步状态失败: {}",
                        other.ip, other.port, e
                    );
                }
            }
        }
        if !confirmed {
            log::warn!(
                "故障转移：replica {}:{} 未在超时内确认同步",
                other.ip, other.port
            );
        }
    }

    // 3. 更新 Sentinel 内部状态：将 master 指向新地址
    sentinel.update_master_addr(master_name, replica.ip.clone(), replica.port);
    sentinel.set_odown(master_name, false);

    log::info!(
        "故障转移完成：master {} 已切换到 {}:{}",
        master_name, replica.ip, replica.port
    );
}

/// 向 replica 发送 REPLICAOF NO ONE
async fn send_replicaof_no_one(ip: &str, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, port);
    let timeout = std::time::Duration::from_secs(5);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();

    write_half.write_all(b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n").await?;

    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;

    if line.trim().starts_with("+OK") {
        Ok(())
    } else {
        Err(format!("REPLICAOF NO ONE 响应异常: {}", line.trim()).into())
    }
}

/// 向 replica 发送 REPLICAOF host port
async fn send_replicaof(ip: &str, port: u16, master_ip: &str, master_port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, port);
    let timeout = std::time::Duration::from_secs(5);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();

    let cmd = format!(
        "*3\r\n$9\r\nREPLICAOF\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        master_ip.len(), master_ip,
        master_port.to_string().len(), master_port
    );
    write_half.write_all(cmd.as_bytes()).await?;

    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;

    if line.trim().starts_with("+OK") {
        Ok(())
    } else {
        Err(format!("REPLICAOF 响应异常: {}", line.trim()).into())
    }
}

/// 检查实例的 role 是否为 master
async fn check_role_master(ip: &str, port: u16) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let info = get_info_replication(ip, port).await?;
    Ok(info.lines().any(|line| line.trim() == "role:master"))
}

/// 检查 replica 是否已指向正确的 master
async fn check_replica_master(
    ip: &str,
    port: u16,
    expected_master_ip: &str,
    expected_master_port: u16,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let info = get_info_replication(ip, port).await?;
    let mut master_host = None;
    let mut master_port = None;
    for line in info.lines() {
        if let Some(stripped) = line.strip_prefix("master_host:") {
            master_host = Some(stripped.trim().to_string());
        }
        if let Some(stripped) = line.strip_prefix("master_port:") {
            master_port = stripped.trim().parse::<u16>().ok();
        }
    }
    Ok(master_host.as_deref() == Some(expected_master_ip) && master_port == Some(expected_master_port))
}

/// 向实例发送 INFO replication，返回响应内容
async fn get_info_replication(ip: &str, port: u16) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
