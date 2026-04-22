// Sentinel ODOWN 判定与 Leader 选举模块

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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
                        Ok(true) => {
                            agree_count += 1;
                        }
                        _ => {}
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
                            &sentinel, &master.name, &master.sentinels,
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
async fn ask_sentinel_is_down(
    sentinel_ip: &str,
    sentinel_port: u16,
    master_ip: &str,
    master_port: u16,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", sentinel_ip, sentinel_port);
    let timeout = Duration::from_millis(500);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();

    // 发送 SENTINEL is-master-down-by-addr master_ip master_port 0 *
    let cmd = format!(
        "*5\r\n$8\r\nSENTINEL\r\n$25\r\nis-master-down-by-addr\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n*\r\n",
        master_ip.len(),
        master_ip,
        master_port.to_string().len(),
        master_port
    );
    write_half.write_all(cmd.as_bytes()).await?;

    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;

    // 简化解析：如果响应包含 ":1" 表示同意下线
    Ok(line.contains(":1"))
}

/// 尝试成为 leader（简化的 Raft-like 选举）
async fn try_become_leader(
    sentinel: &SentinelManager,
    master_name: &str,
    peers: &[super::SentinelPeer],
) -> bool {
    if peers.is_empty() {
        // 没有其他 Sentinel，自己就是 leader
        return true;
    }

    let my_runid = &sentinel.runid;
    let mut votes: u32 = 1; // 投自己一票
    let total = peers.len() as u32 + 1;
    let majority = total / 2 + 1;

    for peer in peers {
        match request_vote(&peer.ip, peer.port, master_name, my_runid).await {
            Ok(true) => {
                votes += 1;
            }
            _ => {}
        }
    }

    let elected = votes >= majority;
    if elected {
        log::info!(
            "Sentinel: leader 选举成功，获得 {}/{} 票（需要 {}）",
            votes,
            total,
            majority
        );
    } else {
        log::info!(
            "Sentinel: leader 选举失败，获得 {}/{} 票（需要 {}）",
            votes,
            total,
            majority
        );
    }

    elected
}

/// 向其他 Sentinel 请求投票
async fn request_vote(
    sentinel_ip: &str,
    sentinel_port: u16,
    master_name: &str,
    my_runid: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", sentinel_ip, sentinel_port);
    let timeout = Duration::from_millis(500);
    let stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    let (read_half, mut write_half) = stream.into_split();

    // 发送投票请求（复用 is-master-down-by-addr，带上 runid）
    let cmd = format!(
        "*5\r\n$8\r\nSENTINEL\r\n$25\r\nis-master-down-by-addr\r\n${}\r\n{}\r\n$1\r\n0\r\n${}\r\n{}\r\n",
        master_name.len(),
        master_name,
        my_runid.len(),
        my_runid
    );
    write_half.write_all(cmd.as_bytes()).await?;

    let mut reader = tokio::io::BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line)).await??;

    // 简化：如果响应包含我们的 runid，表示投票给我们
    Ok(line.contains(my_runid))
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
