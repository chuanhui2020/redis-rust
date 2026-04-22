// Cluster Gossip 协议模块

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ClusterState;

/// 启动 Gossip 任务：每秒向所有已知节点发送 PING
pub fn start_gossip(cluster: Arc<ClusterState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            let nodes = cluster.get_nodes();
            let my_id = cluster.myself_id();
            let my_node = cluster.myself();
            
            // 获取自己的 IP 和端口
            let (my_ip, my_port) = if let Some(ref node) = my_node {
                (node.ip.clone(), node.port)
            } else {
                continue;
            };
            
            // 向所有非自身节点发送 PING
            for node in &nodes {
                if node.id == my_id {
                    continue;
                }
                
                let node_ip = node.ip.clone();
                let node_bus_port = node.bus_port;
                let cluster_clone = cluster.clone();
                let my_id_clone = my_id.clone();
                let my_ip_clone = my_ip.clone();
                
                tokio::spawn(async move {
                    match send_cluster_ping(&node_ip, node_bus_port, &my_id_clone, &my_ip_clone, my_port, &cluster_clone).await {
                        Ok(()) => {
                            // 更新 pong_recv 时间
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            cluster_clone.update_pong_recv(&node_ip, node_bus_port - 10000, now);
                        }
                        Err(e) => {
                            log::debug!("Gossip PING {}:{} 失败: {}", node_ip, node_bus_port, e);
                        }
                    }
                });
            }
        }
    })
}

/// 向节点的集群总线端口发送 PING
/// 消息格式：PING <my_node_id> <my_ip> <my_port> <current_epoch>\n
/// 期望响应：PONG <remote_node_id> <remote_ip> <remote_port> <current_epoch>\n
pub async fn send_cluster_ping(
    ip: &str, 
    bus_port: u16, 
    my_id: &str,
    my_ip: &str,
    my_port: u16,
    cluster: &ClusterState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, bus_port);
    let timeout = Duration::from_millis(500);
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    
    // 发送 PING 消息，携带自己的节点信息和当前 epoch
    let epoch = cluster.get_current_epoch();
    let msg = format!("PING {} {} {} {}\n", my_id, my_ip, my_port, epoch);
    stream.write_all(msg.as_bytes()).await?;
    
    // 读取 PONG 响应
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(timeout, stream.read(&mut buf)).await??;
    let response = String::from_utf8_lossy(&buf[..n]);
    
    // 解析 PONG 响应中的节点信息
    if let Some(line) = response.lines().next() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 4 && parts[0] == "PONG" {
            let remote_id = parts[1];
            let remote_ip = parts[2];
            let remote_port: u16 = parts[3].parse().unwrap_or(0);
            let remote_epoch: u64 = parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
            
            // 如果远程节点未知，自动添加到集群状态
            if cluster.get_node(remote_id).is_none() && remote_port > 0 {
                // 检查是否已有相同地址但不同 ID 的节点，避免重复
                let existing_id = cluster.get_nodes().iter()
                    .find(|n| n.ip == remote_ip && n.port == remote_port)
                    .map(|n| n.id.clone());
                if let Some(old_id) = existing_id {
                    if old_id != remote_id {
                        cluster.remove_node(&old_id);
                    }
                }
                let new_node = super::ClusterNode::new(remote_id.to_string(), remote_ip.to_string(), remote_port);
                cluster.add_node(new_node);
            }

            // 如果远程 epoch 更高，更新本地 epoch
            if remote_epoch > cluster.get_current_epoch() {
                cluster.set_current_epoch(remote_epoch);
            }
            
            return Ok(());
        }
    }
    
    Err(format!("期望 PONG，收到: {}", response.trim()).into())
}

/// 广播拓扑更新给所有已知节点（故障转移后使用）
pub async fn broadcast_topology_update(cluster: Arc<ClusterState>) {
    let nodes = cluster.get_nodes();
    let my_id = cluster.myself_id();
    let my_node = match cluster.myself() {
        Some(n) => n,
        None => return,
    };

    let epoch = cluster.get_current_epoch();
    let flags = my_node.flags_string();
    let master_id = my_node.master_id.clone().unwrap_or_else(|| "-".to_string());

    // 构建 slot 范围字符串
    let slots = my_node.get_slots();
    let mut slot_ranges = Vec::new();
    let mut i = 0;
    while i < slots.len() {
        let start = slots[i];
        let mut end = start;
        while i + 1 < slots.len() && slots[i + 1] == end + 1 {
            end += 1;
            i += 1;
        }
        if start == end {
            slot_ranges.push(start.to_string());
        } else {
            slot_ranges.push(format!("{}-{}", start, end));
        }
        i += 1;
    }

    for node in &nodes {
        if node.id == my_id {
            continue;
        }

        let node_ip = node.ip.clone();
        let node_bus_port = node.bus_port;
        let update_msg = format!(
            "UPDATE {} {} {} {} {} {} {}\n",
            my_id,
            my_node.ip,
            my_node.port,
            epoch,
            flags,
            master_id,
            slot_ranges.join(" ")
        );

        tokio::spawn(async move {
            let addr = format!("{}:{}", node_ip, node_bus_port);
            let timeout = Duration::from_millis(500);
            if let Ok(Ok(mut stream)) = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
                let _ = tokio::time::timeout(timeout, stream.write_all(update_msg.as_bytes())).await;
            }
        });
    }
}
