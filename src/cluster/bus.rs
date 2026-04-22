// 集群总线模块：处理节点间通信

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::ClusterState;

/// 启动集群总线监听
pub async fn start_cluster_bus(addr: &str, cluster: Arc<ClusterState>) -> crate::error::Result<()> {
    let listener = TcpListener::bind(addr).await.map_err(crate::error::AppError::Io)?;
    log::info!("集群总线已启动: {}", addr);
    
    loop {
        let (stream, peer_addr) = listener.accept().await.map_err(crate::error::AppError::Io)?;
        let cluster = cluster.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_bus_connection(stream, cluster).await {
                log::debug!("集群总线连接处理失败 ({}): {}", peer_addr, e);
            }
        });
    }
}

/// 处理单个集群总线连接
/// 支持文本协议 PING/PONG/UPDATE 以及二进制协议
async fn handle_bus_connection(
    mut stream: tokio::net::TcpStream,
    cluster: Arc<ClusterState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        // 先 peek 4 字节判断协议类型
        let mut magic_buf = [0u8; 4];
        let peeked = match stream.peek(&mut magic_buf).await {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };

        if peeked >= 4 && &magic_buf == super::protocol::MAGIC {
            // 二进制协议
            let msg = super::protocol::read_message(&mut stream).await?;
            let peer_ip = stream.peer_addr()?.ip().to_string();

            cluster.update_from_message(&msg, &peer_ip);
            if msg.current_epoch > cluster.get_current_epoch() {
                cluster.set_current_epoch(msg.current_epoch);
            }

            // 回复 PONG
            let my_node = cluster.myself();
            if let Some(node) = my_node {
                let my_id = cluster.myself_id();
                let slots = super::protocol::build_slot_ranges(&node.slots, &my_id);
                let reply = super::protocol::ClusterMessage {
                    msg_type: super::protocol::MSG_TYPE_PONG,
                    flags: 0,
                    sender_port: node.port,
                    current_epoch: cluster.get_current_epoch(),
                    sender_id: my_id,
                    slots,
                    nodes: vec![],
                };
                super::protocol::write_message(&mut stream, &reply).await?;
            }
            continue;
        }

        // 文本协议
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        
        let msg = String::from_utf8_lossy(&buf[..n]);
        for line in msg.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            match parts[0] {
                "PING" if parts.len() >= 4 => {
                    let remote_id = parts[1];
                    let remote_ip = parts[2];
                    let remote_port: u16 = parts[3].parse().unwrap_or(0);
                    let remote_epoch: u64 = parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
                    
                    // 如果是未知节点，自动添加到集群状态
                    if cluster.get_node(remote_id).is_none() && remote_port > 0 {
                        let existing_id = cluster.get_nodes().iter()
                            .find(|n| n.ip == remote_ip && n.port == remote_port)
                            .map(|n| n.id.clone());
                        if let Some(old_id) = existing_id {
                            if old_id != remote_id {
                                cluster.remove_node(&old_id);
                            }
                        }
                        let new_node = super::ClusterNode::new(
                            remote_id.to_string(),
                            remote_ip.to_string(),
                            remote_port,
                        );
                        cluster.add_node(new_node);
                    }

                    // 如果远程 epoch 更高，更新本地 epoch
                    if remote_epoch > cluster.get_current_epoch() {
                        cluster.set_current_epoch(remote_epoch);
                    }
                    
                    // 回复 PONG，携带自己的节点信息和当前 epoch
                    let my_node = cluster.myself();
                    if let Some(node) = my_node {
                        let response = format!("PONG {} {} {} {}\n", node.id, node.ip, node.port, cluster.get_current_epoch());
                        stream.write_all(response.as_bytes()).await?;
                    }
                }
                "UPDATE" if parts.len() >= 7 => {
                    let remote_id = parts[1];
                    let remote_ip = parts[2];
                    let remote_port: u16 = parts[3].parse().unwrap_or(0);
                    let remote_epoch: u64 = parts[4].parse().unwrap_or(0);
                    let flags_str = parts[5];
                    let master_id_str = parts[6];

                    // 创建或更新节点
                    if cluster.get_node(remote_id).is_none() && remote_port > 0 {
                        let new_node = super::ClusterNode::new(
                            remote_id.to_string(),
                            remote_ip.to_string(),
                            remote_port,
                        );
                        cluster.add_node(new_node);
                    }

                    let flags = super::state::parse_node_flags(flags_str);
                    let master_id = if master_id_str == "-" {
                        None
                    } else {
                        Some(master_id_str.to_string())
                    };

                    // 收集 slot
                    let mut slots = Vec::new();
                    for i in 7..parts.len() {
                        super::state::parse_slot_range(parts[i], |slot| {
                            if slot < super::state::CLUSTER_SLOTS {
                                slots.push(slot);
                            }
                        });
                    }

                    cluster.update_node_topology(remote_id, flags, master_id, remote_epoch, slots.clone());

                    // 更新 slot 分配表
                    for slot in slots {
                        cluster.assign_slot(slot, remote_id);
                    }

                    // 如果远程 epoch 更高，更新本地 epoch
                    if remote_epoch > cluster.get_current_epoch() {
                        cluster.set_current_epoch(remote_epoch);
                    }
                }
                _ => {}
            }
        }
    }
}
