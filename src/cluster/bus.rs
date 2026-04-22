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
/// 解析 PING 消息中的 node_id/ip/port，如果是未知节点则自动添加到 ClusterState，回复 PONG
async fn handle_bus_connection(
    mut stream: tokio::net::TcpStream,
    cluster: Arc<ClusterState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = vec![0u8; 4096];
    
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        
        let msg = String::from_utf8_lossy(&buf[..n]);
        for line in msg.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 && parts[0] == "PING" {
                let remote_id = parts[1];
                let remote_ip = parts[2];
                let remote_port: u16 = parts[3].parse().unwrap_or(0);
                
                // 如果是未知节点，自动添加到集群状态
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
                    let new_node = super::ClusterNode::new(
                        remote_id.to_string(),
                        remote_ip.to_string(),
                        remote_port,
                    );
                    cluster.add_node(new_node);
                }
                
                // 回复 PONG，携带自己的节点信息
                let my_node = cluster.myself();
                if let Some(node) = my_node {
                    let response = format!("PONG {} {} {}\n", node.id, node.ip, node.port);
                    stream.write_all(response.as_bytes()).await?;
                }
            }
        }
    }
}
