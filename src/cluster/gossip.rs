// Cluster Gossip 协议模块

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ClusterState;

/// 启动 Gossip 任务：每秒随机选择节点发送 PING
pub fn start_gossip(cluster: Arc<ClusterState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        
        loop {
            interval.tick().await;
            
            let nodes = cluster.get_nodes();
            let my_id = cluster.myself_id();
            
            // 向所有非自身节点发送 PING
            for node in &nodes {
                if node.id == my_id {
                    continue;
                }
                
                let node_ip = node.ip.clone();
                let node_bus_port = node.bus_port;
                let cluster_clone = cluster.clone();
                let my_id_clone = my_id.clone();
                
                tokio::spawn(async move {
                    match send_cluster_ping(&node_ip, node_bus_port, &my_id_clone).await {
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
async fn send_cluster_ping(ip: &str, bus_port: u16, my_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", ip, bus_port);
    let timeout = Duration::from_millis(500);
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await??;
    
    // 发送简化的 PING 消息：PING <my_node_id>\n
    let msg = format!("PING {}\n", my_id);
    stream.write_all(msg.as_bytes()).await?;
    
    // 读取 PONG 响应
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(timeout, stream.read(&mut buf)).await??;
    let response = String::from_utf8_lossy(&buf[..n]);
    
    if response.contains("PONG") {
        Ok(())
    } else {
        Err(format!("期望 PONG，收到: {}", response.trim()).into())
    }
}
