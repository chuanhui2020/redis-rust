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
            if line.starts_with("PING") {
                // 回复 PONG <my_node_id>
                let my_id = cluster.myself_id();
                let response = format!("PONG {}\n", my_id);
                stream.write_all(response.as_bytes()).await?;
            }
        }
    }
}
