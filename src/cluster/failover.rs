// Cluster 故障检测与自动故障转移模块

use std::sync::Arc;
use std::time::Duration;

use super::state::{ClusterState, NodeFlag};

/// 启动集群故障检测任务
pub fn start_failure_detector(cluster: Arc<ClusterState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let cluster_node_timeout: u64 = 15000; // 默认 15 秒

        loop {
            interval.tick().await;

            let nodes = cluster.get_nodes();
            let my_id = cluster.myself_id();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            for node in &nodes {
                if node.id == my_id {
                    continue;
                }

                // 检查节点是否超时（PFAIL 判定）
                let last_pong = node.pong_recv;
                if last_pong == 0 {
                    continue; // 尚未收到过 PONG，跳过
                }

                let elapsed = now.saturating_sub(last_pong);

                if elapsed > cluster_node_timeout {
                    if !node.flags.contains(&NodeFlag::PFail) && !node.flags.contains(&NodeFlag::Fail) {
                        log::warn!(
                            "Cluster: 节点 {} ({}:{}) 标记为 PFAIL，超时 {}ms",
                            node.id, node.ip, node.port, elapsed
                        );
                        cluster.set_node_flag(&node.id, NodeFlag::PFail);
                    }
                } else if node.flags.contains(&NodeFlag::PFail) {
                    log::info!(
                        "Cluster: 节点 {} ({}:{}) 恢复，清除 PFAIL",
                        node.id, node.ip, node.port
                    );
                    cluster.remove_node_flag(&node.id, &NodeFlag::PFail);
                }
            }

            // 检查是否有 PFAIL 节点需要升级为 FAIL
            // 简化实现：如果本节点是 master 且检测到 PFAIL，直接标记 FAIL
            check_and_promote_fail(&cluster);

            // 更新集群状态
            update_cluster_ok(&cluster);
        }
    })
}

/// 检查 PFAIL 节点是否应升级为 FAIL
fn check_and_promote_fail(cluster: &ClusterState) {
    let nodes = cluster.get_nodes();

    for node in &nodes {
        if !node.flags.contains(&NodeFlag::PFail) {
            continue;
        }

        // 简化实现：如果节点是 master 且有 slot，标记为 FAIL
        if node.flags.contains(&NodeFlag::Master) && node.slot_count() > 0 {
            log::error!(
                "Cluster: 节点 {} ({}:{}) 标记为 FAIL",
                node.id, node.ip, node.port
            );
            cluster.remove_node_flag(&node.id, &NodeFlag::PFail);
            cluster.set_node_flag(&node.id, NodeFlag::Fail);
        }
    }
}

/// 更新集群 OK 状态
fn update_cluster_ok(cluster: &ClusterState) {
    let assigned = cluster.assigned_slots_count();
    if assigned < super::state::CLUSTER_SLOTS {
        cluster.set_cluster_ok(false);
        return;
    }

    // 检查是否有 FAIL 的 master 节点持有 slot
    let nodes = cluster.get_nodes();
    let has_failed_master = nodes.iter().any(|n| {
        n.flags.contains(&NodeFlag::Fail) && n.flags.contains(&NodeFlag::Master) && n.slot_count() > 0
    });

    cluster.set_cluster_ok(!has_failed_master);
}
