//! redis-rust 服务器入口，解析命令行参数并启动各子系统
// 程序入口，启动 Redis-like 缓存服务器

#[cfg(unix)]
use tikv_jemallocator::Jemalloc;

#[cfg(unix)]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use std::sync::Arc;
use std::time::Duration;

use log::info;

use redis_rust::acl::AclManager;
use redis_rust::aof::{AofAsyncWriter, AofReplayer, AppendFsync};
use redis_rust::pubsub::PubSubManager;
use redis_rust::replication::ReplicationManager;
use redis_rust::sentinel::SentinelManager;
use redis_rust::server::Server;
use redis_rust::storage::{EvictionPolicy, StorageEngine};

/// 服务器默认监听端口
const DEFAULT_PORT: u16 = 6379;

/// Sentinel 模式默认监听端口
const SENTINEL_DEFAULT_PORT: u16 = 26379;

/// 后台清理任务的执行间隔（毫秒）
const CLEANUP_INTERVAL_MS: u64 = 1000;

/// AOF 持久化文件路径
const AOF_PATH: &str = "appendonly.aof";

/// RDB 快照文件路径
const RDB_PATH: &str = "dump.rdb";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志系统
    env_logger::init();

    // 解析命令行参数
    let mut port = DEFAULT_PORT;
    let mut no_aof = false;
    let mut sentinel_mode = false;
    let mut cluster_enabled = false;
    let mut appendfsync = AppendFsync::EverySec;
    let mut bind = "127.0.0.1".to_string();
    let mut cluster_announce_ip = None::<String>;
    let mut cluster_announce_port = None::<u16>;
    let mut cluster_announce_bus_port = None::<u16>;
    let mut maxmemory = None::<u64>;
    let mut maxmemory_policy = None::<String>;
    let mut timeout = None::<u64>;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port" {
            if let Some(p) = args.next() {
                port = p.parse().unwrap_or(DEFAULT_PORT);
            }
        } else if arg == "--bind" {
            if let Some(v) = args.next() {
                bind = v;
            }
        } else if arg == "--cluster-enabled"
            && let Some(val) = args.next()
        {
            cluster_enabled = val.eq_ignore_ascii_case("yes");
        } else if arg == "--cluster-announce-ip" {
            cluster_announce_ip = args.next();
        } else if arg == "--cluster-announce-port" {
            if let Some(v) = args.next() {
                cluster_announce_port = v.parse().ok();
            }
        } else if arg == "--cluster-announce-bus-port" {
            if let Some(v) = args.next() {
                cluster_announce_bus_port = v.parse().ok();
            }
        } else if arg == "--no-aof" {
            no_aof = true;
        } else if arg == "--sentinel" {
            sentinel_mode = true;
        } else if arg == "--appendfsync"
            && let Some(val) = args.next()
        {
            appendfsync = match val.to_ascii_lowercase().as_str() {
                "always" => AppendFsync::Always,
                "everysec" => AppendFsync::EverySec,
                "no" => AppendFsync::No,
                _ => AppendFsync::EverySec,
            };
        } else if arg == "--maxmemory" {
            if let Some(v) = args.next() {
                maxmemory = v.parse().ok();
            }
        } else if arg == "--maxmemory-policy" {
            maxmemory_policy = args.next();
        } else if arg == "--timeout" {
            if let Some(v) = args.next() {
                timeout = v.parse().ok();
            }
        }
    }
    // Sentinel 模式下默认端口为 26379
    if sentinel_mode && port == DEFAULT_PORT {
        port = SENTINEL_DEFAULT_PORT;
    }
    let addr = format!("{}:{}", bind, port);

    let announce_ip = cluster_announce_ip.unwrap_or_else(|| bind.clone());
    let announce_port = cluster_announce_port.unwrap_or(port);
    let announce_bus_port = cluster_announce_bus_port.unwrap_or(port + 10000);

    if sentinel_mode {
        info!("启动 redis-rust Sentinel 模式，监听 {}", addr);
    } else {
        info!("启动 redis-rust 服务器，监听 {}", addr);
    }

    // 创建存储引擎
    let storage = StorageEngine::new();
    if let Some(bytes) = maxmemory {
        storage.set_maxmemory(bytes);
        info!("maxmemory 设置为 {} bytes ({:.2} MB)", bytes, bytes as f64 / 1024.0 / 1024.0);
    }
    if let Some(ref policy_str) = maxmemory_policy {
        let policy = match policy_str.to_ascii_lowercase().as_str() {
            "noeviction" => EvictionPolicy::NoEviction,
            "allkeys-lru" => EvictionPolicy::AllKeysLru,
            "allkeys-random" => EvictionPolicy::AllKeysRandom,
            "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
            "volatile-lru" => EvictionPolicy::VolatileLru,
            "volatile-ttl" => EvictionPolicy::VolatileTtl,
            "volatile-random" => EvictionPolicy::VolatileRandom,
            "volatile-lfu" => EvictionPolicy::VolatileLfu,
            _ => {
                log::warn!("未知的 maxmemory-policy: {}，使用默认 allkeys-lru", policy_str);
                EvictionPolicy::AllKeysLru
            }
        };
        storage.set_eviction_policy(policy);
        info!("maxmemory-policy 设置为 {:?}", policy);
    }

    // 优先加载 RDB 快照（基础数据）
    let rdb_repl_info = match redis_rust::rdb::load(&storage, RDB_PATH) {
        Ok((replid, offset)) => {
            info!("RDB 快照加载成功");
            (replid, offset)
        }
        Err(e) => {
            // RDB 文件不存在是正常现象
            log::debug!("RDB 加载跳过: {}", e);
            (None, None)
        }
    };

    // 然后加载 AOF（增量数据）
    if !no_aof && let Err(e) = AofReplayer::replay(AOF_PATH, storage.clone()) {
        log::error!("AOF 重放失败: {}", e);
    }

    // 创建 AOF 写入器
    let aof = if no_aof {
        None
    } else {
        Some(Arc::new(AofAsyncWriter::new_with_fsync(AOF_PATH, appendfsync)?))
    };

    // 启动后台过期键清理任务，每秒扫描并删除一次过期键
    let _cleanup_handle = storage.start_cleanup_task(CLEANUP_INTERVAL_MS);
    info!(
        "后台过期键清理任务已启动，间隔 {} 毫秒",
        CLEANUP_INTERVAL_MS
    );

    // 创建发布订阅管理器
    let pubsub = PubSubManager::new();

    // 创建 ACL 管理器（默认用户拥有所有权限）
    let acl = AclManager::new();

    // 创建复制管理器
    let replication = Arc::new(ReplicationManager::new());

    // 如果 RDB 中包含复制信息，恢复到复制管理器
    if let (Some(replid), Some(offset)) = rdb_repl_info {
        replication.set_replid_and_offset(replid, offset);
        info!(
            "从 RDB 恢复复制信息: replid={}, offset={}",
            replication.get_master_replid(),
            replication.get_master_repl_offset()
        );
    }

    // 创建 Cluster 状态（仅在 Cluster 模式下）
    let cluster = if cluster_enabled {
        let c = Arc::new(redis_rust::cluster::ClusterState::new(
            announce_ip,
            announce_port,
            announce_bus_port,
        ));
        // 尝试加载已有拓扑
        if let Err(e) = c.load_nodes_conf("nodes.conf") {
            log::debug!("nodes.conf 加载失败或不存在: {}", e);
        } else {
            log::info!("从 nodes.conf 恢复集群拓扑");
        }
        // 启动时立即保存一次，确保文件存在
        if let Err(e) = c.save_nodes_conf("nodes.conf") {
            log::warn!("初始 nodes.conf 保存失败: {}", e);
        }
        Some(c)
    } else {
        None
    };

    // 创建 Sentinel 管理器（仅在 Sentinel 模式下）
    let sentinel = if sentinel_mode {
        let s = Arc::new(SentinelManager::new());
        // 加载已有配置
        if let Err(e) = s.load_config() {
            log::warn!("Sentinel 配置加载失败: {}", e);
        }
        Some(s)
    } else {
        None
    };

    // Sentinel 模式下启动监控任务
    if let Some(ref s) = sentinel {
        redis_rust::sentinel::monitor::start_monitor(s.clone());
    }

    // Sentinel 模式下启动发现任务
    if let Some(ref s) = sentinel {
        redis_rust::sentinel::discovery::start_discovery(s.clone(), port);
    }

    // Sentinel 模式下启动 ODOWN 检查任务
    if let Some(ref s) = sentinel {
        drop(redis_rust::sentinel::failover::start_odown_checker(
            s.clone(),
        ));
    }

    // Cluster 模式下启动集群总线监听
    if cluster_enabled {
        let bus_addr = format!("{}:{}", bind, announce_bus_port);
        let bus_addr_for_task = bus_addr.clone();
        let cluster_for_bus = cluster.clone().unwrap();
        tokio::spawn(async move {
            if let Err(e) =
                redis_rust::cluster::bus::start_cluster_bus(&bus_addr_for_task, cluster_for_bus)
                    .await
            {
                log::error!("集群总线启动失败: {}", e);
            }
        });
        log::info!("集群总线监听: {}", bus_addr);
    }

    // Cluster 模式下启动 Gossip 任务
    if cluster_enabled && let Some(ref c) = cluster {
        redis_rust::cluster::gossip::start_gossip(c.clone());
    }

    // Cluster 模式下启动故障检测任务
    if cluster_enabled && let Some(ref c) = cluster {
        redis_rust::cluster::failover::start_failure_detector(c.clone());
    }

    // Cluster 模式下定期保存拓扑（每 10 秒）
    if cluster_enabled && let Some(ref c) = cluster {
        let cluster_save = c.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if let Err(e) = cluster_save.save_nodes_conf("nodes.conf") {
                    log::warn!("nodes.conf 保存失败: {}", e);
                }
            }
        });
    }

    // 创建 TCP 服务器并启动
    let mut server = Server::new(&addr, storage, aof, pubsub, None)
        .with_rdb_path(RDB_PATH)
        .with_acl(acl)
        .with_replication(replication);
    if let Some(secs) = timeout {
        server = server.with_timeout(secs);
    }
    if let Some(ref c) = cluster {
        server = server.with_cluster(c.clone());
    }
    if let Some(s) = sentinel {
        server = server.with_sentinel(s);
    }

    // 监听优雅关闭信号（Ctrl+C / SIGINT）
    let server_for_signal = server.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            log::error!("信号监听失败: {}", e);
            return;
        }
        log::info!("收到 SIGINT/Ctrl+C，开始优雅关闭");
        server_for_signal.shutdown(true);
    });

    server.run().await?;

    Ok(())
}
