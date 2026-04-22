// 程序入口，启动 Redis-like 缓存服务器

use std::sync::{Arc, Mutex};

use log::info;

use redis_rust::acl::AclManager;
use redis_rust::aof::{AofReplayer, AofWriter};
use redis_rust::pubsub::PubSubManager;
use redis_rust::replication::ReplicationManager;
use redis_rust::server::Server;
use redis_rust::storage::StorageEngine;

/// 服务器默认监听端口
const DEFAULT_PORT: u16 = 6379;

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
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port" {
            if let Some(p) = args.next() {
                port = p.parse().unwrap_or(DEFAULT_PORT);
            }
        } else if arg == "--no-aof" {
            no_aof = true;
        }
    }
    let addr = format!("127.0.0.1:{}", port);

    info!("启动 redis-rust 服务器，监听 {}", addr);

    // 创建存储引擎
    let storage = StorageEngine::new();

    // 优先加载 RDB 快照（基础数据）
    match redis_rust::rdb::load(&storage, RDB_PATH) {
        Ok(()) => {
            info!("RDB 快照加载成功");
        }
        Err(e) => {
            // RDB 文件不存在是正常现象
            log::debug!("RDB 加载跳过: {}", e);
        }
    }

    // 然后加载 AOF（增量数据）
    if !no_aof {
        if let Err(e) = AofReplayer::replay(AOF_PATH, storage.clone()) {
            log::error!("AOF 重放失败: {}", e);
        }
    }

    // 创建 AOF 写入器
    let aof = if no_aof {
        None
    } else {
        Some(Arc::new(Mutex::new(AofWriter::new(AOF_PATH)?)))
    };

    // 启动后台过期键清理任务，每秒扫描并删除一次过期键
    let _cleanup_handle = storage.start_cleanup_task(CLEANUP_INTERVAL_MS);
    info!("后台过期键清理任务已启动，间隔 {} 毫秒", CLEANUP_INTERVAL_MS);

    // 创建发布订阅管理器
    let pubsub = PubSubManager::new();

    // 创建 ACL 管理器（默认用户拥有所有权限）
    let acl = AclManager::new();

    // 创建复制管理器
    let replication = Arc::new(ReplicationManager::new());

    // 创建 TCP 服务器并启动
    let server = Server::new(&addr, storage, aof, pubsub, None)
        .with_rdb_path(RDB_PATH)
        .with_acl(acl)
        .with_replication(replication);
    server.run().await?;

    Ok(())
}
