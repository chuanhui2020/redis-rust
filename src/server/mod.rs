// TCP 服务器模块，负责监听连接和处理客户端请求

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::aof::AofWriter;
use crate::error::Result;
use crate::keyspace::KeyspaceNotifier;
use crate::protocol::RespValue;
use crate::pubsub::PubSubManager;
use crate::scripting::ScriptEngine;
use crate::slowlog::SlowLog;
use crate::storage::StorageEngine;


pub mod connection;
pub mod handler;
pub mod pubsub;
pub mod transaction;

/// 辅助函数：创建 BulkString
pub(crate) fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::copy_from_slice(s.as_bytes())))
}

/// 辅助函数：从 Bytes 创建 BulkString
pub(crate) fn bulk_bytes(b: &Bytes) -> RespValue {
    RespValue::BulkString(Some(b.clone()))
}

pub use handler::ConnectionHandler;


/// 客户端消息（从订阅频道转发而来）
#[derive(Debug, Clone)]
pub(crate) enum ClientMessage {
    /// 精确频道消息: (频道名, 内容)
    Message(String, Bytes),
    /// 模式消息: (模式, 频道名, 内容)
    PMessage(String, String, Bytes),
}

/// 客户端订阅状态
pub(crate) struct SubscriptionState {
    /// 精确订阅: 频道名 -> 转发任务 abort handle
    channels: HashMap<String, tokio::task::AbortHandle>,
    /// 模式订阅: 模式 -> 转发任务 abort handle
    patterns: HashMap<String, tokio::task::AbortHandle>,
    /// 用于向客户端主循环发送聚合消息（主要被克隆到转发任务中使用）
    #[allow(dead_code)]
    msg_tx: mpsc::UnboundedSender<ClientMessage>,
}

impl SubscriptionState {
    fn new(msg_tx: mpsc::UnboundedSender<ClientMessage>) -> Self {
        Self {
            channels: HashMap::new(),
            patterns: HashMap::new(),
            msg_tx,
        }
    }

    /// 当前活跃订阅总数
    fn total(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
}

/// 回复模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMode {
    /// 正常回复
    On,
    /// 不回复任何命令
    Off,
    /// 跳过下一条命令的回复
    Skip,
}

/// 客户端信息
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    pub name: Option<String>,
    pub db: usize,
    /// 客户端标志
    pub flags: HashSet<String>,
    /// 是否被阻塞
    pub blocked: bool,
    /// 阻塞原因
    pub blocked_reason: Option<String>,
}

/// TCP 服务器结构体
#[derive(Debug, Clone)]
pub struct Server {
    /// 监听地址
    addr: String,
    /// 存储引擎
    storage: StorageEngine,
    /// AOF 写入器（可选）
    aof: Option<Arc<Mutex<AofWriter>>>,
    /// 发布订阅管理器
    pubsub: PubSubManager,
    /// 密码（可选）
    password: Option<String>,
    /// 已连接客户端注册表
    clients: Arc<RwLock<HashMap<u64, ClientInfo>>>,
    /// 下一个客户端 ID
    next_client_id: Arc<AtomicU64>,
    /// Lua 脚本引擎
    script_engine: ScriptEngine,
    /// RDB 快照文件路径（可选）
    rdb_path: Option<String>,
    /// 慢查询日志
    slowlog: SlowLog,
    /// ACL 管理器（可选）
    acl: Option<crate::acl::AclManager>,
    /// 客户端暂停状态：(结束时间, 模式 WRITE|ALL)
    client_pause: Arc<RwLock<Option<(Instant, String)>>>,
    /// 待关闭的客户端 ID 集合
    client_kill_flags: Arc<Mutex<HashSet<u64>>>,
    /// MONITOR 广播发送器
    monitor_tx: tokio::sync::broadcast::Sender<String>,
    /// 延迟追踪器
    latency: crate::latency::LatencyTracker,
    /// 全局 Keyspace 通知器
    keyspace_notifier: Arc<KeyspaceNotifier>,
}

impl Server {
    /// 创建新的服务器实例
    pub fn new(
        addr: &str,
        mut storage: StorageEngine,
        aof: Option<Arc<Mutex<AofWriter>>>,
        pubsub: PubSubManager,
        password: Option<String>,
    ) -> Self {
        let keyspace_notifier = Arc::new(KeyspaceNotifier::new(pubsub.clone()));
        storage.set_keyspace_notifier(keyspace_notifier.clone());
        Self {
            addr: addr.to_string(),
            storage,
            aof,
            pubsub,
            password,
            clients: Arc::new(RwLock::new(HashMap::new())),
            next_client_id: Arc::new(AtomicU64::new(1)),
            script_engine: ScriptEngine::new(),
            rdb_path: None,
            slowlog: SlowLog::new(),
            acl: None,
            client_pause: Arc::new(RwLock::new(None)),
            client_kill_flags: Arc::new(Mutex::new(HashSet::new())),
            monitor_tx: tokio::sync::broadcast::channel(1024).0,
            latency: crate::latency::LatencyTracker::new(),
            keyspace_notifier,
        }
    }

    /// 设置 ACL 管理器
    pub fn with_acl(mut self, acl: crate::acl::AclManager) -> Self {
        self.acl = Some(acl);
        self
    }

    /// 设置 RDB 快照文件路径
    pub fn with_rdb_path(mut self, path: &str) -> Self {
        self.rdb_path = Some(path.to_string());
        self
    }

    /// 启动服务器，开始监听并处理连接
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        self.run_with_listener(listener).await
    }

    /// 绑定到指定地址，返回实际绑定的地址和后台任务句柄
    /// 适用于测试场景（使用 0 端口让 OS 分配随机端口）
    pub async fn start(self) -> Result<(SocketAddr, tokio::task::JoinHandle<Result<()>>)> {
        let listener = TcpListener::bind(&self.addr).await?;
        let local_addr = listener.local_addr()?;
        let handle = tokio::spawn(self.run_with_listener(listener));
        Ok((local_addr, handle))
    }

    /// 使用已绑定的监听器运行服务主循环
    async fn run_with_listener(self, listener: TcpListener) -> Result<()> {
        log::info!("服务器已启动，等待客户端连接...");

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            log::info!("客户端已连接: {}", peer_addr);

            // 每个连接克隆一份存储引擎、AOF 写入器和 pubsub
            let storage = self.storage.clone();
            let aof = self.aof.clone();
            let pubsub = self.pubsub.clone();
            let password = self.password.clone();
            let clients = self.clients.clone();
            let next_client_id = self.next_client_id.clone();
            let script_engine = self.script_engine.clone();
            let rdb_path = self.rdb_path.clone();
            let slowlog = self.slowlog.clone();
            let acl = self.acl.clone();
            let client_pause = self.client_pause.clone();
            let client_kill_flags = self.client_kill_flags.clone();
            let monitor_tx = self.monitor_tx.clone();
            let latency = self.latency.clone();
            let keyspace_notifier = self.keyspace_notifier.clone();
            tokio::spawn(async move {
                if let Err(e) = connection::handle_connection(
                    stream, peer_addr.to_string(), storage, aof, pubsub,
                    password, clients, next_client_id, script_engine, rdb_path, slowlog, acl,
                    client_pause, client_kill_flags, monitor_tx, latency,
                    keyspace_notifier,
                ).await {
                    log::error!("处理连接 {} 时出错: {}", peer_addr, e);
                }
                log::info!("客户端已断开: {}", peer_addr);
            });
        }
    }
}

