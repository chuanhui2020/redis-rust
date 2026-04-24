//! 主从复制模块（Replication）
//!
//! 本模块负责实现 Redis 兼容的主从复制机制，包括：
//!
//! - **PSYNC 协议**：支持全量同步（FULLRESYNC）与增量同步（CONTINUE）。
//! - **全量同步**：从节点连接主节点后，接收 RDB 快照并加载本地数据。
//! - **增量同步**：基于复制积压缓冲区（Replication Backlog）实现断线续传，
//!   当从节点的偏移量仍在积压窗口内时，只需补发缺失的命令数据。
//! - **心跳机制**：从节点定期向主节点发送 `REPLCONF ACK <offset>`，
//!   主节点据此检测连接健康并计算复制延迟（lag）。
//! - **复制积压缓冲区**：固定容量的环形字节缓冲区，用于缓存最近的写命令，
//!   为增量同步提供数据来源。
//! - **WAIT 命令**：阻塞等待指定数量的副本确认接收到目标偏移量，
//!   保障数据持久化与一致性语义。
//! - **断线重连**：从节点在网络异常后按指数退避策略自动重连主节点。
//!
//! 核心类型为 [`ReplicationManager`]，它在主节点模式下管理已连接的副本列表、
//! 广播写命令并维护积压缓冲区；在从节点模式下负责与主节点握手、同步及持续接收命令。

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU16, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use rand::Rng;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Notify};
use crate::error::{AppError, Result};
use crate::storage::StorageEngine;

/// 复制角色：标识当前节点在主从架构中的身份。
///
/// - [`Master`](ReplicationRole::Master) — 主节点，可接受读写请求并向副本广播写命令。
/// - [`Slave`](ReplicationRole::Slave) — 从节点，复制主节点数据，通常只读。
#[derive(Debug, Clone)]
pub enum ReplicationRole {
    Master,
    Slave,
}

/// 副本连接状态：描述从节点与主节点之间的当前同步阶段。
///
/// - [`Online`](ReplicaState::Online) — 已完成同步，正常接收并执行主节点命令。
/// - [`Wait`](ReplicaState::Wait) — 刚注册或尚未完成同步，等待进一步状态更新。
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaState {
    Online,
    Wait,
}

/// 单个已连接副本的元数据。
///
/// 主节点为每一个接入的从节点维护一个 [`ReplicaInfo`]，
/// 用于跟踪其复制偏移量、网络状态及延迟。
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub addr: String,
    pub port: u16,
    pub offset: i64,
    pub state: ReplicaState,
    pub last_ack_time: Instant,
    pub lag: i64,
}

/// 复制积压缓冲区（环形缓冲区）。
///
/// 用于缓存最近传播的写命令字节流。当从节点因网络抖动短暂断开并重连时，
/// 若其请求的偏移量仍落在本缓冲区的窗口内，主节点可直接从中提取缺失数据，
/// 避免昂贵的全量同步（RDB 传输）。
///
/// 内部基于 [`VecDeque<u8>`] 实现，超出容量时自动丢弃最早的数据，并同步推进
/// [`start_offset`](ReplicationBacklog::start_offset)。
#[derive(Debug)]
struct ReplicationBacklog {
    /// 存储最近命令字节的环形缓冲区
    buffer: VecDeque<u8>,
    /// 最大容量（默认 1MB）
    max_size: usize,
    /// 缓冲区第一个字节对应的复制偏移量
    start_offset: i64,
    /// 缓冲区最后一个字节之后的复制偏移量
    end_offset: i64,
}

impl ReplicationBacklog {
    /// 创建新的复制积压缓冲区。
    ///
    /// # 参数
    /// - `max_size` — 缓冲区最大字节容量。
    fn new(max_size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size),
            max_size,
            start_offset: 0,
            end_offset: 0,
        }
    }

    /// 向缓冲区追加数据，超出容量时从前端丢弃最早的字节。
    ///
    /// # 参数
    /// - `data` — 待追加的字节切片。
    fn append(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let data_len = data.len();
        // 如果单次写入超过容量，只保留最后 max_size 字节
        let to_append = if data_len > self.max_size {
            &data[data_len - self.max_size..]
        } else {
            data
        };
        // 计算需要从前面丢弃多少字节
        let need_space = self.buffer.len() + to_append.len();
        if need_space > self.max_size {
            let to_drop = need_space - self.max_size;
            self.buffer.drain(..to_drop);
            self.start_offset += to_drop as i64;
        }
        self.buffer.extend(to_append);
        self.end_offset += data_len as i64;
    }

    /// 从指定全局偏移量获取积压数据。
    ///
    /// # 参数
    /// - `offset` — 全局复制偏移量。
    ///
    /// # 返回值
    /// - `Some(Vec<u8>)` — 从该偏移量开始到缓冲区末尾的数据。
    /// - `None` — 偏移量已不在当前缓冲区窗口内。
    fn get_data_from_offset(&self, offset: i64) -> Option<Vec<u8>> {
        if offset < self.start_offset || offset > self.end_offset {
            return None;
        }
        let skip = (offset - self.start_offset) as usize;
        if skip > self.buffer.len() {
            return None;
        }
        Some(self.buffer.iter().skip(skip).copied().collect())
    }
}

/// 复制管理器，负责管理主从复制的完整生命周期。
///
/// [`ReplicationManager`] 以 [`RwLock`] 与原子变量组合的方式在多线程/异步环境中
/// 安全地维护复制状态。它同时承担两种角色：
///
/// **主节点角色**：
/// - 维护 [`connected_replicas`](ReplicationManager::connected_replicas) 列表。
/// - 通过 [`repl_tx`](ReplicationManager::repl_tx) 广播写命令给所有已连接副本。
/// - 维护 [`backlog`](ReplicationManager::backlog) 以支持增量同步。
/// - 响应 [`WAIT`](ReplicationManager::wait_for_replicas) 命令，等待副本 ACK。
///
/// **从节点角色**：
/// - 记录主节点地址（[`master_host`](ReplicationManager::master_host)、
///   [`master_port`](ReplicationManager::master_port)）。
/// - 通过 [`start_replication`](ReplicationManager::start_replication) 与主节点握手、
///   执行 PSYNC，并持续接收执行主节点下发的写命令。
/// - 定期发送 `REPLCONF ACK` 心跳。
#[derive(Debug)]
pub struct ReplicationManager {
    /// 当前节点的复制角色（主/从）。
    role: RwLock<ReplicationRole>,
    /// 主节点复制 ID（40 字符随机十六进制字符串）。
    /// 主节点模式下为本机生成的 replid；从节点模式下为所连接主节点的 replid。
    master_replid: RwLock<String>,
    /// 主节点复制偏移量，表示已传播（或已接收）的命令字节总数。
    master_repl_offset: AtomicI64,
    /// 主节点主机地址（仅在从节点模式下有效）。
    master_host: RwLock<Option<String>>,
    /// 主节点端口（仅在从节点模式下有效）。
    master_port: RwLock<Option<u16>>,
    /// 主节点连接状态（仅在从节点模式下有效）：`true` 表示连接正常。
    master_link_up: AtomicBool,
    /// 最后一次收到主节点数据的时间（从节点心跳检测用）。
    master_last_io: RwLock<Option<Instant>>,
    /// 已连接的副本列表（仅在主节点模式下有效）。
    connected_replicas: RwLock<Vec<ReplicaInfo>>,
    /// 本机监听端口，用于 `REPLCONF listening-port` 握手。
    listening_port: AtomicU16,
    /// 写命令广播通道的发送端，主节点通过它向所有副本异步推送写命令。
    repl_tx: broadcast::Sender<bytes::Bytes>,
    /// 复制积压缓冲区，缓存最近传播的命令字节以支持增量同步。
    backlog: RwLock<ReplicationBacklog>,
    /// 副本 ACK 通知器，当任意副本更新偏移量时通知等待者（用于 [`WAIT`](ReplicationManager::wait_for_replicas)）。
    ack_notify: Notify,
}

impl ReplicationManager {
    /// 创建新的复制管理器，默认以主节点身份启动。
    ///
    /// # 返回值
    /// 初始化后的 [`ReplicationManager`] 实例：
    /// - 角色为 [`Master`](ReplicationRole::Master)。
    /// - 生成一个新的随机 `master_replid`。
    /// - 复制偏移量初始化为 `0`。
    /// - 积压缓冲区容量默认为 1MB（1,048,576 字节）。
    /// - 广播通道容量为 10,000 条消息。
    pub fn new() -> Self {
        let (repl_tx, _) = broadcast::channel(10000);
        Self {
            role: RwLock::new(ReplicationRole::Master),
            master_replid: RwLock::new(Self::generate_replid()),
            master_repl_offset: AtomicI64::new(0),
            master_host: RwLock::new(None),
            master_port: RwLock::new(None),
            master_link_up: AtomicBool::new(false),
            master_last_io: RwLock::new(None),
            connected_replicas: RwLock::new(Vec::new()),
            listening_port: AtomicU16::new(0),
            repl_tx,
            backlog: RwLock::new(ReplicationBacklog::new(1_048_576)),
            ack_notify: Notify::new(),
        }
    }

    /// 获取当前节点的复制角色。
    ///
    /// # 返回值
    /// - [`ReplicationRole::Master`] — 当前为主节点。
    /// - [`ReplicationRole::Slave`] — 当前为从节点。
    pub fn get_role(&self) -> ReplicationRole {
        let guard = self.role.read().unwrap();
        match *guard {
            ReplicationRole::Master => ReplicationRole::Master,
            ReplicationRole::Slave => ReplicationRole::Slave,
        }
    }

    /// 获取主节点复制 ID。
    ///
    /// # 返回值
    /// 当前维护的 40 字符复制 ID 字符串。
    /// 主节点模式下为本机 ID；从节点模式下为所连主节点的 ID。
    pub fn get_master_replid(&self) -> String {
        self.master_replid.read().unwrap().clone()
    }

    /// 获取主节点复制偏移量。
    ///
    /// # 返回值
    /// 当前的全局复制偏移量（已传播/接收的命令字节总数）。
    pub fn get_master_repl_offset(&self) -> i64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    /// 获取主节点地址信息（仅在从节点模式下有效）。
    ///
    /// # 返回值
    /// `(Option<host>, Option<port>)` 元组；若当前为主节点则通常返回 `(None, None)`。
    pub fn get_master_host_port(&self) -> (Option<String>, Option<u16>) {
        let host = self.master_host.read().unwrap().clone();
        let port = *self.master_port.read().unwrap();
        (host, port)
    }

    /// 获取已连接副本列表的快照。
    ///
    /// # 返回值
    /// 包含所有 [`ReplicaInfo`] 的向量；为主节点模式下使用。
    pub fn get_connected_replicas(&self) -> Vec<ReplicaInfo> {
        self.connected_replicas.read().unwrap().clone()
    }

    /// 检查是否至少存在一个已连接的副本。
    ///
    /// # 返回值
    /// `true` 表示当前有副本在线；`false` 表示无副本连接。
    pub fn has_connected_replicas(&self) -> bool {
        // 快速路径：读取锁检查副本列表是否为空
        !self.connected_replicas.read().unwrap().is_empty()
    }

    /// 设置本机监听端口。
    ///
    /// # 参数
    /// - `port` — 节点对外提供服务的 TCP 端口，用于 `REPLCONF listening-port` 握手。
    pub fn set_listening_port(&self, port: u16) {
        self.listening_port.store(port, Ordering::Relaxed);
    }

    /// 获取本机监听端口。
    ///
    /// # 返回值
    /// 之前通过 [`set_listening_port`](ReplicationManager::set_listening_port) 设置的端口号；
    /// 未设置时返回 `0`。
    pub fn get_listening_port(&self) -> u16 {
        self.listening_port.load(Ordering::Relaxed)
    }

    /// 将当前节点设置为指定主节点的从节点。
    ///
    /// # 参数
    /// - `host` — 主节点主机地址。
    /// - `port` — 主节点端口。
    ///
    /// # 说明
    /// 修改角色为 [`Slave`](ReplicationRole::Slave)，记录主节点地址，
    /// 并将 [`master_link_up`](ReplicationManager::master_link_up) 置为 `false`，
    /// 等待后续 [`start_replication`](ReplicationManager::start_replication) 建立实际连接。
    pub fn set_replicaof(&self, host: String, port: u16) {
        let mut role = self.role.write().unwrap();
        let mut master_host = self.master_host.write().unwrap();
        let mut master_port = self.master_port.write().unwrap();
        *role = ReplicationRole::Slave;
        *master_host = Some(host);
        *master_port = Some(port);
        self.master_link_up.store(false, Ordering::Relaxed);
    }

    /// 取消从节点身份，恢复为主节点，并生成新的复制 ID。
    ///
    /// # 说明
    /// 对应 Redis 的 `REPLICAOF NO ONE` 语义：
    /// - 角色切换为 [`Master`](ReplicationRole::Master)。
    /// - 生成全新的 [`master_replid`](ReplicationManager::master_replid)，
    ///   使此前依赖旧 replid 的副本必须执行全量同步。
    /// - 清空主节点地址缓存。
    pub fn set_replicaof_no_one(&self) {
        let mut role = self.role.write().unwrap();
        let mut master_replid = self.master_replid.write().unwrap();
        let mut master_host = self.master_host.write().unwrap();
        let mut master_port = self.master_port.write().unwrap();
        *role = ReplicationRole::Master;
        *master_replid = Self::generate_replid();
        *master_host = None;
        *master_port = None;
        self.master_link_up.store(false, Ordering::Relaxed);
    }

    /// 设置主节点连接状态。
    ///
    /// # 参数
    /// - `up` — `true` 表示连接正常；`false` 表示连接断开或尚未建立。
    pub fn set_master_link_up(&self, up: bool) {
        self.master_link_up.store(up, Ordering::Relaxed);
    }

    /// 更新最后一次收到主节点数据的时间为当前时刻。
    ///
    /// # 说明
    /// 从节点在每次成功读取主节点网络数据或完成同步时调用，
    /// 用于 [`get_info_string`](ReplicationManager::get_info_string) 计算 `master_last_io_seconds_ago`。
    pub fn touch_master_last_io(&self) {
        let mut guard = self.master_last_io.write().unwrap();
        *guard = Some(Instant::now());
    }

    /// 生成 `INFO replication` 段落所需的格式化字符串。
    ///
    /// # 返回值
    /// 符合 Redis INFO 协议的纯文本字符串，包含：
    /// - `role`、`master_replid`、`master_repl_offset`
    /// - 从节点模式下额外输出 `master_host`、`master_port`、`master_link_status`、
    ///   `master_last_io_seconds_ago`、`slave_repl_offset`、`slave_read_only`
    /// - 主节点模式下额外输出 `connected_slaves` 及各副本详情（`ip`、`port`、`state`、`offset`、`lag`）
    pub fn get_info_string(&self) -> String {
        let role_str = match self.get_role() {
            ReplicationRole::Master => "master",
            ReplicationRole::Slave => "slave",
        };
        let mut info = format!("role:{}\r\n", role_str);
        info.push_str(&format!("master_replid:{}\r\n", self.get_master_replid()));
        info.push_str(&format!(
            "master_repl_offset:{}\r\n",
            self.get_master_repl_offset()
        ));
        if let ReplicationRole::Slave = self.get_role() {
            let (host, port) = self.get_master_host_port();
            if let Some(h) = host {
                info.push_str(&format!("master_host:{}\r\n", h));
            }
            if let Some(p) = port {
                info.push_str(&format!("master_port:{}\r\n", p));
            }
            let link_status = if self.master_link_up.load(Ordering::Relaxed) { "up" } else { "down" };
            info.push_str(&format!("master_link_status:{}\r\n", link_status));
            let last_io_secs = {
                let guard = self.master_last_io.read().unwrap();
                match *guard {
                    Some(t) => t.elapsed().as_secs() as i64,
                    None => -1,
                }
            };
            info.push_str(&format!("master_last_io_seconds_ago:{}\r\n", last_io_secs));
            if !self.master_link_up.load(Ordering::Relaxed) {
                let down_since = {
                    let guard = self.master_last_io.read().unwrap();
                    match *guard {
                        Some(t) => t.elapsed().as_secs() as i64,
                        None => -1,
                    }
                };
                info.push_str(&format!("master_link_down_since_seconds:{}\r\n", down_since));
            }
            info.push_str(&format!("slave_repl_offset:{}\r\n", self.get_master_repl_offset()));
            info.push_str("slave_read_only:1\r\n");
        } else {
            let replicas = self.get_connected_replicas();
            info.push_str(&format!("connected_slaves:{}\r\n", replicas.len()));
            for (i, r) in replicas.iter().enumerate() {
                let state_str = match r.state {
                    ReplicaState::Online => "online",
                    ReplicaState::Wait => "wait",
                };
                info.push_str(&format!(
                    "slave{}:ip={},port={},state={},offset={},lag={}\r\n",
                    i, r.addr, r.port, state_str, r.offset, r.lag
                ));
            }
        }
        info
    }

    /// 获取写命令广播发送端的克隆。
    ///
    /// # 返回值
    /// [`broadcast::Sender<bytes::Bytes>`] 克隆，可用于向所有已订阅副本发送写命令数据。
    pub fn get_repl_tx(&self) -> broadcast::Sender<bytes::Bytes> {
        self.repl_tx.clone()
    }

    /// 订阅写命令广播通道。
    ///
    /// # 返回值
    /// 一个新的 [`broadcast::Receiver<bytes::Bytes>`]，可异步接收主节点广播的写命令字节流。
    pub fn subscribe(&self) -> broadcast::Receiver<bytes::Bytes> {
        self.repl_tx.subscribe()
    }

    /// 注册一个已连接的副本。
    ///
    /// # 参数
    /// - `addr` — 副本的 IP 地址。
    /// - `port` — 副本的端口。
    ///
    /// # 说明
    /// 主节点在副本完成初始握手后调用此方法，将其加入 [`connected_replicas`](ReplicationManager::connected_replicas)。
    /// 初始状态为 [`ReplicaState::Wait`]，偏移量为 `0`。
    pub fn add_replica(&self, addr: String, port: u16) {
        let mut replicas = self.connected_replicas.write().unwrap();
        replicas.push(ReplicaInfo {
            addr,
            port,
            offset: 0,
            state: ReplicaState::Wait,
            last_ack_time: Instant::now(),
            lag: 0,
        });
    }

    /// 移除已连接的副本。
    ///
    /// # 参数
    /// - `addr` — 副本的 IP 地址。
    /// - `port` — 副本的端口。
    ///
    /// # 说明
    /// 当副本断开连接时调用，从 [`connected_replicas`](ReplicationManager::connected_replicas) 中移除对应条目。
    pub fn remove_replica(&self, addr: &str, port: u16) {
        let mut replicas = self.connected_replicas.write().unwrap();
        replicas.retain(|r| !(r.addr == addr && r.port == port));
    }

    /// 更新指定副本的复制偏移量及相关状态。
    ///
    /// # 参数
    /// - `addr` — 副本的 IP 地址。
    /// - `port` — 副本的端口。
    /// - `offset` — 副本最新确认的复制偏移量。
    ///
    /// # 说明
    /// 主节点在收到副本的 `REPLCONF ACK <offset>` 后调用此方法。
    /// 更新后会将副本状态置为 [`ReplicaState::Online`]，重新计算 `lag`，
    /// 并通过 [`ack_notify`](ReplicationManager::ack_notify) 唤醒正在执行 [`WAIT`](ReplicationManager::wait_for_replicas) 的等待者。
    pub fn update_replica_offset(&self, addr: &str, port: u16, offset: i64) {
        let master_offset = self.get_master_repl_offset();
        let mut replicas = self.connected_replicas.write().unwrap();
        for r in replicas.iter_mut() {
            if r.addr == addr && r.port == port {
                r.offset = offset;
                r.state = ReplicaState::Online;
                r.last_ack_time = Instant::now();
                r.lag = master_offset - offset;
                break;
            }
        }
        drop(replicas);
        self.ack_notify.notify_waiters();
    }

    /// 增加主节点复制偏移量。
    ///
    /// # 参数
    /// - `delta` — 要增加的字节数（通常为本次传播命令的 RESP 编码长度）。
    pub fn incr_master_repl_offset(&self, delta: i64) {
        self.master_repl_offset.fetch_add(delta, Ordering::Relaxed);
    }

    /// 向复制积压缓冲区追加数据。
    ///
    /// # 参数
    /// - `data` — 待缓存的命令字节切片。
    ///
    /// # 说明
    /// 主节点在每次向副本广播写命令时，应同步调用此方法将相同数据写入积压缓冲区，
    /// 以便后续支持增量同步。
    pub fn append_to_backlog(&self, data: &[u8]) {
        let mut backlog = self.backlog.write().unwrap();
        backlog.append(data);
    }

    /// 从指定偏移量获取积压数据。
    ///
    /// # 参数
    /// - `offset` — 全局复制偏移量。
    ///
    /// # 返回值
    /// - `Some(Vec<u8>)` — 从该偏移量开始到缓冲区末尾的积压数据。
    /// - `None` — 偏移量已超出当前缓冲区窗口，需要执行全量同步。
    pub fn get_backlog_from_offset(&self, offset: i64) -> Option<Vec<u8>> {
        let backlog = self.backlog.read().unwrap();
        backlog.get_data_from_offset(offset)
    }

    /// 统计偏移量大于等于目标偏移量的副本数量。
    ///
    /// # 参数
    /// - `target_offset` — 目标复制偏移量。
    ///
    /// # 返回值
    /// 满足条件的副本数量。
    pub fn count_replicas_at_offset(&self, target_offset: i64) -> i64 {
        let replicas = self.connected_replicas.read().unwrap();
        replicas.iter().filter(|r| r.offset >= target_offset).count() as i64
    }

    /// 阻塞等待指定数量的副本确认已接收到目标偏移量。
    ///
    /// # 参数
    /// - `target_offset` — 目标复制偏移量。
    /// - `numreplicas` — 需要等待确认的最少副本数量。
    /// - `timeout_ms` — 超时时间（毫秒）。`0` 表示无限等待。
    ///
    /// # 返回值
    /// 实际已确认偏移量大于等于 `target_offset` 的副本数量。
    /// 若超时前已达到 `numreplicas`，则提前返回；否则返回超时瞬间的实际数量。
    ///
    /// # 说明
    /// 对应 Redis 的 `WAIT numreplicas timeout` 命令语义，
    /// 用于在写命令返回客户端前保证数据被指定数量的副本接收，提升持久化保障。
    pub async fn wait_for_replicas(&self, target_offset: i64, numreplicas: i64, timeout_ms: i64) -> i64 {
        // 立即检查
        let count = self.count_replicas_at_offset(target_offset);
        if count >= numreplicas || numreplicas <= 0 {
            return count;
        }

        if timeout_ms == 0 {
            // timeout=0 表示无限等待
            loop {
                self.ack_notify.notified().await;
                let count = self.count_replicas_at_offset(target_offset);
                if count >= numreplicas {
                    return count;
                }
            }
        } else {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms as u64);
            loop {
                match tokio::time::timeout_at(deadline, self.ack_notify.notified()).await {
                    Ok(()) => {
                        let count = self.count_replicas_at_offset(target_offset);
                        if count >= numreplicas {
                            return count;
                        }
                    }
                    Err(_) => {
                        return self.count_replicas_at_offset(target_offset);
                    }
                }
            }
        }
    }

    /// 启动从节点复制，外层循环负责断线重连（指数退避）。
    ///
    /// # 参数
    /// - `storage` — 本地存储引擎，用于加载主节点下发的 RDB 快照及执行后续写命令。
    /// - `master_host` — 主节点主机地址。
    /// - `master_port` — 主节点端口。
    ///
    /// # 返回值
    /// - `Ok(())` — 从节点主动取消复制（如执行 `REPLICAOF NO ONE`）或连接正常关闭。
    /// - `Err(AppError)` — 发生不可恢复的错误。
    ///
    /// # 说明
    /// 本函数在异步循环中不断尝试连接主节点：
    /// 1. 首次或重连时调用 [`do_replication`](ReplicationManager::do_replication) 执行实际握手与同步。
    /// 2. 若连接异常断开，按指数退避策略（1秒起，最高 60 秒）等待后重试。
    /// 3. 若节点被提升为主节点（`REPLICAOF NO ONE`），则优雅退出循环。
    pub async fn start_replication(
        &self,
        storage: StorageEngine,
        master_host: String,
        master_port: u16,
    ) -> Result<()> {
        let mut retry_delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(60);

        loop {
            self.set_master_link_up(false);

            match self
                .do_replication(&storage, &master_host, master_port)
                .await
            {
                Ok(()) => {
                    log::info!("主节点连接正常关闭");
                }
                Err(e) => {
                    log::error!("复制连接失败: {}, {}秒后重试", e, retry_delay.as_secs());
                }
            }

            self.set_master_link_up(false);

            // 检查是否仍然是从节点（可能已经被 REPLICAOF NO ONE 取消）
            if matches!(self.get_role(), ReplicationRole::Master) {
                log::info!("已切换为主节点，停止重连");
                return Ok(());
            }

            tokio::time::sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, max_delay);

            log::info!("尝试重连主节点 {}:{}", master_host, master_port);
        }
    }

    /// 内部复制逻辑：建立连接、握手、同步、接收命令
    async fn do_replication(
        &self,
        storage: &StorageEngine,
        master_host: &str,
        master_port: u16,
    ) -> Result<()> {
        let addr = format!("{}:{}", master_host, master_port);
        log::info!("开始连接主节点: {}", addr);

        let mut stream = TcpStream::connect(&addr).await.map_err(AppError::Io)?;
        let (mut read_half, mut write_half) = stream.split();
        let mut reader = tokio::io::BufReader::new(&mut read_half);

        // 1. 发送 PING
        write_half
            .write_all(b"*1\r\n$4\r\nPING\r\n")
            .await
            .map_err(AppError::Io)?;
        let mut line = String::new();
        reader.read_line(&mut line).await.map_err(AppError::Io)?;
        if !line.trim().starts_with("+PONG") {
            return Err(AppError::Command(format!("主节点未响应 PONG: {}", line)));
        }

        // 2. 发送 REPLCONF listening-port
        let port = self.get_listening_port();
        let cmd = format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
            port.to_string().len(),
            port
        );
        write_half
            .write_all(cmd.as_bytes())
            .await
            .map_err(AppError::Io)?;
        line.clear();
        reader.read_line(&mut line).await.map_err(AppError::Io)?;
        if !line.trim().starts_with("+OK") {
            return Err(AppError::Command(format!(
                "REPLCONF listening-port 失败: {}",
                line
            )));
        }

        // 3. 发送 REPLCONF capa eof capa psync2
        write_half
            .write_all(b"*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            .await
            .map_err(AppError::Io)?;
        line.clear();
        reader.read_line(&mut line).await.map_err(AppError::Io)?;
        if !line.trim().starts_with("+OK") {
            return Err(AppError::Command(format!(
                "REPLCONF capa 失败: {}",
                line
            )));
        }

        // 4. 发送 PSYNC：如果已有 replid 和 offset，尝试增量同步
        let current_replid = self.get_master_replid();
        let current_offset = self.get_master_repl_offset();
        let psync_cmd = if current_offset > 0 {
            format!(
                "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                current_replid.len(),
                current_replid,
                current_offset.to_string().len(),
                current_offset
            )
        } else {
            "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".to_string()
        };
        write_half
            .write_all(psync_cmd.as_bytes())
            .await
            .map_err(AppError::Io)?;
        line.clear();
        reader.read_line(&mut line).await.map_err(AppError::Io)?;
        if line.trim().starts_with("+FULLRESYNC") {
            // 解析 replid 和 offset
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                return Err(AppError::Command("FULLRESYNC 响应格式错误".to_string()));
            }
            let replid = parts[1].to_string();
            let offset = parts[2].parse::<i64>().unwrap_or(0);

            // 5. 读取 RDB 长度行
            line.clear();
            reader.read_line(&mut line).await.map_err(AppError::Io)?;
            let rdb_len_str = line.trim();
            if !rdb_len_str.starts_with('$') {
                return Err(AppError::Command(format!(
                    "期望 RDB 长度行，收到: {}",
                    rdb_len_str
                )));
            }
            let rdb_len: usize = rdb_len_str[1..]
                .parse()
                .map_err(|_| AppError::Command("RDB 长度解析失败".to_string()))?;

            // 6. 读取 RDB 数据
            let mut rdb_data = vec![0u8; rdb_len];
            reader.read_exact(&mut rdb_data).await.map_err(AppError::Io)?;

            // 7. 清空现有数据后保存到临时文件并加载
            if let Err(e) = storage.flush() {
                log::error!("全量同步前清空数据失败: {}", e);
                return Err(AppError::Storage(format!("FLUSHALL 失败: {}", e)));
            }
            let temp_path = format!("temp_rdb_{}.rdb", std::process::id());
            {
                let mut file = std::fs::File::create(&temp_path).map_err(AppError::Io)?;
                file.write_all(&rdb_data).map_err(AppError::Io)?;
            }
            let (loaded_replid, loaded_offset) = crate::rdb::load(storage, &temp_path)?;
            let _ = std::fs::remove_file(&temp_path);

            // 8. 更新复制状态：优先使用 RDB 中保存的 replid/offset，回退到 FULLRESYNC 响应中的值
            let final_replid = loaded_replid.unwrap_or_else(|| replid.clone());
            let final_offset = loaded_offset.unwrap_or(offset);
            {
                let mut master_replid_guard = self.master_replid.write().unwrap();
                *master_replid_guard = final_replid.clone();
            }
            self.master_repl_offset.store(final_offset, Ordering::Relaxed);

            log::info!("全量同步完成，replid: {}, offset: {}", replid, offset);
            self.set_master_link_up(true);
            self.touch_master_last_io();
        } else if line.trim().starts_with("+CONTINUE") {
            // 增量同步 - 直接进入命令接收循环
            log::info!("增量同步开始");
            self.set_master_link_up(true);
            self.touch_master_last_io();
        } else {
            return Err(AppError::Command(format!(
                "主节点未响应 FULLRESYNC 或 CONTINUE: {}",
                line
            )));
        }

        // 9. 进入命令接收循环，持续执行主节点发送的写命令
        let parser = crate::protocol::RespParser::new();
        let mut buf = bytes::BytesMut::with_capacity(4096);
        let cmd_parser = crate::command::CommandParser::new();
        let executor = crate::command::CommandExecutor::new(storage.clone());
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));

        loop {
            // 检查是否已被 REPLICAOF NO ONE 取消，如果是则主动断开
            if matches!(self.get_role(), ReplicationRole::Master) {
                log::info!("已切换为主节点，主动断开复制连接");
                self.set_master_link_up(false);
                return Ok(());
            }

            // 先处理缓冲区中所有完整命令
            loop {
                let bytes_before = buf.len();
                match parser.parse(&mut buf) {
                    Ok(Some(resp_value)) => {
                        let bytes_consumed = bytes_before - buf.len();
                        match cmd_parser.parse(resp_value) {
                            Ok(cmd) => {
                                // 在副本上执行主节点发送的命令（写命令和非写命令如 SELECT）
                                let _ = executor.execute(cmd);
                                self.incr_master_repl_offset(bytes_consumed as i64);
                            }
                            Err(e) => {
                                log::warn!("复制命令解析错误: {}", e);
                                self.incr_master_repl_offset(bytes_consumed as i64);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        log::warn!("RESP 解析错误: {}", e);
                        // 清空缓冲区避免死循环
                        buf.clear();
                        break;
                    }
                }
            }

            // 等待网络数据或心跳定时器
            tokio::select! {
                result = reader.read_buf(&mut buf) => {
                    match result {
                        Ok(0) => {
                            log::info!("主节点连接已关闭");
                            self.set_master_link_up(false);
                            return Ok(());
                        }
                        Ok(n) => {
                            log::debug!("从主节点读取 {} 字节", n);
                            self.touch_master_last_io();
                        }
                        Err(e) => {
                            log::error!("读取主节点数据失败: {}", e);
                            self.set_master_link_up(false);
                            return Err(AppError::Io(e));
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let offset = self.get_master_repl_offset();
                    let ack_cmd = format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                        offset.to_string().len(),
                        offset
                    );
                    if let Err(e) = write_half.write_all(ack_cmd.as_bytes()).await {
                        log::error!("发送 REPLCONF ACK 失败: {}", e);
                        return Err(AppError::Io(e));
                    }
                }
            }
        }
    }

    /// 设置主节点复制 ID 和偏移量（用于从 RDB 恢复）。
    ///
    /// # 参数
    /// - `replid` — 要设置的复制 ID。
    /// - `offset` — 要设置的复制偏移量。
    pub fn set_replid_and_offset(&self, replid: String, offset: i64) {
        let mut master_replid = self.master_replid.write().unwrap();
        *master_replid = replid;
        self.master_repl_offset.store(offset, Ordering::Relaxed);
    }

    /// 生成 40 字符随机十六进制复制 ID
    fn generate_replid() -> String {
        let mut rng = rand::thread_rng();
        (0..40)
            .map(|_| format!("{:x}", rng.gen_range(0..16)))
            .collect()
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 将 Redis 命令序列化为 RESP 字节数组。
///
/// # 参数
/// - `cmd` — 要序列化的 [`Command`](crate::command::Command) 引用。
///
/// # 返回值
/// 命令对应的 RESP 编码字节序列，可直接通过网络发送给副本或主节点。
pub fn serialize_command_to_resp(cmd: &crate::command::Command) -> bytes::Bytes {
    let resp = cmd.to_resp_value();
    let parser = crate::protocol::RespParser::new();
    parser.encode(&resp)
}
