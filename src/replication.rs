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

/// 复制角色：主节点或从节点
#[derive(Debug, Clone)]
pub enum ReplicationRole {
    Master,
    Slave,
}

/// 副本连接状态
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaState {
    Online,
    Wait,
}

/// 副本信息
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub addr: String,
    pub port: u16,
    pub offset: i64,
    pub state: ReplicaState,
    pub last_ack_time: Instant,
    pub lag: i64,
}

/// 复制积压缓冲区（环形缓冲区）
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
    /// 创建新的复制积压缓冲区
    fn new(max_size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size),
            max_size,
            start_offset: 0,
            end_offset: 0,
        }
    }

    /// 追加数据，超出容量时从前端丢弃
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

    /// 从指定偏移量获取数据
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

/// 复制管理器，负责管理主从复制状态
#[derive(Debug)]
pub struct ReplicationManager {
    /// 当前角色
    role: RwLock<ReplicationRole>,
    /// 主节点复制 ID
    master_replid: RwLock<String>,
    /// 主节点复制偏移量
    master_repl_offset: AtomicI64,
    /// 主节点主机地址（从节点模式下有效）
    master_host: RwLock<Option<String>>,
    /// 主节点端口（从节点模式下有效）
    master_port: RwLock<Option<u16>>,
    /// 主节点连接状态（从节点模式下有效）
    master_link_up: AtomicBool,
    /// 最后一次收到主节点数据的时间
    master_last_io: RwLock<Option<Instant>>,
    /// 已连接的副本列表
    connected_replicas: RwLock<Vec<ReplicaInfo>>,
    /// 本机监听端口（用于 REPLCONF listening-port）
    listening_port: AtomicU16,
    /// 写命令广播通道（发送端）
    repl_tx: broadcast::Sender<bytes::Bytes>,
    /// 复制积压缓冲区
    backlog: RwLock<ReplicationBacklog>,
    /// 副本 ACK 通知（用于 WAIT 命令唤醒）
    ack_notify: Notify,
}

impl ReplicationManager {
    /// 创建新的复制管理器，默认以主节点身份启动
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

    /// 获取当前角色
    pub fn get_role(&self) -> ReplicationRole {
        let guard = self.role.read().unwrap();
        match *guard {
            ReplicationRole::Master => ReplicationRole::Master,
            ReplicationRole::Slave => ReplicationRole::Slave,
        }
    }

    /// 获取主节点复制 ID
    pub fn get_master_replid(&self) -> String {
        self.master_replid.read().unwrap().clone()
    }

    /// 获取主节点复制偏移量
    pub fn get_master_repl_offset(&self) -> i64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    /// 获取主节点主机和端口（从节点模式下有效）
    pub fn get_master_host_port(&self) -> (Option<String>, Option<u16>) {
        let host = self.master_host.read().unwrap().clone();
        let port = self.master_port.read().unwrap().clone();
        (host, port)
    }

    /// 获取已连接的副本列表快照
    pub fn get_connected_replicas(&self) -> Vec<ReplicaInfo> {
        self.connected_replicas.read().unwrap().clone()
    }

    /// 检查是否至少有一个已连接的副本
    pub fn has_connected_replicas(&self) -> bool {
        // 快速路径：读取锁检查副本列表是否为空
        !self.connected_replicas.read().unwrap().is_empty()
    }

    /// 设置本机监听端口
    pub fn set_listening_port(&self, port: u16) {
        self.listening_port.store(port, Ordering::Relaxed);
    }

    /// 获取本机监听端口
    pub fn get_listening_port(&self) -> u16 {
        self.listening_port.load(Ordering::Relaxed)
    }

    /// 设置为指定主节点的从节点
    pub fn set_replicaof(&self, host: String, port: u16) {
        let mut role = self.role.write().unwrap();
        let mut master_host = self.master_host.write().unwrap();
        let mut master_port = self.master_port.write().unwrap();
        *role = ReplicationRole::Slave;
        *master_host = Some(host);
        *master_port = Some(port);
        self.master_link_up.store(false, Ordering::Relaxed);
    }

    /// 取消从节点身份，恢复为主节点，并生成新的复制 ID
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

    /// 设置主节点连接状态
    pub fn set_master_link_up(&self, up: bool) {
        self.master_link_up.store(up, Ordering::Relaxed);
    }

    /// 更新最后一次收到主节点数据的时间
    pub fn touch_master_last_io(&self) {
        let mut guard = self.master_last_io.write().unwrap();
        *guard = Some(Instant::now());
    }

    /// 获取 INFO replication 段的格式化字符串
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

    /// 获取写命令广播发送端克隆
    pub fn get_repl_tx(&self) -> broadcast::Sender<bytes::Bytes> {
        self.repl_tx.clone()
    }

    /// 订阅写命令广播通道
    pub fn subscribe(&self) -> broadcast::Receiver<bytes::Bytes> {
        self.repl_tx.subscribe()
    }

    /// 注册已连接的副本
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

    /// 移除已连接的副本
    pub fn remove_replica(&self, addr: &str, port: u16) {
        let mut replicas = self.connected_replicas.write().unwrap();
        replicas.retain(|r| !(r.addr == addr && r.port == port));
    }

    /// 更新副本偏移量
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

    /// 增加主节点复制偏移量
    pub fn incr_master_repl_offset(&self, delta: i64) {
        self.master_repl_offset.fetch_add(delta, Ordering::Relaxed);
    }

    /// 向复制积压缓冲区追加数据
    pub fn append_to_backlog(&self, data: &[u8]) {
        let mut backlog = self.backlog.write().unwrap();
        backlog.append(data);
    }

    /// 从指定偏移量获取积压数据
    pub fn get_backlog_from_offset(&self, offset: i64) -> Option<Vec<u8>> {
        let backlog = self.backlog.read().unwrap();
        backlog.get_data_from_offset(offset)
    }

    /// 统计 offset >= target_offset 的副本数量
    pub fn count_replicas_at_offset(&self, target_offset: i64) -> i64 {
        let replicas = self.connected_replicas.read().unwrap();
        replicas.iter().filter(|r| r.offset >= target_offset).count() as i64
    }

    /// 等待指定数量的副本确认写入到 target_offset
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

    /// 启动从节点复制，外层循环负责断线重连（指数退避）
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
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
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

    /// 设置主节点复制 ID 和偏移量（用于从 RDB 恢复）
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

/// 将命令序列化为 RESP 字节数组
pub fn serialize_command_to_resp(cmd: &crate::command::Command) -> bytes::Bytes {
    let resp = cmd.to_resp_value();
    let parser = crate::protocol::RespParser::new();
    parser.encode(&resp)
}
