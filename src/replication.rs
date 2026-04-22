use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicU16, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use rand::Rng;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use crate::error::{AppError, Result};
use crate::storage::StorageEngine;

/// 复制角色：主节点或从节点
#[derive(Debug, Clone)]
pub enum ReplicationRole {
    Master,
    Slave,
}

/// 副本信息
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub addr: String,
    pub port: u16,
    pub offset: i64,
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
        for &byte in data {
            if self.buffer.len() >= self.max_size {
                self.buffer.pop_front();
                self.start_offset += 1;
            }
            self.buffer.push_back(byte);
        }
        self.end_offset += data.len() as i64;
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
    /// 已连接的副本列表
    connected_replicas: RwLock<Vec<ReplicaInfo>>,
    /// 本机监听端口（用于 REPLCONF listening-port）
    listening_port: AtomicU16,
    /// 写命令广播通道（发送端）
    repl_tx: broadcast::Sender<bytes::Bytes>,
    /// 复制积压缓冲区
    backlog: RwLock<ReplicationBacklog>,
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
            connected_replicas: RwLock::new(Vec::new()),
            listening_port: AtomicU16::new(0),
            repl_tx,
            backlog: RwLock::new(ReplicationBacklog::new(1_048_576)),
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
            info.push_str("master_link_status:down\r\n");
        } else {
            let replicas = self.get_connected_replicas();
            info.push_str(&format!("connected_slaves:{}\r\n", replicas.len()));
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
        replicas.push(ReplicaInfo { addr, port, offset: 0 });
    }

    /// 移除已连接的副本
    pub fn remove_replica(&self, addr: &str, port: u16) {
        let mut replicas = self.connected_replicas.write().unwrap();
        replicas.retain(|r| !(r.addr == addr && r.port == port));
    }

    /// 更新副本偏移量
    pub fn update_replica_offset(&self, addr: &str, port: u16, offset: i64) {
        let mut replicas = self.connected_replicas.write().unwrap();
        for r in replicas.iter_mut() {
            if r.addr == addr && r.port == port {
                r.offset = offset;
                break;
            }
        }
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

    /// 启动从节点复制，与主节点建立连接并完成全量同步
    pub async fn start_replication(
        &self,
        storage: StorageEngine,
        master_host: String,
        master_port: u16,
    ) -> Result<()> {
        let addr = format!("{}:{}", master_host, master_port);
        log::info!("开始连接主节点: {}", addr);

        let mut stream = TcpStream::connect(&addr).await.map_err(AppError::Io)?;
        let (mut read_half, mut write_half) = stream.split();
        let mut reader = tokio::io::BufReader::new(&mut read_half);

        // 1. 发送 PING
        write_half.write_all(b"*1\r\n$4\r\nPING\r\n").await.map_err(AppError::Io)?;
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
        write_half.write_all(cmd.as_bytes()).await.map_err(AppError::Io)?;
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

        // 4. 发送 PSYNC ? -1
        write_half
            .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
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

            // 7. 保存到临时文件并加载
            let temp_path = format!("temp_rdb_{}.rdb", std::process::id());
            {
                let mut file = std::fs::File::create(&temp_path).map_err(AppError::Io)?;
                file.write_all(&rdb_data).map_err(AppError::Io)?;
            }
            crate::rdb::load(&storage, &temp_path)?;
            let _ = std::fs::remove_file(&temp_path);

            // 8. 更新复制状态
            {
                let mut master_replid_guard = self.master_replid.write().unwrap();
                *master_replid_guard = replid.clone();
            }
            self.master_repl_offset.store(offset, Ordering::Relaxed);

            log::info!("全量同步完成，replid: {}, offset: {}", replid, offset);
        } else if line.trim().starts_with("+CONTINUE") {
            // 增量同步 - 直接进入命令接收循环
            log::info!("增量同步开始");
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
        let executor = crate::command::CommandExecutor::new(storage);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));

        loop {
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
                            return Ok(());
                        }
                        Ok(n) => {
                            log::debug!("从主节点读取 {} 字节", n);
                        }
                        Err(e) => {
                            log::error!("读取主节点数据失败: {}", e);
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
