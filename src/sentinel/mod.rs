// Sentinel 模块

pub mod monitor;
pub mod discovery;
pub mod failover;

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

/// 被监控的 Redis 实例信息
#[derive(Debug, Clone)]
pub struct SentinelInstance {
    /// 实例名称（master name）
    pub name: String,
    /// IP 地址
    pub ip: String,
    /// 端口
    pub port: u16,
    /// quorum（判定客观下线所需的 Sentinel 数量）
    pub quorum: u32,
    /// down-after-milliseconds（判定主观下线的超时时间）
    pub down_after_ms: u64,
    /// 当前是否主观下线
    pub sdown: bool,
    /// 当前是否客观下线
    pub odown: bool,
    /// 最后一次 PING 响应时间
    pub last_ping_reply: Option<Instant>,
    /// 已发现的副本列表
    pub replicas: Vec<ReplicaInstance>,
    /// 已发现的其他 Sentinel 列表
    pub sentinels: Vec<SentinelPeer>,
}

/// 副本实例信息
#[derive(Debug, Clone)]
pub struct ReplicaInstance {
    pub ip: String,
    pub port: u16,
    pub offset: i64,
    pub last_ping_reply: Option<Instant>,
    pub sdown: bool,
}

/// 其他 Sentinel 节点信息
#[derive(Debug, Clone)]
pub struct SentinelPeer {
    pub ip: String,
    pub port: u16,
    pub runid: String,
    pub last_hello_time: Option<Instant>,
}

/// Sentinel 管理器
#[derive(Debug)]
pub struct SentinelManager {
    /// 被监控的 master 列表（name -> instance）
    masters: RwLock<HashMap<String, SentinelInstance>>,
    /// 本 Sentinel 的 runid
    pub runid: String,
}

impl SentinelManager {
    /// 创建新的 Sentinel 管理器
    pub fn new() -> Self {
        Self {
            masters: RwLock::new(HashMap::new()),
            runid: Self::generate_runid(),
        }
    }

    /// 添加监控的 master
    pub fn monitor(&self, name: String, ip: String, port: u16, quorum: u32) {
        let instance = SentinelInstance {
            name: name.clone(),
            ip,
            port,
            quorum,
            down_after_ms: 30000,
            sdown: false,
            odown: false,
            last_ping_reply: None,
            replicas: Vec::new(),
            sentinels: Vec::new(),
        };
        let mut masters = self.masters.write().unwrap();
        masters.insert(name, instance);
    }

    /// 获取所有被监控的 master
    pub fn get_masters(&self) -> Vec<SentinelInstance> {
        let masters = self.masters.read().unwrap();
        masters.values().cloned().collect()
    }

    /// 获取指定 master
    pub fn get_master(&self, name: &str) -> Option<SentinelInstance> {
        let masters = self.masters.read().unwrap();
        masters.get(name).cloned()
    }

    /// 移除监控的 master
    pub fn remove(&self, name: &str) -> bool {
        let mut masters = self.masters.write().unwrap();
        masters.remove(name).is_some()
    }

    /// 获取 master 的地址
    pub fn get_master_addr_by_name(&self, name: &str) -> Option<(String, u16)> {
        let masters = self.masters.read().unwrap();
        masters.get(name).map(|m| (m.ip.clone(), m.port))
    }

    /// 更新 master 的最后 PING 响应时间
    pub fn update_last_ping_reply(&self, name: &str) {
        let mut masters = self.masters.write().unwrap();
        if let Some(master) = masters.get_mut(name) {
            master.last_ping_reply = Some(Instant::now());
        }
    }

    /// 更新 master 的 replica 列表
    pub fn update_replicas(&self, name: &str, replicas: Vec<ReplicaInstance>) {
        let mut masters = self.masters.write().unwrap();
        if let Some(master) = masters.get_mut(name) {
            master.replicas = replicas;
        }
    }

    /// 设置 master 的 ODOWN 状态
    pub fn set_odown(&self, name: &str, odown: bool) {
        let mut masters = self.masters.write().unwrap();
        if let Some(master) = masters.get_mut(name) {
            master.odown = odown;
        }
    }

    /// 更新 master 的地址（故障转移后使用）
    pub fn update_master_addr(&self, name: &str, new_ip: String, new_port: u16) {
        let mut masters = self.masters.write().unwrap();
        if let Some(master) = masters.get_mut(name) {
            let old_ip = master.ip.clone();
            let old_port = master.port;
            master.ip = new_ip.clone();
            master.port = new_port;
            master.sdown = false;
            master.odown = false;
            master.last_ping_reply = None;
            // 清空旧的 replica 列表，等待下次 INFO replication 重新发现
            master.replicas.clear();
            log::info!(
                "master {} 地址已更新: {}:{} -> {}:{}",
                name, old_ip, old_port, new_ip, new_port
            );
        }
    }

    /// 检查并更新所有 master 的 SDOWN 状态
    pub fn check_sdown(&self) {
        let mut masters = self.masters.write().unwrap();
        let now = Instant::now();
        for master in masters.values_mut() {
            let was_sdown = master.sdown;
            match master.last_ping_reply {
                Some(last) => {
                    let elapsed = now.duration_since(last).as_millis() as u64;
                    master.sdown = elapsed > master.down_after_ms;
                }
                None => {
                    // 从未收到过 PING 响应，视为 SDOWN
                    master.sdown = true;
                }
            }
            if master.sdown && !was_sdown {
                log::warn!("Sentinel: master {} ({}:{}) 进入主观下线 (SDOWN)", master.name, master.ip, master.port);
            } else if !master.sdown && was_sdown {
                log::info!("Sentinel: master {} ({}:{}) 退出主观下线", master.name, master.ip, master.port);
            }
        }
    }

    /// 设置 master 的 down-after-milliseconds
    pub fn set_down_after_ms(&self, name: &str, ms: u64) {
        let mut masters = self.masters.write().unwrap();
        if let Some(master) = masters.get_mut(name) {
            master.down_after_ms = ms;
        }
    }

    /// 更新或添加 Sentinel 节点信息
    pub fn update_sentinel_peer(&self, master_name: &str, peer: SentinelPeer) {
        let mut masters = self.masters.write().unwrap();
        if let Some(master) = masters.get_mut(master_name) {
            // 查找是否已存在该 Sentinel
            if let Some(existing) = master.sentinels.iter_mut().find(|s| s.runid == peer.runid) {
                existing.ip = peer.ip;
                existing.port = peer.port;
                existing.last_hello_time = peer.last_hello_time;
            } else {
                log::info!("发现新 Sentinel: {}:{} (runid={})", peer.ip, peer.port, peer.runid);
                master.sentinels.push(peer);
            }
        }
    }

    /// 生成 40 字符随机十六进制 runid
    fn generate_runid() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..40).map(|_| format!("{:x}", rng.gen_range(0..16))).collect()
    }
}

impl Default for SentinelManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 检查命令是否在 Sentinel 模式下允许
pub fn is_sentinel_allowed_command(cmd: &crate::command::Command) -> bool {
    use crate::command::Command;
    matches!(cmd,
        Command::Ping(_)
        | Command::Info(_)
        | Command::Subscribe(_)
        | Command::Unsubscribe(_)
        | Command::PSubscribe(_)
        | Command::PUnsubscribe(_)
        | Command::Publish(_, _)
        | Command::Quit
        | Command::Reset
        | Command::Auth(_, _)
        | Command::Hello(_, _, _)
        | Command::ClientSetName(_)
        | Command::ClientGetName
        | Command::ClientList
        | Command::ClientId
        | Command::ClientInfo
        | Command::CommandInfo
        | Command::CommandCount
        | Command::CommandList(_)
        | Command::CommandDocs(_)
        | Command::Shutdown(_)
        | Command::SentinelMasters
        | Command::SentinelMaster(_)
        | Command::SentinelReplicas(_)
        | Command::SentinelSentinels(_)
        | Command::SentinelGetMasterAddrByName(_)
        | Command::SentinelMonitor { .. }
        | Command::SentinelRemove(_)
        | Command::SentinelSet { .. }
        | Command::SentinelFailover(_)
        | Command::SentinelReset(_)
        | Command::SentinelCkquorum(_)
        | Command::SentinelMyId
    )
}
