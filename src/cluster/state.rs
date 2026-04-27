//! Cluster 状态管理

use std::collections::HashMap;
use std::sync::RwLock;

/// 16384 个 slot
pub const CLUSTER_SLOTS: usize = 16384;

/// 节点标志
#[derive(Debug, Clone, PartialEq)]
pub enum NodeFlag {
    Master,
    Slave,
    Myself,
    Fail,
    PFail,
    Handshake,
    NoAddr,
}

/// 集群节点信息
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// 节点 ID（40 字符十六进制）
    pub id: String,
    /// IP 地址
    pub ip: String,
    /// 数据端口
    pub port: u16,
    /// 集群总线端口（数据端口 + 10000）
    pub bus_port: u16,
    /// 节点标志
    pub flags: Vec<NodeFlag>,
    /// 主节点 ID（如果是从节点）
    pub master_id: Option<String>,
    /// 负责的 slot 列表
    pub slots: Vec<bool>, // slots[i] = true 表示负责 slot i
    /// 最后一次 PING 发送时间（毫秒时间戳）
    pub ping_sent: u64,
    /// 最后一次 PONG 接收时间（毫秒时间戳）
    pub pong_recv: u64,
    /// 当前 epoch
    pub config_epoch: u64,
}

impl ClusterNode {
    /// 创建新节点
    pub fn new(id: String, ip: String, port: u16) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            id,
            ip,
            port,
            bus_port: port + 10000,
            flags: vec![NodeFlag::Master],
            master_id: None,
            slots: vec![false; CLUSTER_SLOTS],
            ping_sent: 0,
            pong_recv: now,
            config_epoch: 0,
        }
    }

    /// 分配 slot 给此节点
    pub fn add_slot(&mut self, slot: usize) {
        if slot < CLUSTER_SLOTS {
            self.slots[slot] = true;
        }
    }

    /// 移除此节点的 slot
    pub fn del_slot(&mut self, slot: usize) {
        if slot < CLUSTER_SLOTS {
            self.slots[slot] = false;
        }
    }

    /// 获取此节点负责的所有 slot
    pub fn get_slots(&self) -> Vec<usize> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, v)| **v)
            .map(|(i, _)| i)
            .collect()
    }

    /// 获取此节点负责的 slot 数量
    pub fn slot_count(&self) -> usize {
        self.slots.iter().filter(|&&v| v).count()
    }

    /// 检查节点是否负责指定 slot
    pub fn has_slot(&self, slot: usize) -> bool {
        slot < CLUSTER_SLOTS && self.slots[slot]
    }

    /// 获取节点标志字符串
    pub fn flags_string(&self) -> String {
        self.flags
            .iter()
            .map(|f| match f {
                NodeFlag::Master => "master",
                NodeFlag::Slave => "slave",
                NodeFlag::Myself => "myself",
                NodeFlag::Fail => "fail",
                NodeFlag::PFail => "fail?",
                NodeFlag::Handshake => "handshake",
                NodeFlag::NoAddr => "noaddr",
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Slot 范围（用于 CLUSTER SLOTS 输出）
#[derive(Debug, Clone)]
pub struct SlotRange {
    pub start: usize,
    pub end: usize,
    pub node_id: String,
}

/// 集群状态
#[derive(Debug)]
pub struct ClusterState {
    /// 本节点 ID
    myself_id: RwLock<String>,
    /// 所有已知节点（node_id -> ClusterNode）
    nodes: RwLock<HashMap<String, ClusterNode>>,
    /// slot 分配表：slot_index -> node_id
    slot_assignment: RwLock<Box<[Option<String>; CLUSTER_SLOTS]>>,
    /// 当前 epoch
    current_epoch: RwLock<u64>,
    /// 集群是否处于 OK 状态
    cluster_ok: RwLock<bool>,
    /// 正在迁入的 slot（slot -> 源节点 ID）
    importing_slots: RwLock<HashMap<usize, String>>,
    /// 正在迁出的 slot（slot -> 目标节点 ID）
    migrating_slots: RwLock<HashMap<usize, String>>,
}

impl ClusterState {
    /// 创建新的集群状态
    pub fn new(my_ip: String, my_port: u16) -> Self {
        let my_id = Self::generate_node_id();
        let mut my_node = ClusterNode::new(my_id.clone(), my_ip, my_port);
        my_node.flags.push(NodeFlag::Myself);

        let mut nodes = HashMap::new();
        nodes.insert(my_id.clone(), my_node);

        Self {
            myself_id: RwLock::new(my_id),
            nodes: RwLock::new(nodes),
            slot_assignment: RwLock::new(Box::new([const { None }; CLUSTER_SLOTS])),
            current_epoch: RwLock::new(0),
            cluster_ok: RwLock::new(false),
            importing_slots: RwLock::new(HashMap::new()),
            migrating_slots: RwLock::new(HashMap::new()),
        }
    }

    /// 获取本节点 ID
    pub fn myself_id(&self) -> String {
        self.myself_id.read().unwrap().clone()
    }

    /// 获取本节点信息
    pub fn myself(&self) -> Option<ClusterNode> {
        let id = self.myself_id();
        let nodes = self.nodes.read().unwrap();
        nodes.get(&id).cloned()
    }

    /// 获取所有节点
    pub fn get_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().unwrap().values().cloned().collect()
    }

    /// 获取节点写锁
    pub fn nodes_write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, ClusterNode>> {
        self.nodes.write().unwrap()
    }

    /// 获取指定节点
    pub fn get_node(&self, id: &str) -> Option<ClusterNode> {
        self.nodes.read().unwrap().get(id).cloned()
    }

    /// 添加节点（CLUSTER MEET）
    pub fn add_node(&self, node: ClusterNode) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.insert(node.id.clone(), node);
    }

    /// 移除节点
    pub fn remove_node(&self, id: &str) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.remove(id);
    }

    /// 分配 slot 给指定节点
    pub fn assign_slot(&self, slot: usize, node_id: &str) -> bool {
        if slot >= CLUSTER_SLOTS {
            return false;
        }
        let mut assignment = self.slot_assignment.write().unwrap();
        let old_id = assignment[slot].clone();
        assignment[slot] = Some(node_id.to_string());
        drop(assignment); // 释放锁，避免死锁

        let mut nodes = self.nodes.write().unwrap();
        // 从旧节点移除 slot
        if let Some(ref old) = old_id
            && old != node_id
            && let Some(node) = nodes.get_mut(old)
        {
            node.del_slot(slot);
        }
        if let Some(node) = nodes.get_mut(node_id) {
            node.add_slot(slot);
        }
        true
    }

    /// 取消 slot 分配
    pub fn unassign_slot(&self, slot: usize) -> bool {
        if slot >= CLUSTER_SLOTS {
            return false;
        }
        let mut assignment = self.slot_assignment.write().unwrap();
        if let Some(ref old_id) = assignment[slot] {
            let old_id = old_id.clone();
            assignment[slot] = None;
            let mut nodes = self.nodes.write().unwrap();
            if let Some(node) = nodes.get_mut(&old_id) {
                node.del_slot(slot);
            }
        }
        true
    }

    /// 查询 slot 分配给哪个节点
    pub fn get_slot_node(&self, slot: usize) -> Option<String> {
        if slot >= CLUSTER_SLOTS {
            return None;
        }
        let assignment = self.slot_assignment.read().unwrap();
        assignment[slot].clone()
    }

    /// 查询 key 对应的 slot
    pub fn key_slot(key: &str) -> usize {
        // 支持 hash tag: {tag}key 中 {tag} 部分决定 slot
        let hash_key = if let Some(start) = key.find('{') {
            if let Some(end) = key[start + 1..].find('}') {
                let tag = &key[start + 1..start + 1 + end];
                if !tag.is_empty() { tag } else { key }
            } else {
                key
            }
        } else {
            key
        };
        crc16(hash_key.as_bytes()) as usize % CLUSTER_SLOTS
    }

    /// 查询 key 应该由哪个节点处理
    pub fn get_node_for_key(&self, key: &str) -> Option<ClusterNode> {
        let slot = Self::key_slot(key);
        let node_id = self.get_slot_node(slot)?;
        self.get_node(&node_id)
    }

    /// 检查 key 是否属于本节点
    pub fn is_my_slot(&self, key: &str) -> bool {
        let slot = Self::key_slot(key);
        let my_id = self.myself_id();
        match self.get_slot_node(slot) {
            Some(id) => id == my_id,
            None => false,
        }
    }

    /// 获取集群是否 OK
    pub fn is_cluster_ok(&self) -> bool {
        *self.cluster_ok.read().unwrap()
    }

    /// 设置集群状态
    pub fn set_cluster_ok(&self, ok: bool) {
        *self.cluster_ok.write().unwrap() = ok;
    }

    /// 更新节点的 pong_recv 时间
    pub fn update_pong_recv(&self, ip: &str, port: u16, timestamp: u64) {
        let mut nodes = self.nodes.write().unwrap();
        for node in nodes.values_mut() {
            if node.ip == ip && node.port == port {
                node.pong_recv = timestamp;
                break;
            }
        }
    }

    /// 给节点添加标志
    pub fn set_node_flag(&self, node_id: &str, flag: NodeFlag) {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id)
            && !node.flags.contains(&flag)
        {
            node.flags.push(flag);
        }
    }

    /// 移除节点标志
    pub fn remove_node_flag(&self, node_id: &str, flag: &NodeFlag) {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id) {
            node.flags.retain(|f| f != flag);
        }
    }

    /// 获取当前 epoch
    pub fn get_current_epoch(&self) -> u64 {
        *self.current_epoch.read().unwrap()
    }

    /// 递增 epoch
    pub fn incr_epoch(&self) -> u64 {
        let mut epoch = self.current_epoch.write().unwrap();
        *epoch += 1;
        *epoch
    }

    /// 设置节点 epoch
    pub fn set_node_epoch(&self, node_id: &str, epoch: u64) {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id)
            && epoch > node.config_epoch
        {
            node.config_epoch = epoch;
        }
    }

    /// 设置当前 epoch（当收到更高 epoch 时更新）
    pub fn set_current_epoch(&self, epoch: u64) {
        let mut current = self.current_epoch.write().unwrap();
        if epoch > *current {
            *current = epoch;
        }
    }

    /// 设置本节点 ID（加载 nodes.conf 时使用）
    pub fn set_myself_id(&self, new_id: String) {
        let mut myself_id = self.myself_id.write().unwrap();
        let old_id = myself_id.clone();
        if old_id == new_id {
            return;
        }
        *myself_id = new_id.clone();
        drop(myself_id);

        let mut nodes = self.nodes.write().unwrap();
        if let Some(mut old_node) = nodes.remove(&old_id) {
            old_node.id = new_id.clone();
            nodes.insert(new_id.clone(), old_node);
        }
        drop(nodes);

        // 更新 slot 分配表中的旧 ID
        let mut assignment = self.slot_assignment.write().unwrap();
        for slot_opt in assignment.iter_mut() {
            if let Some(id) = slot_opt
                && id == &old_id
            {
                *slot_opt = Some(new_id.clone());
            }
        }
    }

    /// 将从节点提升为 master（故障转移时使用）
    pub fn promote_replica_to_master(&self, failed_master_id: &str) -> bool {
        let my_id = self.myself_id();
        let myself = self.myself();

        let myself = match myself {
            Some(n) => n,
            None => return false,
        };

        // 检查本节点是否是指定 master 的从节点
        if myself.master_id.as_ref() != Some(&failed_master_id.to_string()) {
            return false;
        }

        // 从 slot_assignment 表获取故障 master 的 slot
        let failed_slots = self.slots_for_node(failed_master_id);
        if failed_slots.is_empty() {
            return false;
        }

        let new_epoch = self.incr_epoch();

        // 更新本节点为 master
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(&my_id) {
            node.master_id = None;
            node.flags.retain(|f| *f != NodeFlag::Slave);
            if !node.flags.contains(&NodeFlag::Master) {
                node.flags.push(NodeFlag::Master);
            }
            node.slots = vec![false; CLUSTER_SLOTS];
            for &slot in &failed_slots {
                node.add_slot(slot);
            }
            node.config_epoch = new_epoch;
        }
        drop(nodes);

        // 更新 slot 分配表
        let mut assignment = self.slot_assignment.write().unwrap();
        for slot in &failed_slots {
            assignment[*slot] = Some(my_id.clone());
        }

        true
    }

    /// 保存集群拓扑到 nodes.conf 文件
    pub fn save_nodes_conf(&self, path: &str) -> std::io::Result<()> {
        let nodes = self.nodes.read().unwrap();
        let mut lines = Vec::new();

        for node in nodes.values() {
            let mut parts = Vec::new();
            parts.push(node.id.clone());
            parts.push(format!("{}:{}@{}", node.ip, node.port, node.bus_port));
            parts.push(node.flags_string());
            parts.push(node.master_id.clone().unwrap_or_else(|| "-".to_string()));
            parts.push(node.ping_sent.to_string());
            parts.push(node.pong_recv.to_string());
            parts.push(node.config_epoch.to_string());

            // 压缩 slot 为范围表示
            let slots = node.get_slots();
            if !slots.is_empty() {
                let mut ranges = Vec::new();
                let mut i = 0;
                while i < slots.len() {
                    let start = slots[i];
                    let mut end = start;
                    while i + 1 < slots.len() && slots[i + 1] == end + 1 {
                        end += 1;
                        i += 1;
                    }
                    if start == end {
                        ranges.push(start.to_string());
                    } else {
                        ranges.push(format!("{}-{}", start, end));
                    }
                    i += 1;
                }
                parts.push(ranges.join(" "));
            }

            lines.push(parts.join(" "));
        }

        let content = lines.join("\n") + "\n";
        std::fs::write(path, content)
    }

    /// 更新节点拓扑信息（从总线 UPDATE 消息使用）
    pub fn update_node_topology(
        &self,
        node_id: &str,
        flags: Vec<NodeFlag>,
        master_id: Option<String>,
        epoch: u64,
        slots: Vec<usize>,
    ) {
        let my_id = self.myself_id();
        if node_id == my_id {
            return;
        }
        // 过滤掉 Myself flag，它不应该从远程节点传播
        let mut flags: Vec<NodeFlag> = flags
            .into_iter()
            .filter(|f| *f != NodeFlag::Myself)
            .collect();
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(node_id) {
            if epoch < node.config_epoch {
                return;
            }
            // 保留本地的 PFAIL/FAIL 标记，不被远程 gossip 覆盖
            if node.flags.contains(&NodeFlag::PFail) && !flags.contains(&NodeFlag::PFail) {
                flags.push(NodeFlag::PFail);
            }
            if node.flags.contains(&NodeFlag::Fail) && !flags.contains(&NodeFlag::Fail) {
                flags.push(NodeFlag::Fail);
            }
            node.flags = flags;
            node.master_id = master_id;
            node.config_epoch = epoch;
            node.slots = vec![false; CLUSTER_SLOTS];
            for slot in slots {
                if slot < CLUSTER_SLOTS {
                    node.add_slot(slot);
                }
            }
        }
    }

    /// 从 nodes.conf 文件加载集群拓扑
    pub fn load_nodes_conf(&self, path: &str) -> std::io::Result<()> {
        let content = std::fs::read_to_string(path)?;

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 7 {
                continue; // 至少需要 id addr flags master_id ping pong epoch
            }

            let node_id = parts[0].to_string();
            let addr = parts[1];
            let flags_str = parts[2];
            let master_id_str = parts[3];
            let ping_sent: u64 = parts[4].parse().unwrap_or(0);
            let pong_recv: u64 = parts[5].parse().unwrap_or(0);
            let config_epoch: u64 = parts[6].parse().unwrap_or(0);

            // 解析地址 ip:port@bus_port
            let (ip, port, bus_port) = parse_node_addr(addr);

            let flags = parse_node_flags(flags_str);
            let is_myself = flags.contains(&NodeFlag::Myself);

            let master_id = if master_id_str == "-" {
                None
            } else {
                Some(master_id_str.to_string())
            };

            if is_myself {
                self.set_myself_id(node_id.clone());
                let mut nodes = self.nodes.write().unwrap();
                if let Some(node) = nodes.get_mut(&node_id) {
                    node.ip = ip;
                    node.port = port;
                    node.bus_port = bus_port;
                    node.ping_sent = ping_sent;
                    node.pong_recv = pong_recv;
                    node.config_epoch = config_epoch;
                    node.master_id = master_id.clone();
                    node.flags = flags;

                    // 加载 slot
                    node.slots = vec![false; CLUSTER_SLOTS];
                    for item in parts.iter().skip(7) {
                        parse_slot_range(item, |slot| {
                            if slot < CLUSTER_SLOTS {
                                node.add_slot(slot);
                            }
                        });
                    }
                }
                drop(nodes);
            } else {
                let mut node = ClusterNode::new(node_id.clone(), ip, port);
                node.bus_port = bus_port;
                node.flags = flags;
                node.master_id = master_id.clone();
                node.ping_sent = ping_sent;
                node.pong_recv = pong_recv;
                node.config_epoch = config_epoch;

                for item in parts.iter().skip(7) {
                    parse_slot_range(item, |slot| {
                        if slot < CLUSTER_SLOTS {
                            node.add_slot(slot);
                        }
                    });
                }

                self.add_node(node);
            }

            // 更新 slot 分配表
            if is_myself || master_id.is_none() {
                let node = self.get_node(&node_id);
                if let Some(ref n) = node {
                    for slot in n.get_slots() {
                        let mut assignment = self.slot_assignment.write().unwrap();
                        assignment[slot] = Some(node_id.clone());
                    }
                }
            }
        }

        Ok(())
    }

    /// 根据 ClusterMessage 更新本地节点信息
    pub fn update_from_message(&self, msg: &super::protocol::ClusterMessage, peer_ip: &str) {
        let node_id = msg.sender_id.clone();
        let port = msg.sender_port;

        // 如果发送方节点不存在，创建新节点
        if self.get_node(&node_id).is_none() {
            let new_node = ClusterNode::new(node_id.clone(), peer_ip.to_string(), port);
            self.add_node(new_node);
        }

        // 更新发送方节点 epoch
        self.set_node_epoch(&node_id, msg.current_epoch);

        // 更新 slot 分配
        for slot_info in &msg.slots {
            for slot in slot_info.slot_start..=slot_info.slot_end {
                self.assign_slot(slot as usize, &slot_info.node_id);
            }
        }

        // 更新消息中携带的其他节点拓扑
        for node_info in &msg.nodes {
            if self.get_node(&node_info.node_id).is_none() {
                let ip_str = super::protocol::format_ip(&node_info.ip);
                let mut new_node =
                    ClusterNode::new(node_info.node_id.clone(), ip_str, node_info.port);
                new_node.flags = super::protocol::decode_flags(node_info.flags);
                self.add_node(new_node);
            }
        }
    }

    /// 获取已分配的 slot 数量
    pub fn assigned_slots_count(&self) -> usize {
        let assignment = self.slot_assignment.read().unwrap();
        assignment.iter().filter(|s| s.is_some()).count()
    }

    /// 从 slot_assignment 表查询指定节点拥有的 slot 数量
    pub fn slots_count_for_node(&self, node_id: &str) -> usize {
        let assignment = self.slot_assignment.read().unwrap();
        assignment
            .iter()
            .filter(|s| s.as_deref() == Some(node_id))
            .count()
    }

    /// 从 slot_assignment 表查询指定节点拥有的 slot 列表
    pub fn slots_for_node(&self, node_id: &str) -> Vec<usize> {
        let assignment = self.slot_assignment.read().unwrap();
        assignment
            .iter()
            .enumerate()
            .filter(|(_, s)| s.as_deref() == Some(node_id))
            .map(|(i, _)| i)
            .collect()
    }

    /// 获取集群信息字符串（用于 CLUSTER INFO）
    pub fn get_info_string(&self) -> String {
        let nodes = self.nodes.read().unwrap();
        let assigned = self.assigned_slots_count();
        let ok = assigned == CLUSTER_SLOTS;

        let mut info = String::new();
        info.push_str("cluster_enabled:1\r\n");
        info.push_str(&format!(
            "cluster_state:{}\r\n",
            if ok { "ok" } else { "fail" }
        ));
        info.push_str(&format!("cluster_slots_assigned:{}\r\n", assigned));
        info.push_str(&format!("cluster_slots_ok:{}\r\n", assigned));
        info.push_str("cluster_slots_pfail:0\r\n");
        info.push_str("cluster_slots_fail:0\r\n");
        info.push_str(&format!("cluster_known_nodes:{}\r\n", nodes.len()));
        info.push_str(&format!(
            "cluster_size:{}\r\n",
            nodes
                .values()
                .filter(|n| n.flags.contains(&NodeFlag::Master) && n.slot_count() > 0)
                .count()
        ));
        info.push_str(&format!(
            "cluster_current_epoch:{}\r\n",
            self.get_current_epoch()
        ));
        info.push_str(&format!(
            "cluster_my_epoch:{}\r\n",
            self.get_current_epoch()
        ));
        info
    }

    /// 设置 slot 为 IMPORTING 状态
    pub fn set_slot_importing(&self, slot: usize, source_node_id: String) {
        let mut importing = self.importing_slots.write().unwrap();
        importing.insert(slot, source_node_id);
    }

    /// 设置 slot 为 MIGRATING 状态
    pub fn set_slot_migrating(&self, slot: usize, target_node_id: String) {
        let mut migrating = self.migrating_slots.write().unwrap();
        migrating.insert(slot, target_node_id);
    }

    /// 清除 slot 的迁移状态
    pub fn set_slot_stable(&self, slot: usize) {
        let mut importing = self.importing_slots.write().unwrap();
        let mut migrating = self.migrating_slots.write().unwrap();
        importing.remove(&slot);
        migrating.remove(&slot);
    }

    /// 检查 slot 是否正在迁出
    pub fn is_slot_migrating(&self, slot: usize) -> Option<String> {
        let migrating = self.migrating_slots.read().unwrap();
        migrating.get(&slot).cloned()
    }

    /// 检查 slot 是否正在迁入
    pub fn is_slot_importing(&self, slot: usize) -> Option<String> {
        let importing = self.importing_slots.read().unwrap();
        importing.get(&slot).cloned()
    }

    /// 生成 40 字符随机十六进制节点 ID
    pub fn generate_node_id() -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..40)
            .map(|_| format!("{:x}", rng.gen_range(0..16)))
            .collect()
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new("127.0.0.1".to_string(), 6379)
    }
}

/// 解析节点地址 ip:port@bus_port
pub(crate) fn parse_node_addr(addr: &str) -> (String, u16, u16) {
    let mut ip = "127.0.0.1".to_string();
    let mut port = 6379u16;
    let mut bus_port = 16379u16;

    if let Some(at_pos) = addr.find('@') {
        let main = &addr[..at_pos];
        let bus = &addr[at_pos + 1..];
        bus_port = bus.parse().unwrap_or(port + 10000);
        if let Some(colon_pos) = main.find(':') {
            ip = main[..colon_pos].to_string();
            port = main[colon_pos + 1..].parse().unwrap_or(6379);
        }
    } else if let Some(colon_pos) = addr.find(':') {
        ip = addr[..colon_pos].to_string();
        port = addr[colon_pos + 1..].parse().unwrap_or(6379);
        bus_port = port + 10000;
    }

    (ip, port, bus_port)
}

/// 解析节点标志字符串
pub(crate) fn parse_node_flags(flags_str: &str) -> Vec<NodeFlag> {
    let mut flags = Vec::new();
    for f in flags_str.split(',') {
        match f.trim() {
            "master" => flags.push(NodeFlag::Master),
            "slave" => flags.push(NodeFlag::Slave),
            "myself" => flags.push(NodeFlag::Myself),
            "fail" => flags.push(NodeFlag::Fail),
            "fail?" => flags.push(NodeFlag::PFail),
            "handshake" => flags.push(NodeFlag::Handshake),
            "noaddr" => flags.push(NodeFlag::NoAddr),
            _ => {}
        }
    }
    flags
}

/// 解析 slot 范围字符串并执行回调
pub(crate) fn parse_slot_range<F>(s: &str, mut callback: F)
where
    F: FnMut(usize),
{
    if let Some(dash_pos) = s.find('-') {
        let start: usize = s[..dash_pos].parse().unwrap_or(0);
        let end: usize = s[dash_pos + 1..].parse().unwrap_or(0);
        for slot in start..=end {
            callback(slot);
        }
    } else {
        let slot: usize = s.parse().unwrap_or(0);
        callback(slot);
    }
}

/// CRC16-CCITT 实现（Redis Cluster 使用）
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc = ((crc << 8) & 0xFF00) ^ CRC16_TAB[((crc >> 8) as u8 ^ byte) as usize];
    }
    crc
}

/// CRC16-CCITT 查找表
const CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_nodes_conf_path(name: &str) -> String {
        let mut path = std::env::temp_dir();
        path.push(format!("redis_rust_{}_{}.conf", name, std::process::id()));
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_crc16() {
        // Redis 官方测试向量
        assert_eq!(
            crc16(b"123456789") % CLUSTER_SLOTS as u16,
            12739 % CLUSTER_SLOTS as u16
        );
    }

    #[test]
    fn test_key_slot() {
        // 普通 key
        let slot1 = ClusterState::key_slot("foo");
        assert!(slot1 < CLUSTER_SLOTS);

        // hash tag
        let slot2 = ClusterState::key_slot("{user}.name");
        let slot3 = ClusterState::key_slot("{user}.age");
        assert_eq!(slot2, slot3);

        // 空 hash tag 使用整个 key
        let slot4 = ClusterState::key_slot("{}.foo");
        let slot5 = ClusterState::key_slot("{}.foo");
        assert_eq!(slot4, slot5);
    }

    #[test]
    fn test_cluster_state() {
        let state = ClusterState::new("127.0.0.1".to_string(), 6379);
        let my_id = state.myself_id();
        assert_eq!(my_id.len(), 40);

        // 分配 slot
        assert!(state.assign_slot(0, &my_id));
        assert!(state.assign_slot(100, &my_id));
        assert_eq!(state.get_slot_node(0), Some(my_id.clone()));
        assert_eq!(state.assigned_slots_count(), 2);

        // 取消分配
        assert!(state.unassign_slot(0));
        assert_eq!(state.get_slot_node(0), None);
        assert_eq!(state.assigned_slots_count(), 1);
    }

    #[test]
    fn test_cluster_node() {
        let mut node = ClusterNode::new(
            "abc".repeat(14)[..40].to_string(),
            "127.0.0.1".to_string(),
            6379,
        );
        assert_eq!(node.bus_port, 16379);
        assert_eq!(node.slot_count(), 0);

        node.add_slot(0);
        node.add_slot(1);
        assert_eq!(node.slot_count(), 2);
        assert!(node.has_slot(0));
        assert!(!node.has_slot(2));

        node.del_slot(0);
        assert_eq!(node.slot_count(), 1);
    }

    #[test]
    fn test_save_and_load_nodes_conf() {
        let path = temp_nodes_conf_path("nodes");
        let _ = std::fs::remove_file(&path);

        // 创建集群状态并分配 slot
        let state = ClusterState::new("127.0.0.1".to_string(), 6379);
        let my_id = state.myself_id();
        state.assign_slot(0, &my_id);
        state.assign_slot(1, &my_id);
        state.assign_slot(100, &my_id);

        // 添加另一个节点
        let other_id = "b".repeat(40);
        let other_node = ClusterNode::new(other_id.clone(), "192.168.1.2".to_string(), 6380);
        state.add_node(other_node);
        state.assign_slot(50, &other_id);

        // 保存到文件
        assert!(state.save_nodes_conf(&path).is_ok());

        // 新实例加载
        let state2 = ClusterState::new("127.0.0.1".to_string(), 6379);
        assert!(state2.load_nodes_conf(&path).is_ok());

        // 验证 myself ID 被恢复
        assert_eq!(state2.myself_id(), my_id);

        // 验证 slot 分配
        assert_eq!(state2.get_slot_node(0), Some(my_id.clone()));
        assert_eq!(state2.get_slot_node(1), Some(my_id.clone()));
        assert_eq!(state2.get_slot_node(100), Some(my_id.clone()));
        assert_eq!(state2.get_slot_node(50), Some(other_id.clone()));
        assert_eq!(state2.get_slot_node(99), None);

        // 验证其他节点信息
        let other = state2.get_node(&other_id).unwrap();
        assert_eq!(other.ip, "192.168.1.2");
        assert_eq!(other.port, 6380);
        assert_eq!(other.bus_port, 16380);

        let myself = state2.myself().unwrap();
        assert!(myself.flags.contains(&NodeFlag::Myself));
        assert!(myself.flags.contains(&NodeFlag::Master));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_parse_node_addr() {
        let (ip, port, bus_port) = parse_node_addr("192.168.1.1:6379@16379");
        assert_eq!(ip, "192.168.1.1");
        assert_eq!(port, 6379);
        assert_eq!(bus_port, 16379);

        let (ip2, port2, bus_port2) = parse_node_addr("127.0.0.1:6380");
        assert_eq!(ip2, "127.0.0.1");
        assert_eq!(port2, 6380);
        assert_eq!(bus_port2, 16380);
    }

    #[test]
    fn test_parse_node_flags() {
        let flags = parse_node_flags("master,myself,fail");
        assert!(flags.contains(&NodeFlag::Master));
        assert!(flags.contains(&NodeFlag::Myself));
        assert!(flags.contains(&NodeFlag::Fail));
        assert!(!flags.contains(&NodeFlag::Slave));
    }

    #[test]
    fn test_parse_slot_range() {
        let mut slots = Vec::new();
        parse_slot_range("5", |s| slots.push(s));
        assert_eq!(slots, vec![5]);

        let mut slots2 = Vec::new();
        parse_slot_range("3-6", |s| slots2.push(s));
        assert_eq!(slots2, vec![3, 4, 5, 6]);
    }
}
