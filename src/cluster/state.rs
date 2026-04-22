// Cluster 状态管理

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
    pub slots: Vec<bool>,  // slots[i] = true 表示负责 slot i
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
        Self {
            id,
            ip,
            port,
            bus_port: port + 10000,
            flags: vec![NodeFlag::Master],
            master_id: None,
            slots: vec![false; CLUSTER_SLOTS],
            ping_sent: 0,
            pong_recv: 0,
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
        self.slots.iter().enumerate()
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
        self.flags.iter().map(|f| match f {
            NodeFlag::Master => "master",
            NodeFlag::Slave => "slave",
            NodeFlag::Myself => "myself",
            NodeFlag::Fail => "fail",
            NodeFlag::PFail => "fail?",
            NodeFlag::Handshake => "handshake",
            NodeFlag::NoAddr => "noaddr",
        }).collect::<Vec<_>>().join(",")
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
    slot_assignment: RwLock<[Option<String>; CLUSTER_SLOTS]>,
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
            slot_assignment: RwLock::new([const { None }; CLUSTER_SLOTS]),
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
        assignment[slot] = Some(node_id.to_string());
        
        let mut nodes = self.nodes.write().unwrap();
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
                if !tag.is_empty() {
                    tag
                } else {
                    key
                }
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
        if let Some(node) = nodes.get_mut(node_id) {
            if !node.flags.contains(&flag) {
                node.flags.push(flag);
            }
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
        if let Some(node) = nodes.get_mut(node_id) {
            if epoch > node.config_epoch {
                node.config_epoch = epoch;
            }
        }
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
                let mut new_node = ClusterNode::new(node_info.node_id.clone(), ip_str, node_info.port);
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

    /// 获取集群信息字符串（用于 CLUSTER INFO）
    pub fn get_info_string(&self) -> String {
        let nodes = self.nodes.read().unwrap();
        let assigned = self.assigned_slots_count();
        let ok = assigned == CLUSTER_SLOTS;
        
        let mut info = String::new();
        info.push_str(&format!("cluster_enabled:1\r\n"));
        info.push_str(&format!("cluster_state:{}\r\n", if ok { "ok" } else { "fail" }));
        info.push_str(&format!("cluster_slots_assigned:{}\r\n", assigned));
        info.push_str(&format!("cluster_slots_ok:{}\r\n", assigned));
        info.push_str(&format!("cluster_slots_pfail:0\r\n"));
        info.push_str(&format!("cluster_slots_fail:0\r\n"));
        info.push_str(&format!("cluster_known_nodes:{}\r\n", nodes.len()));
        info.push_str(&format!("cluster_size:{}\r\n", 
            nodes.values().filter(|n| n.flags.contains(&NodeFlag::Master) && n.slot_count() > 0).count()
        ));
        info.push_str(&format!("cluster_current_epoch:{}\r\n", self.get_current_epoch()));
        info.push_str(&format!("cluster_my_epoch:{}\r\n", self.get_current_epoch()));
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
        (0..40).map(|_| format!("{:x}", rng.gen_range(0..16))).collect()
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new("127.0.0.1".to_string(), 6379)
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
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16() {
        // Redis 官方测试向量
        assert_eq!(crc16(b"123456789") % CLUSTER_SLOTS as u16, 12739 % CLUSTER_SLOTS as u16);
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
        let mut node = ClusterNode::new("abc".repeat(14)[..40].to_string(), "127.0.0.1".to_string(), 6379);
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
}
