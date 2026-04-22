// Cluster 二进制总线协议模块
// 定义消息格式、编解码逻辑和辅助函数

use super::state::NodeFlag;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 消息类型常量
pub const MSG_TYPE_PING: u8 = 1;
pub const MSG_TYPE_PONG: u8 = 2;
pub const MSG_TYPE_MEET: u8 = 3;
pub const MSG_TYPE_FAIL: u8 = 4;

/// 消息头魔数
pub const MAGIC: &[u8] = b"RBUS";

/// 集群总线消息结构
#[derive(Debug, Clone)]
pub struct ClusterMessage {
    /// 消息类型
    pub msg_type: u8,
    /// 标志位
    pub flags: u8,
    /// 发送方数据端口
    pub sender_port: u16,
    /// 当前 epoch
    pub current_epoch: u64,
    /// 发送方节点 ID（40 字符）
    pub sender_id: String,
    /// Slot 分配信息列表
    pub slots: Vec<SlotInfo>,
    /// 节点拓扑信息列表
    pub nodes: Vec<NodeInfo>,
}

/// Slot 范围信息
#[derive(Debug, Clone)]
pub struct SlotInfo {
    /// Slot 起始编号
    pub slot_start: u16,
    /// Slot 结束编号
    pub slot_end: u16,
    /// 负责该 slot 范围的节点 ID
    pub node_id: String,
}

/// 节点信息
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// 节点 ID
    pub node_id: String,
    /// IP 地址（IPv4，4 字节）
    pub ip: [u8; 4],
    /// 数据端口
    pub port: u16,
    /// 节点标志位掩码
    pub flags: u8,
}

/// 将节点标志列表编码为单字节位掩码
pub fn encode_flags(flags: &[NodeFlag]) -> u8 {
    let mut mask = 0u8;
    for f in flags {
        mask |= match f {
            NodeFlag::Master => 1 << 0,
            NodeFlag::Slave => 1 << 1,
            NodeFlag::Myself => 1 << 2,
            NodeFlag::Fail => 1 << 3,
            NodeFlag::PFail => 1 << 4,
            NodeFlag::Handshake => 1 << 5,
            NodeFlag::NoAddr => 1 << 6,
        };
    }
    mask
}

/// 将单字节位掩码解码为节点标志列表
pub fn decode_flags(mask: u8) -> Vec<NodeFlag> {
    let mut flags = Vec::new();
    if mask & (1 << 0) != 0 {
        flags.push(NodeFlag::Master);
    }
    if mask & (1 << 1) != 0 {
        flags.push(NodeFlag::Slave);
    }
    if mask & (1 << 2) != 0 {
        flags.push(NodeFlag::Myself);
    }
    if mask & (1 << 3) != 0 {
        flags.push(NodeFlag::Fail);
    }
    if mask & (1 << 4) != 0 {
        flags.push(NodeFlag::PFail);
    }
    if mask & (1 << 5) != 0 {
        flags.push(NodeFlag::Handshake);
    }
    if mask & (1 << 6) != 0 {
        flags.push(NodeFlag::NoAddr);
    }
    flags
}

/// 将 IPv4 字符串解析为 4 字节数组
pub fn parse_ip(ip: &str) -> [u8; 4] {
    let mut result = [0u8; 4];
    let parts: Vec<&str> = ip.split('.').collect();
    for (i, part) in parts.iter().take(4).enumerate() {
        if let Ok(n) = part.parse::<u8>() {
            result[i] = n;
        }
    }
    result
}

/// 将 4 字节数组格式化为 IPv4 字符串
pub fn format_ip(ip: &[u8; 4]) -> String {
    format!("{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3])
}

/// 将节点的 slot 布尔数组压缩为 SlotInfo 范围列表
pub fn build_slot_ranges(slots: &[bool], node_id: &str) -> Vec<SlotInfo> {
    let mut ranges = Vec::new();
    let mut i = 0usize;
    while i < slots.len() {
        if slots[i] {
            let start = i;
            while i < slots.len() && slots[i] {
                i += 1;
            }
            ranges.push(SlotInfo {
                slot_start: start as u16,
                slot_end: (i.saturating_sub(1)) as u16,
                node_id: node_id.to_string(),
            });
        } else {
            i += 1;
        }
    }
    ranges
}

/// 将 SlotInfo 范围列表展开为单个 slot 编号列表
pub fn expand_slots(slots: &[SlotInfo]) -> Vec<usize> {
    let mut result = Vec::new();
    for info in slots {
        for s in info.slot_start..=info.slot_end {
            result.push(s as usize);
        }
    }
    result
}

/// 编码消息为字节流
pub fn encode_message(
    msg_type: u8,
    sender_id: &str,
    port: u16,
    epoch: u64,
    slots: Vec<SlotInfo>,
    nodes: Vec<NodeInfo>,
) -> Vec<u8> {
    let mut buf = Vec::new();

    // 消息头（16 字节）
    buf.extend_from_slice(MAGIC); // 4 字节
    buf.push(msg_type); // 1 字节
    buf.push(0u8); // flags, 1 字节
    buf.extend_from_slice(&port.to_be_bytes()); // 2 字节
    buf.extend_from_slice(&epoch.to_be_bytes()); // 8 字节

    // sender_id（40 字节，不足补 0）
    let mut sender_id_bytes = [0u8; 40];
    let raw = sender_id.as_bytes();
    let len = raw.len().min(40);
    sender_id_bytes[..len].copy_from_slice(&raw[..len]);
    buf.extend_from_slice(&sender_id_bytes);

    // slot 信息数量（2 字节）
    buf.extend_from_slice(&(slots.len() as u16).to_be_bytes());
    // 每条 slot 信息（44 字节）
    for slot in &slots {
        buf.extend_from_slice(&slot.slot_start.to_be_bytes());
        buf.extend_from_slice(&slot.slot_end.to_be_bytes());
        let mut nid = [0u8; 40];
        let raw_id = slot.node_id.as_bytes();
        let len = raw_id.len().min(40);
        nid[..len].copy_from_slice(&raw_id[..len]);
        buf.extend_from_slice(&nid);
    }

    // 节点信息数量（2 字节）
    buf.extend_from_slice(&(nodes.len() as u16).to_be_bytes());
    // 每条节点信息（47 字节）
    for node in &nodes {
        let mut nid = [0u8; 40];
        let raw_id = node.node_id.as_bytes();
        let len = raw_id.len().min(40);
        nid[..len].copy_from_slice(&raw_id[..len]);
        buf.extend_from_slice(&nid);
        buf.extend_from_slice(&node.ip);
        buf.extend_from_slice(&node.port.to_be_bytes());
        buf.push(node.flags);
    }

    buf
}

/// 从字节流解码消息
pub fn decode_message(data: &[u8]) -> Result<ClusterMessage, String> {
    if data.len() < 16 {
        return Err("数据长度不足，无法解析消息头".to_string());
    }

    if &data[0..4] != MAGIC {
        return Err("消息头 magic 不匹配".to_string());
    }

    let msg_type = data[4];
    let flags = data[5];
    let sender_port = u16::from_be_bytes([data[6], data[7]]);
    let current_epoch = u64::from_be_bytes(data[8..16].try_into().unwrap());

    let mut offset = 16usize;

    // sender_id（40 字节）
    if data.len() < offset + 40 {
        return Err("数据长度不足，无法解析 sender_id".to_string());
    }
    let sender_id = String::from_utf8_lossy(&data[offset..offset + 40])
        .trim_end_matches('\0')
        .to_string();
    offset += 40;

    // num_slots_info（2 字节）
    if data.len() < offset + 2 {
        return Err("数据长度不足，无法解析 slot 数量".to_string());
    }
    let num_slots = u16::from_be_bytes([data[offset], data[offset + 1]]);
    offset += 2;

    let mut slots = Vec::new();
    for _ in 0..num_slots {
        if data.len() < offset + 44 {
            return Err("数据长度不足，无法解析 slot 信息条目".to_string());
        }
        let slot_start = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let slot_end = u16::from_be_bytes([data[offset + 2], data[offset + 3]]);
        let node_id = String::from_utf8_lossy(&data[offset + 4..offset + 44])
            .trim_end_matches('\0')
            .to_string();
        slots.push(SlotInfo {
            slot_start,
            slot_end,
            node_id,
        });
        offset += 44;
    }

    // num_nodes_info（2 字节）
    if data.len() < offset + 2 {
        return Err("数据长度不足，无法解析节点数量".to_string());
    }
    let num_nodes = u16::from_be_bytes([data[offset], data[offset + 1]]);
    offset += 2;

    let mut nodes = Vec::new();
    for _ in 0..num_nodes {
        if data.len() < offset + 47 {
            return Err("数据长度不足，无法解析节点信息条目".to_string());
        }
        let node_id = String::from_utf8_lossy(&data[offset..offset + 40])
            .trim_end_matches('\0')
            .to_string();
        let ip = [data[offset + 40], data[offset + 41], data[offset + 42], data[offset + 43]];
        let port = u16::from_be_bytes([data[offset + 44], data[offset + 45]]);
        let flags = data[offset + 46];
        nodes.push(NodeInfo {
            node_id,
            ip,
            port,
            flags,
        });
        offset += 47;
    }

    Ok(ClusterMessage {
        msg_type,
        flags,
        sender_port,
        current_epoch,
        sender_id,
        slots,
        nodes,
    })
}

/// 从异步流中读取一条完整的 ClusterMessage
pub async fn read_message<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> Result<ClusterMessage, Box<dyn std::error::Error + Send + Sync>> {
    let mut header = [0u8; 16];
    stream.read_exact(&mut header).await?;

    if &header[0..4] != MAGIC {
        return Err("消息头 magic 不匹配".into());
    }

    let msg_type = header[4];
    let flags = header[5];
    let sender_port = u16::from_be_bytes([header[6], header[7]]);
    let current_epoch = u64::from_be_bytes(header[8..16].try_into().unwrap());

    let mut sender_id_buf = [0u8; 40];
    stream.read_exact(&mut sender_id_buf).await?;
    let sender_id = String::from_utf8_lossy(&sender_id_buf)
        .trim_end_matches('\0')
        .to_string();

    let mut num_slots_buf = [0u8; 2];
    stream.read_exact(&mut num_slots_buf).await?;
    let num_slots = u16::from_be_bytes(num_slots_buf);

    let mut slots = Vec::new();
    for _ in 0..num_slots {
        let mut buf = [0u8; 44];
        stream.read_exact(&mut buf).await?;
        let slot_start = u16::from_be_bytes([buf[0], buf[1]]);
        let slot_end = u16::from_be_bytes([buf[2], buf[3]]);
        let node_id = String::from_utf8_lossy(&buf[4..44])
            .trim_end_matches('\0')
            .to_string();
        slots.push(SlotInfo {
            slot_start,
            slot_end,
            node_id,
        });
    }

    let mut num_nodes_buf = [0u8; 2];
    stream.read_exact(&mut num_nodes_buf).await?;
    let num_nodes = u16::from_be_bytes(num_nodes_buf);

    let mut nodes = Vec::new();
    for _ in 0..num_nodes {
        let mut buf = [0u8; 47];
        stream.read_exact(&mut buf).await?;
        let node_id = String::from_utf8_lossy(&buf[0..40])
            .trim_end_matches('\0')
            .to_string();
        let ip = [buf[40], buf[41], buf[42], buf[43]];
        let port = u16::from_be_bytes([buf[44], buf[45]]);
        let flags = buf[46];
        nodes.push(NodeInfo {
            node_id,
            ip,
            port,
            flags,
        });
    }

    Ok(ClusterMessage {
        msg_type,
        flags,
        sender_port,
        current_epoch,
        sender_id,
        slots,
        nodes,
    })
}

/// 向异步流写入一条完整的 ClusterMessage
pub async fn write_message<W: AsyncWriteExt + Unpin>(
    stream: &mut W,
    msg: &ClusterMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let data = encode_message(
        msg.msg_type,
        &msg.sender_id,
        msg.sender_port,
        msg.current_epoch,
        msg.slots.clone(),
        msg.nodes.clone(),
    );
    stream.write_all(&data).await?;
    Ok(())
}


