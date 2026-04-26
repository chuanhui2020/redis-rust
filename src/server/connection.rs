//! 客户端连接处理模块，实现请求解析、命令路由、pipeline 和 Cluster 重定向

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use crate::aof::AofWriter;
use crate::command::{Command, CommandExecutor, CommandParser, extract_cmd_info};
use crate::error::Result;
use crate::keyspace::KeyspaceNotifier;
use crate::protocol::{RespParser, RespValue};
use crate::pubsub::PubSubManager;
use crate::scripting::ScriptEngine;
use crate::slowlog::SlowLog;
use crate::storage::StorageEngine;
use super::{bulk, ClientMessage, ClientInfo, ReplyMode, SubscriptionState};
use super::handler::{ConnectionHandler, write_resp, send_reply};

/// 客户端连接守卫，连接断开时自动从注册表中移除
struct ClientGuard {
    client_id: u64,
    clients: Arc<RwLock<HashMap<u64, ClientInfo>>>,
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        let _ = self.clients.write().unwrap().remove(&self.client_id);
    }
}

/// 格式化 slot 范围列表为字符串
fn format_slot_ranges(slots: &[usize]) -> String {
    if slots.is_empty() { return String::new(); }
    let mut ranges = Vec::new();
    let mut start = slots[0];
    let mut end = slots[0];
    for &s in &slots[1..] {
        if s == end + 1 {
            end = s;
        } else {
            if start == end {
                ranges.push(format!("{}", start));
            } else {
                ranges.push(format!("{}-{}", start, end));
            }
            start = s;
            end = s;
        }
    }
    if start == end {
        ranges.push(format!("{}", start));
    } else {
        ranges.push(format!("{}-{}", start, end));
    }
    ranges.join(" ")
}

/// 判断命令是否为只读命令（用于 READONLY 模式）
fn is_read_command(cmd: &Command) -> bool {
    matches!(cmd,
        Command::Get(_) | Command::MGet(_) | Command::StrLen(_) |
        Command::GetRange(_, _, _) | Command::Type(_) | Command::Exists(_) |
        Command::Ttl(_) | Command::LLen(_) |
        Command::LRange(_, _, _) | Command::LIndex(_, _) |
        Command::SCard(_) | Command::SIsMember(_, _) | Command::SMembers(_) |
        Command::SRandMember(_, _) |
        Command::HGet(_, _) | Command::HMGet(_, _) | Command::HGetAll(_) |
        Command::HLen(_) | Command::HExists(_, _) | Command::HKeys(_) |
        Command::HVals(_) | Command::HScan(_, _, _, _) |
        Command::ZCard(_) | Command::ZScore(_, _) | Command::ZRank(_, _) |
        Command::ZRevRank(_, _) | Command::ZRange(_, _, _, _) |
        Command::ZRangeByScore(_, _, _, _) | Command::ZRevRangeByScore(_, _, _, _, _, _) |
        Command::ZLexCount(_, _, _) | Command::ZRangeByLex(_, _, _) |
        Command::ZRevRangeByLex(_, _, _, _, _) | Command::ZMScore(_, _) |
        Command::XLen(_) | Command::XRange(_, _, _, _) | Command::XRevRange(_, _, _, _) |
        Command::Ping(_) | Command::DbSize | Command::Info(_) |
        Command::ObjectEncoding(_) | Command::ObjectIdleTime(_) |
        Command::ObjectRefCount(_) | Command::ObjectFreq(_) |
        Command::Scan(_, _, _) | Command::SScan(_, _, _, _) |
        Command::ZScan(_, _, _, _) |
        Command::GetBit(_, _) | Command::BitCount(_, _, _, _) |
        Command::BitPos(_, _, _, _, _) | Command::PfCount(_)
    )
}

/// 提取读命令中需要追踪的 key 列表（简化实现）
fn extract_tracking_keys(cmd: &Command) -> Vec<String> {
    use Command::*;
    match cmd {
        Get(k) | StrLen(k) | GetRange(k, _, _) | Type(k) | Ttl(k) |
        LLen(k) | LRange(k, _, _) | LIndex(k, _) |
        SCard(k) | SIsMember(k, _) | SMembers(k) | SRandMember(k, _) |
        HGetAll(k) | HLen(k) | HExists(k, _) | HKeys(k) | HVals(k) | HScan(k, _, _, _) |
        ZCard(k) | ZScore(k, _) | ZRank(k, _) | ZRevRank(k, _) |
        ZRange(k, _, _, _) | ZRangeByScore(k, _, _, _) |
        ZLexCount(k, _, _) | ZRangeByLex(k, _, _) |
        ZRevRangeByLex(k, _, _, _, _) | ZMScore(k, _) |
        XLen(k) | XRange(k, _, _, _) | XRevRange(k, _, _, _) |
        GetBit(k, _) | BitCount(k, _, _, _) | BitPos(k, _, _, _, _) |
        ObjectEncoding(k) | ObjectIdleTime(k) | ObjectRefCount(k) | ObjectFreq(k) |
        HGet(k, _) | HMGet(k, _) | HStrLen(k, _) |
        SScan(k, _, _, _) | ZScan(k, _, _, _) |
        ZRevRangeByScore(k, _, _, _, _, _) => vec![k.clone()],
        MGet(ks) | Exists(ks) | SInter(ks) | SUnion(ks) | SDiff(ks) | PfCount(ks) => ks.clone(),
        Scan(_, _, _) => vec![],
        _ => vec![],
    }
}

/// 格式化集群节点列表为 CLUSTER NODES 输出格式
fn format_cluster_nodes(cluster: &crate::cluster::ClusterState) -> String {
    let nodes = cluster.get_nodes();
    let my_id = cluster.myself_id();
    let mut result = String::new();
    for node in &nodes {
        let flags = if node.id == my_id {
            format!("myself,{}", node.flags_string())
        } else {
            node.flags_string()
        };
        let master_id = node.master_id.as_deref().unwrap_or("-");
        let slots_str = format_slot_ranges(&node.get_slots());
        result.push_str(&format!(
            "{} {}:{}@{} {} {} {} {} {} {}\n",
            node.id, node.ip, node.port, node.bus_port,
            flags, master_id, node.ping_sent, node.pong_recv,
            node.config_epoch, slots_str
        ));
    }
    result
}

/// 构建 CLUSTER LINKS 的 RESP 输出（简化实现）
fn build_cluster_links(cluster: &crate::cluster::ClusterState) -> Vec<RespValue> {
    let mut result = Vec::new();
    let my_id = cluster.myself_id();
    for node in cluster.get_nodes() {
        if node.id == my_id {
            continue;
        }
        let direction = "to";
        let events = if node.flags.contains(&crate::cluster::state::NodeFlag::PFail) || node.flags.contains(&crate::cluster::state::NodeFlag::Fail) {
            "r"
        } else {
            "rw"
        };
        let link_info = vec![
            RespValue::BulkString(Some(Bytes::from("direction"))),
            RespValue::BulkString(Some(Bytes::from(direction))),
            RespValue::BulkString(Some(Bytes::from("node"))),
            RespValue::BulkString(Some(Bytes::from(node.id.clone()))),
            RespValue::BulkString(Some(Bytes::from("create-time"))),
            RespValue::Integer(node.pong_recv as i64),
            RespValue::BulkString(Some(Bytes::from("events"))),
            RespValue::BulkString(Some(Bytes::from(events))),
            RespValue::BulkString(Some(Bytes::from("send-buffer-allocated"))),
            RespValue::Integer(0),
            RespValue::BulkString(Some(Bytes::from("send-buffer-used"))),
            RespValue::Integer(0),
        ];
        result.push(RespValue::Array(link_info));
    }
    result
}

/// 构建 CLUSTER SLOTS 的 RESP 输出
fn build_cluster_slots(cluster: &crate::cluster::ClusterState) -> Vec<RespValue> {
    let mut result = Vec::new();
    let mut start = None;
    let mut current_node_id: Option<String> = None;

    for slot in 0..crate::cluster::state::CLUSTER_SLOTS {
        let node_id = cluster.get_slot_node(slot);
        match (current_node_id.as_ref(), node_id.as_ref()) {
            (Some(cur), Some(new_id)) if cur == new_id => {}
            _ => {
                // 结束当前范围
                if let (Some(s), Some(ref node_id)) = (start, current_node_id) {
                    let end = slot.saturating_sub(1);
                    if let Some(node) = cluster.get_node(node_id) {
                        let mut range_arr = vec![
                            RespValue::Integer(s as i64),
                            RespValue::Integer(end as i64),
                            RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from(node.ip.clone()))),
                                RespValue::Integer(node.port as i64),
                                RespValue::BulkString(Some(Bytes::from(node.id.clone()))),
                            ]),
                        ];
                        // 添加副本节点
                        for replica in cluster.get_nodes() {
                            if replica.master_id.as_ref() == Some(node_id) {
                                range_arr.push(RespValue::Array(vec![
                                    RespValue::BulkString(Some(Bytes::from(replica.ip.clone()))),
                                    RespValue::Integer(replica.port as i64),
                                    RespValue::BulkString(Some(Bytes::from(replica.id.clone()))),
                                ]));
                            }
                        }
                        result.push(RespValue::Array(range_arr));
                    }
                }
                start = node_id.as_ref().map(|_| slot);
                current_node_id = node_id;
            }
        }
    }

    // 处理最后一个范围
    if let (Some(s), Some(ref node_id)) = (start, current_node_id) {
        let end = crate::cluster::state::CLUSTER_SLOTS.saturating_sub(1);
        if let Some(node) = cluster.get_node(node_id) {
            let mut range_arr = vec![
                RespValue::Integer(s as i64),
                RespValue::Integer(end as i64),
                RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from(node.ip.clone()))),
                    RespValue::Integer(node.port as i64),
                    RespValue::BulkString(Some(Bytes::from(node.id.clone()))),
                ]),
            ];
            for replica in cluster.get_nodes() {
                if replica.master_id.as_ref() == Some(node_id) {
                    range_arr.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(replica.ip.clone()))),
                        RespValue::Integer(replica.port as i64),
                        RespValue::BulkString(Some(Bytes::from(replica.id.clone()))),
                    ]));
                }
            }
            result.push(RespValue::Array(range_arr));
        }
    }

    result
}

/// 构建 CLUSTER SHARDS 的 RESP 输出（Redis 7 新格式）
fn build_cluster_shards(cluster: &crate::cluster::ClusterState) -> Vec<RespValue> {
    let mut shards = Vec::new();
    let mut processed_masters = std::collections::HashSet::new();

    for node in cluster.get_nodes() {
        if !node.flags.contains(&crate::cluster::state::NodeFlag::Master) {
            continue;
        }
        if !processed_masters.insert(node.id.clone()) {
            continue;
        }

        let slots = node.get_slots();
        if slots.is_empty() {
            continue;
        }

        // 将 slot 分组为连续范围
        let mut slot_ranges = Vec::new();
        let mut s = slots[0];
        let mut e = slots[0];
        for &slot in &slots[1..] {
            if slot == e + 1 {
                e = slot;
            } else {
                slot_ranges.push(RespValue::Integer(s as i64));
                slot_ranges.push(RespValue::Integer(e as i64));
                s = slot;
                e = slot;
            }
        }
        slot_ranges.push(RespValue::Integer(s as i64));
        slot_ranges.push(RespValue::Integer(e as i64));

        // 构建节点数组
        let mut nodes_arr = Vec::new();

        // 主节点
        let master_info = vec![
            RespValue::BulkString(Some(Bytes::from("id"))),
            RespValue::BulkString(Some(Bytes::from(node.id.clone()))),
            RespValue::BulkString(Some(Bytes::from("endpoint"))),
            RespValue::BulkString(Some(Bytes::from(node.ip.clone()))),
            RespValue::BulkString(Some(Bytes::from("ip"))),
            RespValue::BulkString(Some(Bytes::from(node.ip.clone()))),
            RespValue::BulkString(Some(Bytes::from("port"))),
            RespValue::Integer(node.port as i64),
            RespValue::BulkString(Some(Bytes::from("role"))),
            RespValue::BulkString(Some(Bytes::from("master"))),
        ];
        nodes_arr.push(RespValue::Array(master_info));

        // 副本节点
        for replica in cluster.get_nodes() {
            if replica.master_id.as_ref() == Some(&node.id) {
                let replica_info = vec![
                    RespValue::BulkString(Some(Bytes::from("id"))),
                    RespValue::BulkString(Some(Bytes::from(replica.id.clone()))),
                    RespValue::BulkString(Some(Bytes::from("endpoint"))),
                    RespValue::BulkString(Some(Bytes::from(replica.ip.clone()))),
                    RespValue::BulkString(Some(Bytes::from("ip"))),
                    RespValue::BulkString(Some(Bytes::from(replica.ip.clone()))),
                    RespValue::BulkString(Some(Bytes::from("port"))),
                    RespValue::Integer(replica.port as i64),
                    RespValue::BulkString(Some(Bytes::from("role"))),
                    RespValue::BulkString(Some(Bytes::from("slave"))),
                ];
                nodes_arr.push(RespValue::Array(replica_info));
            }
        }

        let shard = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from("slots"))),
            RespValue::Array(slot_ranges),
            RespValue::BulkString(Some(Bytes::from("nodes"))),
            RespValue::Array(nodes_arr),
        ]);
        shards.push(shard);
    }

    shards
}

/// 向目标节点发送 RESTORE 命令，返回成功迁移的 key 列表
/// 向目标节点发送 RESTORE 命令，将本地 key 迁移到目标节点
/// 返回成功迁移的 key 列表
async fn migrate_keys_to_target(
    host: &str,
    port: u16,
    keys: &[String],
    _copy: bool,
    replace: bool,
    storage: &StorageEngine,
    timeout_ms: u64,
) -> std::result::Result<Vec<String>, String> {
    let addr = format!("{}:{}", host, port);
    let timeout = Duration::from_millis(timeout_ms.max(1000));
    let mut target_stream = tokio::time::timeout(timeout, TcpStream::connect(&addr))
        .await
        .map_err(|_| "IOERR error or timeout connecting to the client".to_string())?
        .map_err(|e| format!("IOERR {}", e))?;

    let parser = RespParser::new();
    let mut buf = BytesMut::with_capacity(65536);
    let mut migrated = Vec::new();

    for key in keys {
        let data = match storage.dump(key) {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) => return Err(format!("ERR {}", e)),
        };
        let ttl = storage.pttl(key).unwrap_or(-1);
        let ttl_ms = if ttl < 0 { 0u64 } else { ttl as u64 };

        // 构建 RESTORE 命令
        let mut cmd_parts = vec![
            RespValue::BulkString(Some(Bytes::from("RESTORE"))),
            RespValue::BulkString(Some(Bytes::from(key.clone()))),
            RespValue::BulkString(Some(Bytes::from(ttl_ms.to_string()))),
            RespValue::BulkString(Some(Bytes::from(data))),
        ];
        if replace {
            cmd_parts.push(RespValue::BulkString(Some(Bytes::from("REPLACE"))));
        }
        let restore_cmd = RespValue::Array(cmd_parts);
        let encoded = parser.encode(&restore_cmd);

        target_stream.write_all(&encoded).await
            .map_err(|e| format!("IOERR {}", e))?;

        // 读取响应
        let resp = loop {
            match parser.parse(&mut buf) {
                Ok(Some(r)) => break r,
                Ok(None) => {
                    let n = tokio::time::timeout(timeout, target_stream.read_buf(&mut buf)).await
                        .map_err(|_| "IOERR timeout reading from target".to_string())?
                        .map_err(|e| format!("IOERR {}", e))?;
                    if n == 0 {
                        return Err("IOERR target closed connection".to_string());
                    }
                }
                Err(e) => return Err(format!("ERR protocol error: {}", e)),
            }
        };

        match resp {
            RespValue::SimpleString(ref s) if s == "OK" => {
                migrated.push(key.clone());
            }
            RespValue::Error(ref e) => {
                return Err(format!("ERR Target node returned error: {}", e));
            }
            _ => {
                return Err("ERR Unexpected response from target node".to_string());
            }
        }
    }

    Ok(migrated)
}

/// 将 SentinelInstance 格式化为 Redis 7 兼容的 key-value 数组
fn sentinel_instance_to_resp(inst: &crate::sentinel::SentinelInstance) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from("name"))),
        RespValue::BulkString(Some(Bytes::from(inst.name.clone()))),
        RespValue::BulkString(Some(Bytes::from("ip"))),
        RespValue::BulkString(Some(Bytes::from(inst.ip.clone()))),
        RespValue::BulkString(Some(Bytes::from("port"))),
        RespValue::BulkString(Some(Bytes::from(inst.port.to_string()))),
        RespValue::BulkString(Some(Bytes::from("flags"))),
        RespValue::BulkString(Some(Bytes::from(if inst.sdown { "s_down,master" } else { "master" }))),
        RespValue::BulkString(Some(Bytes::from("quorum"))),
        RespValue::BulkString(Some(Bytes::from(inst.quorum.to_string()))),
    ])
}

/// 副本命令转发循环：将主节点的写命令广播给已连接的副本，并处理副本的 REPLCONF ACK
async fn run_replica_forward_loop(
    stream: TcpStream,
    repl: &crate::replication::ReplicationManager,
    peer_addr: &str,
    replica_listening_port: u16,
) -> Result<()> {
    let (replica_addr, replica_port) = if replica_listening_port > 0 {
        if let Some(colon_pos) = peer_addr.rfind(':') {
            let addr = peer_addr[..colon_pos].to_string();
            (addr, replica_listening_port)
        } else {
            (peer_addr.to_string(), replica_listening_port)
        }
    } else if let Some(colon_pos) = peer_addr.rfind(':') {
        let addr = peer_addr[..colon_pos].to_string();
        let port = peer_addr[colon_pos + 1..].parse::<u16>().unwrap_or(0);
        (addr, port)
    } else {
        (peer_addr.to_string(), 0)
    };

    repl.add_replica(replica_addr.clone(), replica_port);
    log::info!("副本已注册: {}:{}, 进入命令转发循环", replica_addr, replica_port);

    let mut rx = repl.subscribe();
    let (mut read_half, mut write_half) = stream.into_split();
    let mut read_buf = bytes::BytesMut::with_capacity(256);
    let resp_parser = crate::protocol::RespParser::new();
    let cmd_parser = crate::command::CommandParser::new();

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(data) => {
                        if let Err(e) = write_half.write_all(&data).await {
                            log::error!("写入副本失败: {}", e);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("副本落后 {} 条命令", n);
                    }
                }
            }
            result = read_half.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) => {
                        log::info!("副本连接已关闭: {}:{}", replica_addr, replica_port);
                        break;
                    }
                    Ok(n) => {
                        log::debug!("从副本读取 {} 字节", n);
                        while let Ok(Some(resp)) = resp_parser.parse(&mut read_buf) {
                            if let Ok(cmd) = cmd_parser.parse(resp)
                                && let Command::ReplConf { ref args } = cmd
                                    && args.len() >= 2 && args[0].to_uppercase() == "ACK"
                                        && let Ok(offset) = args[1].parse::<i64>() {
                                            repl.update_replica_offset(&replica_addr, replica_port, offset);
                                            log::debug!("收到副本 REPLCONF ACK, offset: {}", offset);
                                        }
                        }
                    }
                    Err(e) => {
                        log::error!("读取副本数据失败: {}", e);
                        break;
                    }
                }
            }
        }
    }

    repl.remove_replica(&replica_addr, replica_port);
    log::info!("副本已断开: {}:{}", replica_addr, replica_port);
    Ok(())
}

/// 处理单个客户端连接
pub(crate) async fn handle_connection(
    stream: TcpStream,
    peer_addr: String,
    mut storage: StorageEngine,
    aof: Option<Arc<Mutex<AofWriter>>>,
    pubsub: PubSubManager,
    password: Option<String>,
    clients: Arc<RwLock<HashMap<u64, ClientInfo>>>,
    next_client_id: Arc<AtomicU64>,
    script_engine: ScriptEngine,
    rdb_path: Option<String>,
    slowlog: SlowLog,
    acl: Option<crate::acl::AclManager>,
    replication: Option<Arc<crate::replication::ReplicationManager>>,
    sentinel: Option<Arc<crate::sentinel::SentinelManager>>,
    cluster: Option<Arc<crate::cluster::ClusterState>>,
    client_pause: Arc<RwLock<Option<(Instant, String)>>>,
    client_kill_flags: Arc<Mutex<HashSet<u64>>>,
    monitor_tx: tokio::sync::broadcast::Sender<String>,
    latency: crate::latency::LatencyTracker,
    keyspace_notifier: Arc<KeyspaceNotifier>,
) -> Result<()> {
    // 分配客户端 ID
    let client_id = next_client_id.fetch_add(1, Ordering::SeqCst);

    // 注册客户端
    {
        let mut clients_guard = clients.write().unwrap();
        clients_guard.insert(client_id, ClientInfo {
            id: client_id,
            addr: peer_addr.clone(),
            name: None,
            db: 0,
            flags: HashSet::new(),
            blocked: false,
            blocked_reason: None,
        });
    }
    let _client_guard = ClientGuard { client_id, clients: clients.clone() };


    // 连接状态
    let mut authenticated = password.is_none() && acl.is_none();
    let mut client_name: Option<String> = None;
    let mut _current_db_index: usize = 0;
    let mut current_user = "default".to_string();
    let mut reply_mode = ReplyMode::On;
    let mut client_flags: HashSet<String> = HashSet::new();
    let blocked = false;
    let _blocked_reason: Option<String> = None;
    let mut replica_listening_port = 0u16;

    // 客户端追踪状态（简化实现）
    let mut tracking_enabled = false;
    let mut tracking_redirect: Option<u64> = None;
    let mut tracking_bcast = false;
    let mut tracking_prefixes: Vec<String> = Vec::new();
    let mut tracking_optin = false;
    let mut tracking_optout = false;
    let mut tracking_noloop = false;
    let mut tracked_keys: HashSet<String> = HashSet::new();
    let mut caching_next: Option<bool> = None;

    let mut executor = match aof.clone() {
        Some(aof_writer) => {
            CommandExecutor::new_with_aof(storage.clone(), aof_writer)
        }
        None => CommandExecutor::new(storage.clone()),
    };
    executor.set_script_engine(script_engine);
    executor.set_slowlog(slowlog.clone());
    executor.set_latency(latency);
    if let Some(ref acl_mgr) = acl {
        executor.set_acl(acl_mgr.clone());
    }
    if let Some(ref repl_mgr) = replication {
        executor.set_replication(repl_mgr.clone());
    }

    // 设置全局 Keyspace 通知器
    executor.set_keyspace_notifier(keyspace_notifier.clone());
    storage.set_keyspace_notifier(keyspace_notifier);
    let handler = ConnectionHandler {
        parser: RespParser::new(),
        cmd_parser: CommandParser::new(),
        executor,
    };

    // 使用 BytesMut 作为读取缓冲区
    let mut buf = BytesMut::with_capacity(65536);
    let mut stream = BufWriter::with_capacity(65536, stream);

    // 订阅状态
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<ClientMessage>();
    let mut sub_state = SubscriptionState::new(msg_tx.clone());
    let mut is_subscribed = false;

    // 事务状态
    let mut in_transaction = false;
    let mut readonly_mode = false;
    let mut tx_queue: Vec<Command> = Vec::new();
    let mut watched: HashMap<String, u64> = HashMap::new();
    // 保留一份 storage 用于事务中的 WATCH/EXEC 检查
    let tx_storage = storage.clone();

    loop {
        // 内层循环：处理缓冲区中所有可用命令（pipeline 支持）
        loop {
        // 尝试从缓冲区解析一个完整的 RESP 消息
        let maybe_resp = match handler.parser.parse(&mut buf) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("RESP 协议解析错误: {}", e);
                let err_resp = RespValue::Error(format!(
                    "ERR protocol error: {}",
                    e
                ));
                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                    log::error!("写入错误响应失败: {}", e);
                    return Ok(());
                }
                continue;
            }
        };

        match maybe_resp {
            Some(resp_value) => {
                // 解析 RESP 为 Command
                match handler.cmd_parser.parse(resp_value) {
                    Ok(cmd) => {
                        log::debug!("收到命令: {:?}", cmd);

                        // 认证检查：未认证且设置了密码时，只允许特定命令
                        let is_auth_exempt = matches!(
                            cmd,
                            Command::Auth(_, _) | Command::Quit | Command::Ping(_) | Command::CommandInfo | Command::CommandCount | Command::CommandList(_) | Command::CommandDocs(_) | Command::CommandGetKeys(_) | Command::Hello(_, _, _)
                        );
                        if !authenticated && password.is_some() && !is_auth_exempt {
                            let resp = RespValue::Error(
                                "NOAUTH Authentication required.".to_string()
                            );
                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                log::error!("写入响应失败: {}", e);
                                return Ok(());
                            }
                            continue;
                        }

                        // ---------- 事务模式处理 ----------
                        if in_transaction {
                            if !super::transaction::handle_in_transaction(
                                cmd,
                                &mut in_transaction,
                                &mut tx_queue,
                                &mut watched,
                                &tx_storage,
                                &mut stream,
                                &handler,
                                &mut reply_mode,
                                &pubsub,
                            ).await? {
                                return Ok(());
                            }
                            continue;
                        }

                        // ---------- 非事务模式命令处理 ----------

                        // ACL 权限检查
                        if let Some(ref acl_mgr) = acl {
                            let (cmd_name, keys) = extract_cmd_info(&cmd);
                            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                            match acl_mgr.check_command(&current_user, &cmd_name, &key_refs) {
                                Ok(true) => {}
                                Ok(false) => {
                                    let _ = acl_mgr.log_deny(&current_user, "command", "toplevel", &cmd_name);
                                    let resp = RespValue::Error(
                                        "NOPERM this user has no permissions to run the '".to_string()
                                            + &cmd_name + "' command"
                                    );
                                    if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                        log::error!("写入响应失败: {}", e);
                                        return Ok(());
                                    }
                                    continue;
                                }
                                Err(e) => {
                                    let resp = RespValue::Error(format!("ERR {}", e));
                                    if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                        log::error!("写入响应失败: {}", e);
                                        return Ok(());
                                    }
                                    continue;
                                }
                            }
                        }

                        // 检查是否被 CLIENT KILL 标记
                        let killed = {
                            let kf = client_kill_flags.lock();
                            kf.map(|g| g.contains(&client_id)).unwrap_or(false)
                        };
                        if killed {
                            let resp = RespValue::Error("ERR Connection closed by CLIENT KILL".to_string());
                            let _ = write_resp(&mut stream, &handler, &resp).await;
                            return Ok(());
                        }

                        // 客户端暂停检查
                        let should_sleep = {
                            let cp = client_pause.read();
                            if let Ok(guard) = cp {
                                if let Some((end, mode)) = guard.as_ref() {
                                    let now = Instant::now();
                                    if now < *end {
                                        let is_write = cmd.is_write_command();
                                        if mode == "ALL" || (mode == "WRITE" && is_write) {
                                            Some(end.duration_since(now))
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };
                        if let Some(remaining) = should_sleep {
                            tokio::time::sleep(tokio::time::Duration::from_millis(remaining.as_millis() as u64)).await;
                        }

                        // Sentinel 模式命令过滤
                        if sentinel.is_some() && !crate::sentinel::is_sentinel_allowed_command(&cmd) {
                            let resp = RespValue::Error(
                                "ERR unknown command, this is a sentinel node".to_string()
                            );
                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                log::error!("写入响应失败: {}", e);
                                return Ok(());
                            }
                            continue;
                        }

                        // SENTINEL 命令在连接层处理
                        if matches!(cmd, Command::SentinelMasters | Command::SentinelMaster(_) | Command::SentinelReplicas(_) | Command::SentinelSentinels(_) | Command::SentinelGetMasterAddrByName(_) | Command::SentinelMonitor { .. } | Command::SentinelRemove(_) | Command::SentinelSet { .. } | Command::SentinelFailover(_) | Command::SentinelReset(_) | Command::SentinelCkquorum(_) | Command::SentinelMyId | Command::SentinelIsMasterDownByAddr { .. }) {
                            let resp = if let Some(s) = sentinel.as_ref() {
                                match cmd {
                                    Command::SentinelMasters => {
                                        let masters = s.get_masters();
                                        let arr: Vec<RespValue> = masters.iter()
                                            .map(sentinel_instance_to_resp)
                                            .collect();
                                        RespValue::Array(arr)
                                    }
                                    Command::SentinelMaster(name) => {
                                        match s.get_master(&name) {
                                            Some(m) => sentinel_instance_to_resp(&m),
                                            None => RespValue::Error("ERR No such master with that name".to_string()),
                                        }
                                    }
                                    Command::SentinelReplicas(name) => {
                                        match s.get_master(&name) {
                                            Some(m) => {
                                                let arr: Vec<RespValue> = m.replicas.iter()
                                                    .map(|r| {
                                                        RespValue::Array(vec![
                                                            RespValue::BulkString(Some(Bytes::from("ip"))),
                                                            RespValue::BulkString(Some(Bytes::from(r.ip.clone()))),
                                                            RespValue::BulkString(Some(Bytes::from("port"))),
                                                            RespValue::BulkString(Some(Bytes::from(r.port.to_string()))),
                                                            RespValue::BulkString(Some(Bytes::from("flags"))),
                                                            RespValue::BulkString(Some(Bytes::from(if r.sdown { "slave,s_down" } else { "slave" }))),
                                                        ])
                                                    })
                                                    .collect();
                                                RespValue::Array(arr)
                                            }
                                            None => RespValue::Error("ERR No such master with that name".to_string()),
                                        }
                                    }
                                    Command::SentinelSentinels(name) => {
                                        match s.get_master(&name) {
                                            Some(m) => {
                                                let arr: Vec<RespValue> = m.sentinels.iter()
                                                    .map(|s| {
                                                        RespValue::Array(vec![
                                                            RespValue::BulkString(Some(Bytes::from("ip"))),
                                                            RespValue::BulkString(Some(Bytes::from(s.ip.clone()))),
                                                            RespValue::BulkString(Some(Bytes::from("port"))),
                                                            RespValue::BulkString(Some(Bytes::from(s.port.to_string()))),
                                                            RespValue::BulkString(Some(Bytes::from("runid"))),
                                                            RespValue::BulkString(Some(Bytes::from(s.runid.clone()))),
                                                        ])
                                                    })
                                                    .collect();
                                                RespValue::Array(arr)
                                            }
                                            None => RespValue::Error("ERR No such master with that name".to_string()),
                                        }
                                    }
                                    Command::SentinelGetMasterAddrByName(name) => {
                                        match s.get_master_addr_by_name(&name) {
                                            Some((ip, port)) => RespValue::Array(vec![
                                                RespValue::BulkString(Some(Bytes::from(ip))),
                                                RespValue::BulkString(Some(Bytes::from(port.to_string()))),
                                            ]),
                                            None => RespValue::Error("ERR No such master with that name".to_string()),
                                        }
                                    }
                                    Command::SentinelMonitor { name, ip, port, quorum } => {
                                        s.monitor(name, ip, port, quorum);
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::SentinelRemove(name) => {
                                        if s.remove(&name) {
                                            RespValue::Integer(1)
                                        } else {
                                            RespValue::Integer(0)
                                        }
                                    }
                                    Command::SentinelSet { name, option, value } => {
                                        match option.to_lowercase().as_str() {
                                            "down-after-milliseconds" => {
                                                if let Ok(ms) = value.parse::<u64>() {
                                                    s.set_down_after_ms(&name, ms);
                                                }
                                            }
                                            "failover-timeout" => {
                                                if let Ok(timeout) = value.parse::<u64>() {
                                                    s.set_failover_timeout(&name, timeout);
                                                }
                                            }
                                            _ => {}
                                        }
                                        let _ = s.save_config();
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::SentinelFailover(name) => {
                                        match crate::sentinel::failover::execute_failover(s, &name).await {
                                            Ok(()) => RespValue::SimpleString("OK".to_string()),
                                            Err(e) => RespValue::Error(format!("ERR {}", e)),
                                        }
                                    }
                                    Command::SentinelReset(pattern) => {
                                        let count = s.reset(&pattern);
                                        RespValue::Integer(count as i64)
                                    }
                                    Command::SentinelCkquorum(name) => {
                                        if let Some(master) = s.get_master(&name) {
                                            let sentinel_count = master.sentinels.len() as u32 + 1; // 包括自己
                                            if sentinel_count >= master.quorum {
                                                RespValue::SimpleString(format!("OK {} usable Sentinels. Quorum and failover authorization is possible.", sentinel_count))
                                            } else {
                                                RespValue::Error(format!("ERR {} usable Sentinels, but need {} for quorum", sentinel_count, master.quorum))
                                            }
                                        } else {
                                            RespValue::Error("ERR No such master with that name".to_string())
                                        }
                                    }
                                    Command::SentinelMyId => {
                                        RespValue::BulkString(Some(Bytes::from(s.runid.clone())))
                                    }
                                    Command::SentinelIsMasterDownByAddr { ip, port, current_epoch, runid } => {
                                        // 查找对应的 master
                                        let masters = s.get_masters();
                                        let matched = masters.iter().find(|m| m.ip == ip && m.port == port);
                                        let down_state = matched.map_or(0i64, |m| if m.sdown { 1 } else { 0 });
                                        let mut leader_runid = "*".to_string();
                                        let mut leader_epoch = 0i64;

                                        if let Some(master) = matched {
                                            // 如果提供了 runid 且不是 *，则尝试投票
                                            if runid != "*" && !runid.is_empty() {
                                                let (voted, voted_runid) = s.vote_for(&master.name, current_epoch, &runid);
                                                if voted {
                                                    leader_runid = voted_runid;
                                                    leader_epoch = current_epoch as i64;
                                                }
                                            }
                                        }

                                        RespValue::Array(vec![
                                            RespValue::Integer(down_state),
                                            RespValue::BulkString(Some(Bytes::from(leader_runid))),
                                            RespValue::Integer(leader_epoch),
                                        ])
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                RespValue::Error("ERR Sentinel mode not enabled".to_string())
                            };
                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                log::error!("写入响应失败: {}", e);
                                return Ok(());
                            }
                            continue;
                        }

                        // CLUSTER 命令在连接层处理
                        if matches!(cmd, Command::ClusterInfo | Command::ClusterNodes | Command::ClusterMyId | Command::ClusterSlots | Command::ClusterShards | Command::ClusterMeet { .. } | Command::ClusterAddSlots(_) | Command::ClusterDelSlots(_) | Command::ClusterSetSlot { .. } | Command::ClusterReplicate(_) | Command::ClusterFailover(_) | Command::ClusterReset(_) | Command::ClusterKeySlot(_) | Command::ClusterCountKeysInSlot(_) | Command::ClusterGetKeysInSlot(_, _) | Command::ClusterLinks | Command::ClusterCountFailureReports(_) | Command::ClusterFlushSlots | Command::ClusterSaveConfig | Command::ClusterSetConfigEpoch(_) | Command::ClusterMyShardId) {
                            let resp = if let Some(c) = cluster.as_ref() {
                                match cmd {
                                    Command::ClusterInfo => {
                                        RespValue::BulkString(Some(Bytes::from(c.get_info_string())))
                                    }
                                    Command::ClusterNodes => {
                                        RespValue::BulkString(Some(Bytes::from(format_cluster_nodes(c))))
                                    }
                                    Command::ClusterMyId => {
                                        RespValue::BulkString(Some(Bytes::from(c.myself_id())))
                                    }
                                    Command::ClusterSlots => {
                                        RespValue::Array(build_cluster_slots(c))
                                    }
                                    Command::ClusterShards => {
                                        RespValue::Array(build_cluster_shards(c))
                                    }
                                    Command::ClusterMeet { ip, port } => {
                                        let meet_ip = ip.clone();
                                        // 检查是否已有相同地址的节点，避免重复
                                        let existing = c.get_nodes().iter()
                                            .find(|n| n.ip == ip && n.port == port)
                                            .map(|n| n.id.clone());
                                        if existing.is_none() {
                                            let node = crate::cluster::ClusterNode::new(
                                                crate::cluster::ClusterState::generate_node_id(),
                                                ip,
                                                port,
                                            );
                                            c.add_node(node);
                                        }
                                        
                                        // 向对端总线端口发送 PING，让对端也发现本节点
                                        if let Some(me) = c.myself() {
                                            let bus_port = port + 10000;
                                            let msg = format!("PING {} {} {}\n", me.id, me.ip, me.port);
                                            tokio::spawn(async move {
                                                let addr = format!("{}:{}", meet_ip, bus_port);
                                                let timeout = Duration::from_millis(500);
                                                if let Ok(Ok(mut stream)) = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
                                                    let _ = tokio::time::timeout(timeout, stream.write_all(msg.as_bytes())).await;
                                                }
                                            });
                                        }
                                        
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterAddSlots(slots) => {
                                        let my_id = c.myself_id();
                                        for slot in slots {
                                            c.assign_slot(slot, &my_id);
                                        }
                                        // ADDSLOTS 后自动保存拓扑
                                        let _ = c.save_nodes_conf("nodes.conf");
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterDelSlots(slots) => {
                                        for slot in slots {
                                            c.unassign_slot(slot);
                                        }
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterSetSlot { slot, state, node_id } => {
                                        match state.as_str() {
                                            "IMPORTING" => {
                                                if let Some(ref source_id) = node_id {
                                                    c.set_slot_importing(slot, source_id.clone());
                                                }
                                            }
                                            "MIGRATING" => {
                                                if let Some(ref target_id) = node_id {
                                                    c.set_slot_migrating(slot, target_id.clone());
                                                }
                                            }
                                            "STABLE" => {
                                                c.set_slot_stable(slot);
                                            }
                                            "NODE" => {
                                                if let Some(ref new_node_id) = node_id {
                                                    c.assign_slot(slot, new_node_id);
                                                    c.set_slot_stable(slot);
                                                }
                                            }
                                            _ => {}
                                        }
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterReplicate(node_id) => {
                                        let my_id = c.myself_id();
                                        if node_id == my_id {
                                            RespValue::Error("ERR Can't replicate myself".to_string())
                                        } else if c.get_node(&node_id).is_none() {
                                            RespValue::Error("ERR I don't know about node ".to_string() + &node_id)
                                        } else {
                                            // 获取 master 的 ip 和 port，用于后续启动复制
                                            let (master_ip, master_port) = if let Some(target) = c.get_node(&node_id) {
                                                (target.ip.clone(), target.port)
                                            } else {
                                                (String::new(), 0)
                                            };
                                            
                                            // 设置本节点为指定 master 的 replica
                                            let mut nodes = c.nodes_write();
                                            if let Some(node) = nodes.get_mut(&my_id) {
                                                node.flags.retain(|f| *f != crate::cluster::state::NodeFlag::Master);
                                                if !node.flags.contains(&crate::cluster::state::NodeFlag::Slave) {
                                                    node.flags.push(crate::cluster::state::NodeFlag::Slave);
                                                }
                                                node.master_id = Some(node_id);
                                                node.slots = vec![false; crate::cluster::state::CLUSTER_SLOTS];
                                            }
                                            drop(nodes);
                                            
                                            // 启动后台复制任务
                                            if let Some(ref repl) = replication {
                                                repl.set_replicaof(master_ip.clone(), master_port);
                                                let repl_clone = repl.clone();
                                                let storage_clone = handler.executor.storage();
                                                tokio::spawn(async move {
                                                    if let Err(e) = repl_clone.start_replication(storage_clone, master_ip, master_port).await {
                                                        log::error!("复制任务失败: {}", e);
                                                    }
                                                });
                                            }
                                            
                                            RespValue::SimpleString("OK".to_string())
                                        }
                                    }
                                    Command::ClusterFailover(_) => {
                                        let _my_id = c.myself_id();
                                        let myself = c.myself();
                                        if let Some(node) = myself {
                                            if node.flags.contains(&crate::cluster::state::NodeFlag::Slave) {
                                                if let Some(ref master_id) = node.master_id {
                                                    if c.promote_replica_to_master(master_id) {
                                                        let epoch = c.get_current_epoch();
                                                        log::warn!("CLUSTER FAILOVER: 本节点已提升为 master，接管 master {} 的 slot，epoch={}", master_id, epoch);
                                                        // 广播拓扑更新
                                                        let cluster_clone = c.clone();
                                                        tokio::spawn(async move {
                                                            crate::cluster::gossip::broadcast_topology_update(cluster_clone).await;
                                                        });
                                                        RespValue::SimpleString("OK".to_string())
                                                    } else {
                                                        RespValue::Error("ERR FAILOVER failed".to_string())
                                                    }
                                                } else {
                                                    RespValue::Error("ERR Node is replica but has no master".to_string())
                                                }
                                            } else {
                                                RespValue::Error("ERR Node is not a replica".to_string())
                                            }
                                        } else {
                                            RespValue::Error("ERR Myself node not found".to_string())
                                        }
                                    }
                                    Command::ClusterReset(_) => {
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterKeySlot(key) => {
                                        let slot = crate::cluster::ClusterState::key_slot(&key);
                                        RespValue::Integer(slot as i64)
                                    }
                                    Command::ClusterCountKeysInSlot(slot) => {
                                        // 遍历当前 db 所有 key，统计匹配 slot 的数量
                                        match storage.keys("*") {
                                            Ok(keys) => {
                                                let count = keys.iter()
                                                    .filter(|k| crate::cluster::ClusterState::key_slot(k) == slot)
                                                    .count();
                                                RespValue::Integer(count as i64)
                                            }
                                            Err(e) => RespValue::Error(format!("ERR {}", e)),
                                        }
                                    }
                                    Command::ClusterGetKeysInSlot(slot, count) => {
                                        // 遍历当前 db 所有 key，返回匹配 slot 的前 count 个
                                        match storage.keys("*") {
                                            Ok(keys) => {
                                                let arr: Vec<RespValue> = keys.into_iter()
                                                    .filter(|k| crate::cluster::ClusterState::key_slot(k) == slot)
                                                    .take(count)
                                                    .map(|k| RespValue::BulkString(Some(Bytes::from(k))))
                                                    .collect();
                                                RespValue::Array(arr)
                                            }
                                            Err(e) => RespValue::Error(format!("ERR {}", e)),
                                        }
                                    }
                                    Command::ClusterLinks => {
                                        RespValue::Array(build_cluster_links(c))
                                    }
                                    Command::ClusterCountFailureReports(node_id) => {
                                        let count = if let Some(node) = c.get_node(&node_id) {
                                            let mut cnt = 0;
                                            if node.flags.contains(&crate::cluster::state::NodeFlag::PFail) { cnt += 1; }
                                            if node.flags.contains(&crate::cluster::state::NodeFlag::Fail) { cnt += 1; }
                                            cnt
                                        } else {
                                            0
                                        };
                                        RespValue::Integer(count as i64)
                                    }
                                    Command::ClusterFlushSlots => {
                                        let my_id = c.myself_id();
                                        let slots = c.slots_for_node(&my_id);
                                        for slot in slots {
                                            c.unassign_slot(slot);
                                        }
                                        let _ = c.save_nodes_conf("nodes.conf");
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterSaveConfig => {
                                        match c.save_nodes_conf("nodes.conf") {
                                            Ok(_) => RespValue::SimpleString("OK".to_string()),
                                            Err(e) => RespValue::Error(format!("ERR {}", e)),
                                        }
                                    }
                                    Command::ClusterSetConfigEpoch(epoch) => {
                                        c.set_current_epoch(epoch);
                                        RespValue::SimpleString("OK".to_string())
                                    }
                                    Command::ClusterMyShardId => {
                                        RespValue::BulkString(Some(Bytes::from(c.myself_id())))
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                RespValue::Error("ERR This instance has cluster support disabled".to_string())
                            };
                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                log::error!("写入响应失败: {}", e);
                                return Ok(());
                            }
                            continue;
                        }

                        // Cluster 模式 MOVED/ASK 重定向检查（豁免复制相关命令）
                        let is_repl_cmd = matches!(cmd,
                            Command::ReplConf { .. } | Command::Psync { .. } |
                            Command::Sync | Command::Ping(_) | Command::Info(_) |
                            Command::ReplicaOf { .. } | Command::ReplicaOfNoOne |
                            Command::ReadOnly | Command::ReadWrite
                        );
                        if !is_repl_cmd {
                        if let Some(ref cluster) = cluster {
                            let (_, keys) = crate::command::extract_cmd_info(&cmd);
                            if let Some(first_key) = keys.first() {
                                let slot = crate::cluster::ClusterState::key_slot(first_key);
                                let my_id = cluster.myself_id();
                                // 如果 slot 正在从本节点迁出，且 key 不存在，返回 ASK
                                if let Some(target_id) = cluster.is_slot_migrating(slot) {
                                    let key_exists = handler.executor.storage().exists(first_key).unwrap_or(false);
                                    if !key_exists
                                        && let Some(target_node) = cluster.get_node(&target_id) {
                                            let resp = RespValue::Error(format!(
                                                "ASK {} {}:{}",
                                                slot, target_node.ip, target_node.port
                                            ));
                                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                log::error!("写入响应失败: {}", e);
                                                return Ok(());
                                            }
                                            continue;
                                        }
                                }
                                if let Some(node_id) = cluster.get_slot_node(slot)
                                    && node_id != my_id {
                                        // READONLY 模式下，replica 允许读取 master 的 slot
                                        let allow_readonly = readonly_mode && is_read_command(&cmd) && {
                                            let myself = cluster.myself();
                                            myself.map(|n| n.master_id.as_deref() == Some(node_id.as_str())).unwrap_or(false)
                                        };
                                        if !allow_readonly
                                            && let Some(node) = cluster.get_node(&node_id) {
                                                let resp = RespValue::Error(format!(
                                                    "MOVED {} {}:{}",
                                                    slot, node.ip, node.port
                                                ));
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                                continue;
                                            }
                                    }
                            }
                        }

                        // Cluster 模式 CROSSSLOT 检查（多 key 命令）
                        if let Some(ref _cluster) = cluster {
                            let (_, keys) = crate::command::extract_cmd_info(&cmd);
                            if keys.len() > 1 {
                                let first_slot = crate::cluster::ClusterState::key_slot(&keys[0]);
                                let cross_slot = keys[1..].iter().any(|k| {
                                    crate::cluster::ClusterState::key_slot(k) != first_slot
                                });
                                if cross_slot {
                                    let resp = RespValue::Error(
                                        "CROSSSLOT Keys in request don't hash to the same slot".to_string()
                                    );
                                    if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                        log::error!("写入响应失败: {}", e);
                                        return Ok(());
                                    }
                                    continue;
                                }
                            }
                        }
                        } // end !is_repl_cmd

                        match cmd {
                            Command::Multi | Command::Watch(_) | Command::Exec | Command::Discard => {
                                if !super::transaction::handle_transaction_init(
                                    cmd,
                                    &mut in_transaction,
                                    &mut tx_queue,
                                    &mut watched,
                                    &tx_storage,
                                    &mut stream,
                                    &handler,
                                ).await? {
                                    return Ok(());
                                }
                            }
                            Command::BgRewriteAof => {
                                let resp = if let Some(ref aof) = aof {
                                    let path = aof.lock().map(|g| g.path().to_string()).ok();
                                    match path {
                                        Some(p) => {
                                            let temp_path = format!("{}.tmp", p);
                                            let use_rdb_preamble = handler.executor.aof_use_rdb_preamble();
                                            match crate::aof::AofRewriter::rewrite(&storage, &temp_path, &p, use_rdb_preamble) {
                                                Ok(_) => {
                                                    let _ = aof.lock().map(|mut g| g.reopen());
                                                    RespValue::SimpleString("Background append only file rewriting started".to_string())
                                                }
                                                Err(e) => {
                                                    RespValue::Error(format!("ERR AOF 重写失败: {}", e))
                                                }
                                            }
                                        }
                                        None => {
                                            RespValue::Error("ERR AOF writer 锁中毒".to_string())
                                        }
                                    }
                                } else {
                                    RespValue::Error("ERR AOF is not enabled".to_string())
                                };
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::Subscribe(_) | Command::Unsubscribe(_) | Command::PSubscribe(_) | Command::PUnsubscribe(_) | Command::Publish(_, _) | Command::PubSubChannels(_) | Command::PubSubNumSub(_) | Command::PubSubNumPat
                            | Command::SSubscribe(_) | Command::SUnsubscribe(_) | Command::SPublish(_, _) | Command::PubSubShardChannels(_) | Command::PubSubShardNumSub(_) => {
                                if !super::pubsub::handle_pubsub_command(
                                    cmd,
                                    &mut is_subscribed,
                                    &mut sub_state,
                                    &msg_tx,
                                    &pubsub,
                                    &mut stream,
                                    &handler,
                                ).await? {
                                    return Ok(());
                                }
                            }
                            // ---------- 无需认证即可执行的命令 ----------
                            Command::Auth(username, pwd) => {
                                let acl = handler.executor.acl();
                                if let Some(ref acl_mgr) = acl {
                                    match acl_mgr.authenticate(&username, &pwd) {
                                        Ok(true) => {
                                            authenticated = true;
                                            current_user = username;
                                            let resp = RespValue::SimpleString("OK".to_string());
                                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                log::error!("写入响应失败: {}", e);
                                                return Ok(());
                                            }
                                        }
                                        Ok(false) => {
                                            let resp = RespValue::Error("ERR invalid password".to_string());
                                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                log::error!("写入响应失败: {}", e);
                                                return Ok(());
                                            }
                                        }
                                        Err(e) => {
                                            let resp = RespValue::Error(format!("ERR {}", e));
                                            if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                log::error!("写入响应失败: {}", e);
                                                return Ok(());
                                            }
                                        }
                                    }
                                } else if let Some(ref expected) = password {
                                    // 兼容旧模式：使用全局密码
                                    if username == "default" && pwd == *expected {
                                        authenticated = true;
                                        let resp = RespValue::SimpleString("OK".to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    } else {
                                        let resp = RespValue::Error("ERR invalid password".to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                } else {
                                    // 未设置密码，直接返回 OK
                                    authenticated = true;
                                    current_user = username;
                                    let resp = RespValue::SimpleString("OK".to_string());
                                    if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                        log::error!("写入响应失败: {}", e);
                                        return Ok(());
                                    }
                                }
                            }
                            Command::Shutdown(_) => {
                                let resp = RespValue::SimpleString("OK".to_string());
                                let _ = write_resp(&mut stream, &handler, &resp).await;
                                return Ok(());
                            }
                            Command::Quit => {
                                let resp = RespValue::SimpleString("OK".to_string());
                                let _ = write_resp(&mut stream, &handler, &resp).await;
                                let _ = stream.flush().await;
                                return Ok(());
                            }
                            Command::CommandInfo | Command::CommandCount | Command::CommandList(_) | Command::CommandDocs(_) | Command::CommandGetKeys(_) => {
                                // redis-benchmark 需要，无需认证
                                match handler.executor.execute(cmd) {
                                    Ok(response) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &response).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            // ---------- 需要认证的数据库/客户端命令 ----------
                            Command::Select(index) => {
                                match handler.executor.select_db(index) {
                                    Ok(()) => {
                                        _current_db_index = index;
                                        // 更新客户端注册表中的 db
                                        if let Ok(mut guard) = clients.write()
                                            && let Some(info) = guard.get_mut(&client_id) {
                                                info.db = index;
                                            }
                                        let resp = RespValue::SimpleString("OK".to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        let resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::ClientSetName(name) => {
                                client_name = Some(name.clone());
                                if let Ok(mut guard) = clients.write()
                                    && let Some(info) = guard.get_mut(&client_id) {
                                        info.name = Some(name);
                                    }
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientGetName => {
                                let resp = match client_name {
                                    Some(ref name) => RespValue::BulkString(Some(Bytes::from(name.clone()))),
                                    None => RespValue::BulkString(None),
                                };
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientList => {
                                let list = if let Ok(guard) = clients.read() {
                                    guard.values()
                                        .map(|info| {
                                            format!("id={} addr={} name={} db={} flags={}",
                                                info.id,
                                                info.addr,
                                                info.name.as_deref().unwrap_or(""),
                                                info.db,
                                                info.flags.iter().cloned().collect::<Vec<_>>().join(","),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                        .join("\n")
                                } else {
                                    String::new()
                                };
                                let resp = RespValue::BulkString(Some(Bytes::from(list)));
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientInfo => {
                                let info_str = format!(
                                    "id={} addr={} name={} db={} flags={} blocked={}",
                                    client_id,
                                    peer_addr,
                                    client_name.as_deref().unwrap_or(""),
                                    _current_db_index,
                                    client_flags.iter().cloned().collect::<Vec<_>>().join(","),
                                    if blocked { "1" } else { "0" }
                                );
                                let resp = RespValue::BulkString(Some(Bytes::from(info_str)));
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientNoEvict(flag) => {
                                if flag {
                                    client_flags.insert("no-evict".to_string());
                                } else {
                                    client_flags.remove("no-evict");
                                }
                                if let Ok(mut guard) = clients.write()
                                    && let Some(info) = guard.get_mut(&client_id) {
                                        if flag {
                                            info.flags.insert("no-evict".to_string());
                                        } else {
                                            info.flags.remove("no-evict");
                                        }
                                    }
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientNoTouch(flag) => {
                                if flag {
                                    client_flags.insert("no-touch".to_string());
                                } else {
                                    client_flags.remove("no-touch");
                                }
                                if let Ok(mut guard) = clients.write()
                                    && let Some(info) = guard.get_mut(&client_id) {
                                        if flag {
                                            info.flags.insert("no-touch".to_string());
                                        } else {
                                            info.flags.remove("no-touch");
                                        }
                                    }
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientReply(mode) => {
                                reply_mode = mode;
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientKill { id, addr, user, skipme } => {
                                let mut count = 0;
                                {
                                    let guard = clients.read().unwrap();
                                    for (cid, info) in guard.iter() {
                                        if *cid == client_id && skipme {
                                            continue;
                                        }
                                        if let Some(kid) = id
                                            && *cid != kid { continue; }
                                        if let Some(ref kaddr) = addr
                                            && info.addr != *kaddr { continue; }
                                        if let Some(ref kuser) = user
                                            && current_user != *kuser { continue; }
                                        if let Ok(mut kf) = client_kill_flags.lock() {
                                            kf.insert(*cid);
                                        }
                                        count += 1;
                                    }
                                }
                                let resp = RespValue::Integer(count as i64);
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientPause(timeout, mode) => {
                                let end = Instant::now() + Duration::from_millis(timeout);
                                if let Ok(mut cp) = client_pause.write() {
                                    *cp = Some((end, mode.clone()));
                                }
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientUnpause => {
                                if let Ok(mut cp) = client_pause.write() {
                                    *cp = None;
                                }
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientUnblock(target_id, reason) => {
                                let mut unblocked = false;
                                if let Ok(mut guard) = clients.write()
                                    && let Some(info) = guard.get_mut(&target_id)
                                        && info.blocked {
                                            info.blocked = false;
                                            info.blocked_reason = Some(reason.clone());
                                            unblocked = true;
                                        }
                                let resp = RespValue::Integer(if unblocked { 1 } else { 0 });
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientId => {
                                let resp = RespValue::Integer(client_id as i64);
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientTracking { on, redirect, bcast, prefixes, optin, optout, noloop } => {
                                tracking_enabled = on;
                                tracking_redirect = redirect;
                                tracking_bcast = bcast;
                                tracking_prefixes = prefixes.clone();
                                tracking_optin = optin;
                                tracking_optout = optout;
                                tracking_noloop = noloop;
                                if !on {
                                    tracked_keys.clear();
                                }
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientCaching(flag) => {
                                caching_next = Some(flag);
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientGetRedir => {
                                let redir = tracking_redirect.unwrap_or(-1i64 as u64);
                                let resp = RespValue::Integer(redir as i64);
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientTrackingInfo => {
                                let mut flags_arr = Vec::new();
                                if tracking_enabled {
                                    flags_arr.push(RespValue::BulkString(Some(Bytes::from("on"))));
                                } else {
                                    flags_arr.push(RespValue::BulkString(Some(Bytes::from("off"))));
                                }
                                if tracking_bcast {
                                    flags_arr.push(RespValue::BulkString(Some(Bytes::from("bcast"))));
                                }
                                if tracking_optin {
                                    flags_arr.push(RespValue::BulkString(Some(Bytes::from("optin"))));
                                }
                                if tracking_optout {
                                    flags_arr.push(RespValue::BulkString(Some(Bytes::from("optout"))));
                                }
                                if tracking_noloop {
                                    flags_arr.push(RespValue::BulkString(Some(Bytes::from("noloop"))));
                                }
                                let mut prefixes_arr = Vec::new();
                                for p in &tracking_prefixes {
                                    prefixes_arr.push(RespValue::BulkString(Some(Bytes::from(p.clone()))));
                                }
                                let resp = RespValue::Array(vec![
                                    RespValue::BulkString(Some(Bytes::from("flags"))),
                                    RespValue::Array(flags_arr),
                                    RespValue::BulkString(Some(Bytes::from("redirect"))),
                                    RespValue::Integer(tracking_redirect.map(|r| r as i64).unwrap_or(-1)),
                                    RespValue::BulkString(Some(Bytes::from("prefixes"))),
                                    RespValue::Array(prefixes_arr),
                                ]);
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::Ping(message) if is_subscribed => {
                                // 订阅模式下的 PING
                                let resp = match message {
                                    Some(m) => RespValue::Array(vec![bulk("pong"), bulk(&m)]),
                                    None => RespValue::SimpleString("PONG".to_string()),
                                };
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            _other if is_subscribed => {
                                // 订阅模式下不允许非 pub/sub 命令
                                let resp = RespValue::Error(
                                    "ERR only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT allowed in this context".to_string()
                                );
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::BLPop(keys, timeout) => {
                                // 先尝试非阻塞弹出
                                let cmd = Command::BLPop(keys.clone(), timeout);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::BulkString(None)) => {
                                        // 需要阻塞等待
                                        match handler.executor.storage().blpop(&keys, timeout).await {
                                            Ok(Some((key, value))) => {
                                                // 阻塞成功，执行 LPOP 写 AOF
                                                let _ = handler.executor.execute(Command::LPop(key.clone()));
                                                let resp = RespValue::Array(vec![
                                                    RespValue::BulkString(Some(bytes::Bytes::from(key))),
                                                    RespValue::BulkString(Some(value)),
                                                ]);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Ok(resp) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BRPop(keys, timeout) => {
                                // 先尝试非阻塞弹出
                                let cmd = Command::BRPop(keys.clone(), timeout);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::BulkString(None)) => {
                                        // 需要阻塞等待
                                        match handler.executor.storage().brpop(&keys, timeout).await {
                                            Ok(Some((key, value))) => {
                                                // 阻塞成功，执行 RPOP 写 AOF
                                                let _ = handler.executor.execute(Command::RPop(key.clone()));
                                                let resp = RespValue::Array(vec![
                                                    RespValue::BulkString(Some(bytes::Bytes::from(key))),
                                                    RespValue::BulkString(Some(value)),
                                                ]);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Ok(resp) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::Save => {
                                match rdb_path {
                                    Some(ref path) => {
                                        let repl_info = replication.as_ref().map(|repl| {
                                            (repl.get_master_replid(), repl.get_master_repl_offset())
                                        });
                                        match crate::rdb::save(&storage, path, repl_info) {
                                            Ok(()) => {
                                                let resp = RespValue::SimpleString("OK".to_string());
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    None => {
                                        let resp = RespValue::Error("ERR RDB 路径未配置".to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BgSave => {
                                match rdb_path {
                                    Some(ref path) => {
                                        let storage_clone = storage.clone();
                                        let path_clone = path.clone();
                                        let repl_info = replication.as_ref().map(|repl| {
                                            (repl.get_master_replid(), repl.get_master_repl_offset())
                                        });
                                        tokio::spawn(async move {
                                            if let Err(e) = crate::rdb::save(&storage_clone, &path_clone, repl_info) {
                                                log::error!("BGSAVE 失败: {}", e);
                                            } else {
                                                log::info!("BGSAVE 完成");
                                            }
                                        });
                                        let resp = RespValue::SimpleString("Background saving started".to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    None => {
                                        let resp = RespValue::Error("ERR RDB 路径未配置".to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BLmove(source, dest, left_from, left_to, timeout) => {
                                let cmd = Command::Lmove(source.clone(), dest.clone(), left_from, left_to);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::BulkString(None)) => {
                                        match handler.executor.storage().blmove(&source, &dest, left_from, left_to, timeout).await {
                                            Ok(Some(value)) => {
                                                let _ = handler.executor.execute(Command::Lmove(source.clone(), dest.clone(), left_from, left_to));
                                                let resp = RespValue::BulkString(Some(value));
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Ok(resp) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BLmpop(keys, left, count, timeout) => {
                                let cmd = Command::Lmpop(keys.clone(), left, count);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::BulkString(None)) => {
                                        match handler.executor.storage().blmpop(&keys, left, count, timeout).await {
                                            Ok(Some((key, values))) => {
                                                let _ = handler.executor.execute(Command::Lmpop(keys.clone(), left, count));
                                                let mut arr = Vec::new();
                                                arr.push(RespValue::BulkString(Some(bytes::Bytes::from(key))));
                                                let vals: Vec<RespValue> = values.into_iter()
                                                    .map(|v| RespValue::BulkString(Some(v)))
                                                    .collect();
                                                arr.push(RespValue::Array(vals));
                                                let resp = RespValue::Array(arr);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Ok(resp) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BRpoplpush(source, dest, timeout) => {
                                let cmd = Command::Rpoplpush(source.clone(), dest.clone());
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::BulkString(None)) => {
                                        match handler.executor.storage().brpoplpush(&source, &dest, timeout).await {
                                            Ok(Some(value)) => {
                                                let _ = handler.executor.execute(Command::Rpoplpush(source.clone(), dest.clone()));
                                                let resp = RespValue::BulkString(Some(value));
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Ok(resp) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BZMpop(timeout, keys, min_or_max, count) => {
                                let cmd = Command::ZMpop(keys.clone(), min_or_max, count);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::BulkString(None)) => {
                                        match handler.executor.storage().bzmpop(&keys, min_or_max, count, timeout).await {
                                            Ok(Some((key, pairs))) => {
                                                let _ = handler.executor.execute(Command::ZMpop(keys.clone(), min_or_max, count));
                                                let mut pair_values = Vec::new();
                                                for (member, score) in pairs {
                                                    pair_values.push(RespValue::BulkString(Some(bytes::Bytes::from(member))));
                                                    pair_values.push(RespValue::BulkString(Some(bytes::Bytes::from(format!("{}", score)))));
                                                }
                                                let resp = RespValue::Array(vec![
                                                    RespValue::BulkString(Some(bytes::Bytes::from(key))),
                                                    RespValue::Array(pair_values),
                                                ]);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                    Ok(resp) => {
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::BZPopMin(keys, timeout) => {
                                let cmd = Command::ZPopMin(keys[0].clone(), 1);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::Array(ref arr)) if !arr.is_empty() => {
                                        let key = &keys[0];
                                        let resp = RespValue::Array(vec![
                                            RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                                            arr[0].clone(),
                                            arr[1].clone(),
                                        ]);
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    _ => {
                                        match handler.executor.storage().bzpopmin(&keys, timeout).await {
                                            Ok(Some((key, member, score))) => {
                                                let _ = handler.executor.execute(Command::ZPopMin(key.clone(), 1));
                                                let resp = RespValue::Array(vec![
                                                    RespValue::BulkString(Some(bytes::Bytes::from(key))),
                                                    RespValue::BulkString(Some(bytes::Bytes::from(member))),
                                                    RespValue::BulkString(Some(bytes::Bytes::from(format!("{}", score)))),
                                                ]);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Command::BZPopMax(keys, timeout) => {
                                let cmd = Command::ZPopMax(keys[0].clone(), 1);
                                match handler.executor.execute(cmd) {
                                    Ok(RespValue::Array(ref arr)) if !arr.is_empty() => {
                                        let key = &keys[0];
                                        let resp = RespValue::Array(vec![
                                            RespValue::BulkString(Some(bytes::Bytes::from(key.clone()))),
                                            arr[0].clone(),
                                            arr[1].clone(),
                                        ]);
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    _ => {
                                        match handler.executor.storage().bzpopmax(&keys, timeout).await {
                                            Ok(Some((key, member, score))) => {
                                                let _ = handler.executor.execute(Command::ZPopMax(key.clone(), 1));
                                                let resp = RespValue::Array(vec![
                                                    RespValue::BulkString(Some(bytes::Bytes::from(key))),
                                                    RespValue::BulkString(Some(bytes::Bytes::from(member))),
                                                    RespValue::BulkString(Some(bytes::Bytes::from(format!("{}", score)))),
                                                ]);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Ok(None) => {
                                                let resp = RespValue::BulkString(None);
                                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                                    log::error!("写入响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                            Err(e) => {
                                                let err_resp = RespValue::Error(format!("ERR {}", e));
                                                if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                                                    log::error!("写入错误响应失败: {}", e);
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // MIGRATE 命令：通过 TCP 连接目标节点，发送 RESTORE 进行真正的网络数据迁移
                            Command::Migrate { host, port, keys, db: _, timeout, copy, replace } => {
                                match migrate_keys_to_target(&host, port, &keys, copy, replace, &handler.executor.storage(), timeout).await {
                                    Ok(migrated_keys) => {
                                        // 非 COPY 模式下，目标节点返回 OK 后删除本地 key
                                        if !copy {
                                            for key in &migrated_keys {
                                                if let Err(e) = handler.executor.storage().del(key) {
                                                    log::warn!("MIGRATE 后删除本地 key {} 失败: {}", key, e);
                                                }
                                            }
                                        }
                                        let resp = if migrated_keys.is_empty() {
                                            RespValue::Error("ERR no such key".to_string())
                                        } else {
                                            RespValue::SimpleString("OK".to_string())
                                        };
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                    Err(err_msg) => {
                                        let resp = RespValue::Error(err_msg.to_string());
                                        if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Command::Asking => {
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ReadOnly => {
                                readonly_mode = true;
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ReadWrite => {
                                readonly_mode = false;
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::Reset => {
                                authenticated = password.is_none() && acl.is_none();
                                current_user = "default".to_string();
                                client_name = None;
                                _current_db_index = 0;
                                reply_mode = ReplyMode::On;
                                client_flags.clear();
                                tracking_enabled = false;
                                tracking_redirect = None;
                                tracking_bcast = false;
                                tracking_prefixes.clear();
                                tracking_optin = false;
                                tracking_optout = false;
                                tracking_noloop = false;
                                tracked_keys.clear();
                                caching_next = None;
                                in_transaction = false;
                                tx_queue.clear();
                                if !watched.is_empty() {
                                    tx_storage.watch_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                watched.clear();
                                if let Ok(mut guard) = clients.write()
                                    && let Some(info) = guard.get_mut(&client_id) {
                                        info.db = 0;
                                        info.name = None;
                                        info.flags.clear();
                                        info.blocked = false;
                                        info.blocked_reason = None;
                                    }
                                let resp = RespValue::SimpleString("RESET".to_string());
                                if let Err(_e) = write_resp(&mut stream, &handler, &resp).await {
                                    return Ok(());
                                }
                            }
                            Command::Hello(protover, auth, setname) => {
                                if let Some((username, pwd)) = auth {
                                    let auth_ok = if let Some(ref acl_mgr) = acl {
                                        acl_mgr.authenticate(&username, &pwd).unwrap_or(false)
                                    } else if let Some(ref expected) = password {
                                        username == "default" && pwd == *expected
                                    } else {
                                        true
                                    };
                                    if !auth_ok {
                                        let resp = RespValue::Error(
                                            "WRONGPASS invalid username-password pair or user is disabled.".to_string()
                                        );
                                        if let Err(_e) = write_resp(&mut stream, &handler, &resp).await {
                                            return Ok(());
                                        }
                                        continue;
                                    }
                                    authenticated = true;
                                    current_user = username;
                                }
                                if let Some(name) = setname {
                                    client_name = Some(name.clone());
                                    if let Ok(mut guard) = clients.write()
                                        && let Some(info) = guard.get_mut(&client_id) {
                                            info.name = Some(name);
                                        }
                                }
                                let map = vec![
                                    RespValue::BulkString(Some(Bytes::from("server"))),
                                    RespValue::BulkString(Some(Bytes::from("redis-rust"))),
                                    RespValue::BulkString(Some(Bytes::from("version"))),
                                    RespValue::BulkString(Some(Bytes::from("7.0.0"))),
                                    RespValue::BulkString(Some(Bytes::from("proto"))),
                                    RespValue::Integer(protover as i64),
                                    RespValue::BulkString(Some(Bytes::from("id"))),
                                    RespValue::Integer(client_id as i64),
                                    RespValue::BulkString(Some(Bytes::from("mode"))),
                                    RespValue::BulkString(Some(Bytes::from("standalone"))),
                                    RespValue::BulkString(Some(Bytes::from("role"))),
                                    RespValue::BulkString(Some(Bytes::from("master"))),
                                ];
                                let resp = RespValue::Array(map);
                                if let Err(_e) = write_resp(&mut stream, &handler, &resp).await {
                                    return Ok(());
                                }
                            }
                            Command::Monitor => {
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(_e) = write_resp(&mut stream, &handler, &resp).await {
                                    return Ok(());
                                }
                                let _ = stream.flush().await;
                                let mut monitor_rx = monitor_tx.subscribe();
                                loop {
                                    tokio::select! {
                                        result = stream.get_mut().read_buf(&mut buf) => {
                                            match result {
                                                Ok(0) => return Ok(()),
                                                Ok(_) => {}
                                                Err(_) => return Ok(()),
                                            }
                                        }
                                        msg = monitor_rx.recv() => {
                                            match msg {
                                                Ok(cmd_str) => {
                                                    let resp = RespValue::SimpleString(cmd_str);
                                                    if let Err(_e) = write_resp(&mut stream, &handler, &resp).await {
                                                        return Ok(());
                                                    }
                                                    let _ = stream.flush().await;
                                                }
                                                Err(broadcast::error::RecvError::Closed) => return Ok(()),
                                                Err(broadcast::error::RecvError::Lagged(_)) => {}
                                            }
                                        }
                                    }
                                }
                            }
                            Command::Wait { numreplicas, timeout } => {
                                let resp = match &replication {
                                    Some(repl) => {
                                        let target_offset = repl.get_master_repl_offset();
                                        let count = repl.wait_for_replicas(target_offset, numreplicas, timeout).await;
                                        RespValue::Integer(count)
                                    }
                                    None => RespValue::Integer(0),
                                };
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::Failover { host, port, timeout, force } => {
                                let resp = match &replication {
                                    Some(repl) => {
                                        if !matches!(repl.get_role(), crate::replication::ReplicationRole::Master) {
                                            RespValue::Error("ERR FAILOVER requires the server to be a master".to_string())
                                        } else {
                                            let replicas = repl.get_connected_replicas();
                                            if replicas.is_empty() {
                                                RespValue::Error("ERR FAILOVER requires connected replicas".to_string())
                                            } else if let (Some(h), Some(p)) = (&host, &port) {
                                                // 检查目标副本是否存在
                                                let found = replicas.iter().any(|r| r.addr == *h && r.port == *p);
                                                if !found && !force {
                                                    RespValue::Error(format!("ERR FAILOVER target {}:{} is not a connected replica", h, p))
                                                } else {
                                                    log::info!("FAILOVER 开始: 目标 {}:{}, timeout={}ms, force={}", h, p, timeout, force);
                                                    RespValue::SimpleString("OK".to_string())
                                                }
                                            } else {
                                                // 无指定目标，选择 offset 最大的副本
                                                log::info!("FAILOVER 开始: 自动选择副本, timeout={}ms, force={}", timeout, force);
                                                RespValue::SimpleString("OK".to_string())
                                            }
                                        }
                                    }
                                    None => RespValue::Error("ERR FAILOVER requires replication to be enabled".to_string()),
                                };
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::FailoverAbort => {
                                log::info!("FAILOVER ABORT");
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            other => {
                                // 记录副本监听端口（用于 REPLCONF listening-port）
                                if let Command::ReplConf { args } = &other
                                    && args.len() >= 2 && args[0].to_uppercase() == "LISTENING-PORT"
                                        && let Ok(port) = args[1].parse::<u16>() {
                                            replica_listening_port = port;
                                        }

                                // 普通命令，交给执行器，同时广播到 MONITOR
                                let is_replconf_ack = matches!(
                                    &other,
                                    Command::ReplConf { args } if args.len() >= 2 && args[0].to_uppercase() == "ACK"
                                );
                                let (cmd_name, args) = extract_cmd_info(&other);
                                let monitor_str = format!(
                                    "{:.6} [{} {}] \"{}\" {}",
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs_f64(),
                                    peer_addr,
                                    client_id,
                                    cmd_name,
                                    args.iter().map(|a| format!("\"{}\"", a)).collect::<Vec<_>>().join(" ")
                                );
                                let _ = monitor_tx.send(monitor_str);
                                let is_read = is_read_command(&other);
                                let tracking_keys = if is_read {
                                    extract_tracking_keys(&other)
                                } else {
                                    vec![]
                                };
                                match handler.executor.execute(other) {
                                    Ok(response) => {
                                        // 处理 CONTINUE 增量同步响应
                                        if let RespValue::SimpleString(ref s) = response
                                            && s.starts_with("CONTINUE") {
                                                let parts: Vec<&str> = s.split_whitespace().collect();
                                                if parts.len() >= 3 {
                                                    let offset: i64 = parts[2].parse().unwrap_or(0);
                                                    // 向客户端发送标准 CONTINUE 响应（仅 replid）
                                                    let continue_resp = RespValue::SimpleString(format!("CONTINUE {}", parts[1]));
                                                    if let Err(e) = send_reply(&mut stream, &handler, &continue_resp, &mut reply_mode).await {
                                                        log::error!("写入 CONTINUE 响应失败: {}", e);
                                                        return Ok(());
                                                    }
                                                    if let Some(ref repl) = replication {
                                                        if let Some(backlog_data) = repl.get_backlog_from_offset(offset)
                                                            && let Err(e) = stream.write_all(&backlog_data).await {
                                                                log::error!("写入增量数据失败: {}", e);
                                                                return Ok(());
                                                            }
                                                        let _ = stream.flush().await;
                                                        return run_replica_forward_loop(stream.into_inner(), repl, &peer_addr, replica_listening_port).await;
                                                    }
                                                }
                                            }

                                        if is_replconf_ack {
                                            continue;
                                        }

                                        if let Err(e) = send_reply(&mut stream, &handler, &response, &mut reply_mode).await {
                                            log::error!("写入响应失败: {}", e);
                                            return Ok(());
                                        }

                                        // CLIENT TRACKING: 追踪读命令的 key
                                        if is_read {
                                            let should_track = if !tracking_enabled {
                                                false
                                            } else if caching_next == Some(true) {
                                                true
                                            } else if caching_next == Some(false) {
                                                false
                                            } else if tracking_optin {
                                                false
                                            } else {
                                                true
                                            };
                                            if should_track {
                                                for key in tracking_keys {
                                                    tracked_keys.insert(key);
                                                }
                                            }
                                            caching_next = None;
                                        }

                                        // 检测到 FULLRESYNC 响应后发送 RDB 数据
                                        if let RespValue::SimpleString(ref s) = response
                                            && s.starts_with("FULLRESYNC") {
                                                let _ = stream.flush().await;
                                                let mut rdb_buf = Vec::new();
                                                let repl_id = replication.as_ref().map(|repl| repl.get_master_replid());
                                                let repl_offset = replication.as_ref().map(|repl| repl.get_master_repl_offset());
                                                if let Err(e) = crate::rdb::save_to_writer_with_repl(
                                                    &handler.executor.storage(),
                                                    &mut rdb_buf,
                                                    repl_id.as_deref(),
                                                    repl_offset,
                                                ) {
                                                    log::error!("生成 RDB 失败: {}", e);
                                                    return Ok(());
                                                }
                                                let header = format!("${}\r\n", rdb_buf.len());
                                                if let Err(e) = stream.write_all(header.as_bytes()).await {
                                                    log::error!("写入 RDB 头部失败: {}", e);
                                                    return Ok(());
                                                }
                                                if let Err(e) = stream.write_all(&rdb_buf).await {
                                                    log::error!("写入 RDB 数据失败: {}", e);
                                                    return Ok(());
                                                }

                                                if let Some(ref repl) = replication {
                                                    // 发送 RDB 生成后积压的 backlog 数据
                                                    if let Some(backlog_data) = repl.get_backlog_from_offset(repl_offset.unwrap_or(0))
                                                        && let Err(e) = stream.write_all(&backlog_data).await {
                                                            log::error!("写入 backlog 数据失败: {}", e);
                                                            return Ok(());
                                                        }
                                                    let _ = stream.flush().await;
                                                    return run_replica_forward_loop(stream.into_inner(), repl, &peer_addr, replica_listening_port).await;
                                                }
                                            }
                                    }
                                    Err(e) => {
                                        log::error!("命令执行失败: {}", e);
                                        let err_resp = RespValue::Error(format!("ERR {}", e));
                                        if let Err(e) = send_reply(&mut stream, &handler, &err_resp, &mut reply_mode).await {
                                            log::error!("写入错误响应失败: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("命令解析失败: {}", e);
                        let err_resp = RespValue::Error(format!("ERR {}", e));
                        if let Err(e) = write_resp(&mut stream, &handler, &err_resp).await {
                            log::error!("写入错误响应失败: {}", e);
                            return Ok(());
                        }
                    }
                }
            }
            None => {
                break;
            }
        }
        } // end inner pipeline loop

        // pipeline 内循环结束，flush 缓冲区
        if let Err(e) = stream.flush().await {
            log::debug!("flush 失败: {}", e);
            return Ok(());
        }

        // 缓冲区中数据不完整，需要从网络读取更多数据
        if buf.capacity() - buf.len() < 1024 {
            buf.reserve(4096);
        }

        if is_subscribed {
            // 订阅模式下需要同时监听网络和订阅消息
            tokio::select! {
                result = stream.get_mut().read_buf(&mut buf) => {
                    match result {
                        Ok(0) => {
                            log::debug!("客户端发送 EOF，关闭连接");
                            return Ok(());
                        }
                        Ok(n) => {
                            log::debug!("从网络读取 {} 字节", n);
                        }
                        Err(e) => {
                            let kind = e.kind();
                            if kind == std::io::ErrorKind::ConnectionReset
                                || kind == std::io::ErrorKind::BrokenPipe
                                || kind == std::io::ErrorKind::ConnectionAborted
                            {
                                log::debug!("客户端断开连接: {}", e);
                            } else {
                                log::debug!("读取数据失败: {}", e);
                            }
                            return Ok(());
                        }
                    }
                }
                maybe_msg = msg_rx.recv() => {
                    if !super::pubsub::handle_pubsub_message(maybe_msg, &mut stream, &handler).await? {
                        return Ok(());
                    }
                    let _ = stream.flush().await;
                }
            }
        } else {
            let bytes_read = match stream.get_mut().read_buf(&mut buf).await {
                Ok(0) => {
                    log::debug!("客户端发送 EOF，关闭连接");
                    if !watched.is_empty() {
                        tx_storage.watch_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Ok(());
                }
                Ok(n) => n,
                Err(e) => {
                    let kind = e.kind();
                    if kind == std::io::ErrorKind::ConnectionReset
                        || kind == std::io::ErrorKind::BrokenPipe
                        || kind == std::io::ErrorKind::ConnectionAborted
                    {
                        log::debug!("客户端断开连接: {}", e);
                    } else {
                        log::error!("读取数据失败: {}", e);
                    }
                    return Ok(());
                }
            };
            log::debug!("从网络读取 {} 字节", bytes_read);
        }
    }
}
