use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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


/// 处理单个客户端连接
pub(crate) async fn handle_connection(
    mut stream: TcpStream,
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

    // 客户端追踪状态（简化实现）
    let mut tracking_enabled = false;
    let mut tracking_redirect: Option<u64> = None;
    let mut tracking_bcast = false;
    let mut tracking_prefixes: Vec<String> = Vec::new();
    let mut tracking_optin = false;
    let mut tracking_optout = false;
    let mut tracking_noloop = false;

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
    let mut buf = BytesMut::with_capacity(4096);

    // 订阅状态
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<ClientMessage>();
    let mut sub_state = SubscriptionState::new(msg_tx.clone());
    let mut is_subscribed = false;

    // 事务状态
    let mut in_transaction = false;
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
                                        if let Ok(mut guard) = clients.write() {
                                            if let Some(info) = guard.get_mut(&client_id) {
                                                info.db = index;
                                            }
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
                                if let Ok(mut guard) = clients.write() {
                                    if let Some(info) = guard.get_mut(&client_id) {
                                        info.name = Some(name);
                                    }
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
                                if let Ok(mut guard) = clients.write() {
                                    if let Some(info) = guard.get_mut(&client_id) {
                                        if flag {
                                            info.flags.insert("no-evict".to_string());
                                        } else {
                                            info.flags.remove("no-evict");
                                        }
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
                                if let Ok(mut guard) = clients.write() {
                                    if let Some(info) = guard.get_mut(&client_id) {
                                        if flag {
                                            info.flags.insert("no-touch".to_string());
                                        } else {
                                            info.flags.remove("no-touch");
                                        }
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
                                        if let Some(kid) = id {
                                            if *cid != kid { continue; }
                                        }
                                        if let Some(ref kaddr) = addr {
                                            if info.addr != *kaddr { continue; }
                                        }
                                        if let Some(ref kuser) = user {
                                            if current_user != *kuser { continue; }
                                        }
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
                                if let Ok(mut guard) = clients.write() {
                                    if let Some(info) = guard.get_mut(&target_id) {
                                        if info.blocked {
                                            info.blocked = false;
                                            info.blocked_reason = Some(reason.clone());
                                            unblocked = true;
                                        }
                                    }
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
                                let resp = RespValue::SimpleString("OK".to_string());
                                if let Err(e) = write_resp(&mut stream, &handler, &resp).await {
                                    log::error!("写入响应失败: {}", e);
                                    return Ok(());
                                }
                            }
                            Command::ClientCaching(_flag) => {
                                // 简化实现：直接返回 OK
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
                                        if let Err(e) = write_resp(&mut stream, &handler, &RespValue::Array(arr.clone())).await {
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
                                        if let Err(e) = write_resp(&mut stream, &handler, &RespValue::Array(arr.clone())).await {
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
                                in_transaction = false;
                                tx_queue.clear();
                                watched.clear();
                                if let Ok(mut guard) = clients.write() {
                                    if let Some(info) = guard.get_mut(&client_id) {
                                        info.db = 0;
                                        info.name = None;
                                        info.flags.clear();
                                        info.blocked = false;
                                        info.blocked_reason = None;
                                    }
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
                                    if let Ok(mut guard) = clients.write() {
                                        if let Some(info) = guard.get_mut(&client_id) {
                                            info.name = Some(name);
                                        }
                                    }
                                }
                                let mut map = Vec::new();
                                map.push(RespValue::BulkString(Some(Bytes::from("server"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("redis-rust"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("version"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("7.0.0"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("proto"))));
                                map.push(RespValue::Integer(protover as i64));
                                map.push(RespValue::BulkString(Some(Bytes::from("id"))));
                                map.push(RespValue::Integer(client_id as i64));
                                map.push(RespValue::BulkString(Some(Bytes::from("mode"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("standalone"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("role"))));
                                map.push(RespValue::BulkString(Some(Bytes::from("master"))));
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
                                let mut monitor_rx = monitor_tx.subscribe();
                                loop {
                                    tokio::select! {
                                        result = stream.read_buf(&mut buf) => {
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
                                match handler.executor.execute(other) {
                                    Ok(response) => {
                                        // 处理 CONTINUE 增量同步响应
                                        if let RespValue::SimpleString(ref s) = response {
                                            if s.starts_with("CONTINUE") {
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
                                                        if let Some(backlog_data) = repl.get_backlog_from_offset(offset) {
                                                            if let Err(e) = stream.write_all(&backlog_data).await {
                                                                log::error!("写入增量数据失败: {}", e);
                                                                return Ok(());
                                                            }
                                                        }
                                                        // 进入副本命令转发循环
                                                        let mut rx = repl.subscribe();
                                                        let (replica_addr, replica_port) = if let Some(colon_pos) = peer_addr.rfind(':') {
                                                            let addr = peer_addr[..colon_pos].to_string();
                                                            let port = peer_addr[colon_pos + 1..].parse::<u16>().unwrap_or(0);
                                                            (addr, port)
                                                        } else {
                                                            (peer_addr.clone(), 0)
                                                        };
                                                        repl.add_replica(replica_addr.clone(), replica_port);
                                                        log::info!("副本已连接（增量同步）: {}:{}", replica_addr, replica_port);

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
                                                                        Ok(0) => break,
                                                                        Ok(_) => {
                                                                            while let Ok(Some(resp)) = resp_parser.parse(&mut read_buf) {
                                                                                if let Ok(cmd) = cmd_parser.parse(resp) {
                                                                                    if let Command::ReplConf { ref args } = cmd {
                                                                                        if args.len() >= 2 && args[0].to_uppercase() == "ACK" {
                                                                                            if let Ok(offset) = args[1].parse::<i64>() {
                                                                                                repl.update_replica_offset(&replica_addr, replica_port, offset);
                                                                                                log::debug!("收到副本 REPLCONF ACK, offset: {}", offset);
                                                                                            }
                                                                                        }
                                                                                    }
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
                                                        return Ok(());
                                                    }
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
                                        // 检测到 FULLRESYNC 响应后发送 RDB 数据
                                        if let RespValue::SimpleString(ref s) = response {
                                            if s.starts_with("FULLRESYNC") {
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

                                                // RDB 发送完成后，进入副本命令转发循环
                                                if let Some(ref repl) = replication {
                                                    let mut rx = repl.subscribe();

                                                    // 解析对端地址和端口
                                                    let (replica_addr, replica_port) = if let Some(colon_pos) = peer_addr.rfind(':') {
                                                        let addr = peer_addr[..colon_pos].to_string();
                                                        let port = peer_addr[colon_pos + 1..].parse::<u16>().unwrap_or(0);
                                                        (addr, port)
                                                    } else {
                                                        (peer_addr.clone(), 0)
                                                    };

                                                    repl.add_replica(replica_addr.clone(), replica_port);
                                                    log::info!("副本已连接: {}:{}", replica_addr, replica_port);

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
                                                                    Ok(0) => break,
                                                                    Ok(_) => {
                                                                        while let Ok(Some(resp)) = resp_parser.parse(&mut read_buf) {
                                                                            if let Ok(cmd) = cmd_parser.parse(resp) {
                                                                                if let Command::ReplConf { ref args } = cmd {
                                                                                    if args.len() >= 2 && args[0].to_uppercase() == "ACK" {
                                                                                        if let Ok(offset) = args[1].parse::<i64>() {
                                                                                            repl.update_replica_offset(&replica_addr, replica_port, offset);
                                                                                            log::debug!("收到副本 REPLCONF ACK, offset: {}", offset);
                                                                                        }
                                                                                    }
                                                                                }
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
                                                    return Ok(());
                                                }
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

        // 缓冲区中数据不完整，需要从网络读取更多数据
        if buf.capacity() - buf.len() < 1024 {
            buf.reserve(4096);
        }

        if is_subscribed {
            // 订阅模式下需要同时监听网络和订阅消息
            tokio::select! {
                result = stream.read_buf(&mut buf) => {
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
                }
            }
        } else {
            let bytes_read = match stream.read_buf(&mut buf).await {
                Ok(0) => {
                    log::debug!("客户端发送 EOF，关闭连接");
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
