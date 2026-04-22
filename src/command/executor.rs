use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use crate::aof::AofWriter;
use crate::error::{AppError, Result};
use crate::keyspace::KeyspaceNotifier;
use crate::protocol::RespValue;
use crate::scripting::ScriptEngine;
use crate::slowlog::SlowLog;
use crate::storage::StorageEngine;

pub struct CommandExecutor {
    /// 存储引擎引用
    pub(crate) storage: StorageEngine,
    /// AOF 写入器（可选），用于持久化写操作
    pub(crate) aof: Option<Arc<Mutex<AofWriter>>>,
    /// Lua 脚本引擎（可选）
    pub(crate) script_engine: Option<ScriptEngine>,
    /// 慢查询日志（可选）
    pub(crate) slowlog: Option<SlowLog>,
    /// ACL 管理器（可选）
    acl: Option<crate::acl::AclManager>,
    /// 复制管理器（可选）
    replication: Option<Arc<crate::replication::ReplicationManager>>,
    /// 延迟追踪器（可选）
    pub(crate) latency: Option<crate::latency::LatencyTracker>,
    /// Keyspace 通知器（可选）
    pub(crate) keyspace_notifier: Option<Arc<KeyspaceNotifier>>,
    /// 是否使用 AOF RDB preamble（混合持久化）
    pub(crate) aof_use_rdb_preamble: Arc<AtomicBool>,
}

impl CommandExecutor {
    /// 创建不带 AOF 的命令执行器（用于 AOF 重放）
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage,
            aof: None,
            script_engine: None,
            slowlog: None,
            acl: None,
            replication: None,
            latency: None,
            keyspace_notifier: None,
            aof_use_rdb_preamble: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 创建带 AOF 的命令执行器（用于正常服务）
    pub fn new_with_aof(
        storage: StorageEngine,
        aof: Arc<Mutex<AofWriter>>,
    ) -> Self {
        Self {
            storage,
            aof: Some(aof),
            script_engine: None,
            slowlog: None,
            acl: None,
            replication: None,
            latency: None,
            keyspace_notifier: None,
            aof_use_rdb_preamble: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 设置 Lua 脚本引擎
    pub fn set_script_engine(&mut self, engine: ScriptEngine) {
        self.script_engine = Some(engine);
    }

    /// 设置慢查询日志
    pub fn set_slowlog(&mut self, slowlog: SlowLog) {
        self.slowlog = Some(slowlog);
    }

    /// 设置 ACL 管理器
    pub fn set_acl(&mut self, acl: crate::acl::AclManager) {
        self.acl = Some(acl);
    }

    /// 设置复制管理器
    pub fn set_replication(&mut self, replication: Arc<crate::replication::ReplicationManager>) {
        self.replication = Some(replication);
    }

    /// 获取复制管理器
    pub fn replication(&self) -> Option<Arc<crate::replication::ReplicationManager>> {
        self.replication.clone()
    }

    /// 设置延迟追踪器
    pub fn set_latency(&mut self, latency: crate::latency::LatencyTracker) {
        self.latency = Some(latency);
    }

    /// 设置 Keyspace 通知器
    pub fn set_keyspace_notifier(&mut self, notifier: Arc<KeyspaceNotifier>) {
        self.keyspace_notifier = Some(notifier);
    }

    /// 设置是否使用 AOF RDB preamble
    pub fn set_aof_use_rdb_preamble(&self, enabled: bool) {
        self.aof_use_rdb_preamble.store(enabled, Ordering::Relaxed);
    }

    /// 获取是否使用 AOF RDB preamble
    pub fn aof_use_rdb_preamble(&self) -> bool {
        self.aof_use_rdb_preamble.load(Ordering::Relaxed)
    }

    /// 获取存储引擎的克隆引用
    pub fn storage(&self) -> StorageEngine {
        self.storage.clone()
    }

    /// 获取 ACL 管理器
    pub fn acl(&self) -> Option<crate::acl::AclManager> {
        self.acl.clone()
    }

    /// 切换当前数据库
    pub fn select_db(&self, index: usize) -> Result<()> {
        self.storage.select(index)
    }

    /// 构建 DEBUG OBJECT key 的返回信息
    pub(crate) fn build_debug_object_info(&self, key: &str) -> Result<String> {
        let encoding = self.storage.object_encoding(key)?
            .unwrap_or_else(|| "none".to_string());
        let refcount = self.storage.object_refcount(key)?
            .unwrap_or(0);
        let idletime = self.storage.object_idletime(key)?
            .unwrap_or(0);
        let serialized_len = match self.storage.dump(key)? {
            Some(data) => data.len(),
            None => 0,
        };

        Ok(format!(
            "Value at:0x{:p} refcount:{} encoding:{} serializedlength:{} lru_seconds_idle:{}",
            key.as_ptr(), refcount, encoding, serialized_len, idletime
        ))
    }

    /// 将写操作追加到 AOF 文件
    pub(crate) fn append_to_aof(&self, cmd: &Command) {
        if !cmd.is_write_command() {
            return;
        }
        if let Some(ref aof) = self.aof {
            match aof.lock() {
                Ok(mut writer) => {
                    if let Err(e) = writer.append(cmd) {
                        log::error!("AOF 写入失败: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("AOF writer 锁中毒: {}", e);
                }
            }
        }
    }

    /// 将写命令广播到所有已连接的副本
    fn propagate_to_replicas(&self, cmd: &Command) {
        if !cmd.is_write_command() {
            return;
        }
        if let Some(ref repl) = self.replication {
            if matches!(repl.get_role(), crate::replication::ReplicationRole::Master) {
                let resp_bytes = crate::replication::serialize_command_to_resp(cmd);
                let len = resp_bytes.len() as i64;
                // 追加到复制积压缓冲区
                repl.append_to_backlog(&resp_bytes);
                // 广播到已连接的副本
                let _ = repl.get_repl_tx().send(resp_bytes);
                repl.incr_master_repl_offset(len);
            }
        }
    }

    /// 执行命令并返回 RESP 结果（自动记录慢查询）
    pub fn execute(&self, cmd: Command) -> Result<RespValue> {
        let start = std::time::Instant::now();
        let (cmd_name, args) = extract_cmd_info(&cmd);

        // 写操作先记录到 AOF，再执行
        self.append_to_aof(&cmd);

        // 保留命令引用用于 keyspace 通知和复制传播
        let cmd_for_notify = cmd.clone();
        let result = self.do_execute(cmd);

        // 命令执行成功后发送 Keyspace 通知并广播到副本
        if result.is_ok() {
            if let Some(ref notifier) = self.keyspace_notifier {
                let db = self.storage.current_db();
                notifier.notify_command(&cmd_for_notify, db);
            }
            self.propagate_to_replicas(&cmd_for_notify);
        }

        let duration_us = start.elapsed().as_micros() as u64;
        if let Some(ref slowlog) = self.slowlog {
            slowlog.record(&cmd_name, args, duration_us);
        }
        if let Some(ref latency) = self.latency {
            // LATENCY 命令自身不记录延迟，避免 RESET 后立即被重新填充
            if !cmd_name.starts_with("LATENCY ") {
                let _ = latency.record("command", duration_us / 1000);
            }
        }

        result
    }

    /// 内部执行命令（不包含计时）
    fn do_execute(&self, cmd: Command) -> Result<RespValue> {
        match cmd {
            Command::Ping(message) => executor_admin::execute_ping(self, message),
            Command::Get(key) => executor_string::execute_get(self, key),
            Command::Set(key, value, options) => executor_string::execute_set(self, key, value, options),
            Command::SetEx(key, value, ttl_ms) => executor_string::execute_set_ex(self, key, value, ttl_ms),
            Command::Del(keys) => executor_string::execute_del(self, keys),
            Command::Exists(keys) => executor_string::execute_exists(self, keys),
            Command::FlushAll => executor_string::execute_flush_all(self),
            Command::Expire(key, seconds) => {
                let result = if self.storage.expire(&key, seconds)? {
                    1i64
                } else {
                    0i64
                };
                Ok(RespValue::Integer(result))
            }
            Command::Ttl(key) => {
                let ttl_ms = self.storage.ttl(&key)?;
                // Redis TTL 返回秒数，-1 和 -2 保持不变
                let ttl_sec = if ttl_ms >= 0 {
                    ttl_ms / 1000
                } else {
                    ttl_ms
                };
                Ok(RespValue::Integer(ttl_sec))
            }
            // redis-benchmark 兼容：CONFIG GET 返回空数组
            // 对 maxmemory 返回实际值
            Command::ConfigGet(key) => executor_admin::execute_config_get(self, key),
            Command::ConfigSet(key, value) => executor_admin::execute_config_set(self, key, value),
            // redis-benchmark 兼容：COMMAND 返回空数组
            Command::ConfigRewrite => executor_admin::execute_config_rewrite(self),
            Command::ConfigResetStat => executor_admin::execute_config_reset_stat(self),
            Command::MemoryUsage(key, _samples) => executor_admin::execute_memory_usage(self, key, _samples),
            Command::MemoryDoctor => executor_admin::execute_memory_doctor(self),
            Command::LatencyLatest => executor_admin::execute_latency_latest(self),
            Command::LatencyHistory(event) => executor_admin::execute_latency_history(self, event),
            Command::LatencyReset(events) => executor_admin::execute_latency_reset(self, events),
            Command::Reset => executor_admin::execute_reset(self),
            Command::Hello(_protover, _auth, _setname) => executor_admin::execute_hello(self, _protover, _auth, _setname),
            Command::Monitor => executor_admin::execute_monitor(self),
            Command::CommandInfo => executor_admin::execute_command_info(self),
            Command::CommandCount => executor_admin::execute_command_count(self),
            Command::CommandList(filter) => executor_admin::execute_command_list(self, filter),
            Command::CommandDocs(names) => executor_admin::execute_command_docs(self, names),
            Command::CommandGetKeys(args) => executor_admin::execute_command_get_keys(self, args),
            Command::BgRewriteAof => executor_admin::execute_bg_rewrite_aof(self),
            Command::SetBit(key, offset, value) => executor_bitmap::execute_set_bit(self, key, offset, value),
            Command::GetBit(key, offset) => executor_bitmap::execute_get_bit(self, key, offset),
            Command::BitCount(key, start, end, is_byte) => executor_bitmap::execute_bit_count(self, key, start, end, is_byte),
            Command::BitOp(op, destkey, keys) => executor_bitmap::execute_bit_op(self, op, destkey, keys),
            Command::BitPos(key, bit, start, end, is_byte) => executor_bitmap::execute_bit_pos(self, key, bit, start, end, is_byte),
            Command::BitField(key, ops) => executor_bitmap::execute_bit_field(self, key, ops),
            Command::BitFieldRo(key, ops) => executor_bitmap::execute_bit_field_ro(self, key, ops),
            Command::XAdd(key, id, fields, nomkstream, max_len, min_id) => executor_stream::execute_x_add(self, key, id, fields, nomkstream, max_len, min_id),
            Command::XLen(key) => executor_stream::execute_x_len(self, key),
            Command::XRange(key, start, end, count) => executor_stream::execute_x_range(self, key, start, end, count),
            Command::XRevRange(key, end, start, count) => executor_stream::execute_x_rev_range(self, key, end, start, count),
            Command::XTrim(key, strategy, threshold) => executor_stream::execute_x_trim(self, key, strategy, threshold),
            Command::XDel(key, ids) => executor_stream::execute_x_del(self, key, ids),
            Command::XRead(keys, ids, count) => executor_stream::execute_x_read(self, keys, ids, count),
            Command::XSetId(key, id) => executor_stream::execute_x_set_id(self, key, id),
            Command::XGroupCreate(key, group, id, mkstream) => executor_stream::execute_x_group_create(self, key, group, id, mkstream),
            Command::XGroupDestroy(key, group) => executor_stream::execute_x_group_destroy(self, key, group),
            Command::XGroupSetId(key, group, id) => executor_stream::execute_x_group_set_id(self, key, group, id),
            Command::XGroupDelConsumer(key, group, consumer) => executor_stream::execute_x_group_del_consumer(self, key, group, consumer),
            Command::XGroupCreateConsumer(key, group, consumer) => executor_stream::execute_x_group_create_consumer(self, key, group, consumer),
            Command::XReadGroup(group, consumer, keys, ids, count, noack) => executor_stream::execute_x_read_group(self, group, consumer, keys, ids, count, noack),
            Command::XAck(key, group, ids) => executor_stream::execute_x_ack(self, key, group, ids),
            Command::XClaim(key, group, consumer, min_idle, ids, justid) => executor_stream::execute_x_claim(self, key, group, consumer, min_idle, ids, justid),
            Command::XAutoClaim(key, group, consumer, min_idle, start, count, justid) => executor_stream::execute_x_auto_claim(self, key, group, consumer, min_idle, start, count, justid),
            Command::XPending(key, group, start, end, count, consumer) => executor_stream::execute_x_pending(self, key, group, start, end, count, consumer),
            Command::XInfoStream(key, full) => executor_stream::execute_x_info_stream(self, key, full),
            Command::XInfoGroups(key) => executor_stream::execute_x_info_groups(self, key),
            Command::XInfoConsumers(key, group) => executor_stream::execute_x_info_consumers(self, key, group),
            Command::PfAdd(key, elements) => {
                let updated = self.storage.pfadd(&key, &elements)?;
                Ok(RespValue::Integer(updated))
            }
            Command::PfCount(keys) => {
                let count = self.storage.pfcount(&keys)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::PfMerge(destkey, sourcekeys) => {
                self.storage.pfmerge(&destkey, &sourcekeys)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::GeoAdd(key, items) => executor_geo::execute_geo_add(self, key, items),
            Command::GeoDist(key, member1, member2, unit) => executor_geo::execute_geo_dist(self, key, member1, member2, unit),
            Command::GeoHash(key, members) => executor_geo::execute_geo_hash(self, key, members),
            Command::GeoPos(key, members) => executor_geo::execute_geo_pos(self, key, members),
            Command::GeoSearch(key, center_lon, center_lat, by_radius, by_box, order, count, withcoord, withdist, withhash) => executor_geo::execute_geo_search(self, key, center_lon, center_lat, by_radius, by_box, order, count, withcoord, withdist, withhash),
            Command::GeoSearchStore(destination, source, center_lon, center_lat, by_radius, by_box, order, count, storedist) => executor_geo::execute_geo_search_store(self, destination, source, center_lon, center_lat, by_radius, by_box, order, count, storedist),
            Command::Select(index) => executor_admin::execute_select(self, index),
            Command::Auth(_, _) => {
                Err(AppError::Command("AUTH 应在连接层处理".to_string()))
            }
            Command::AclSetUser(username, rules) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let rules_ref: Vec<&str> = rules.iter().map(|s| s.as_str()).collect();
                acl.setuser(&username, &rules_ref)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::AclGetUser(username) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                match acl.getuser(&username)? {
                    Some(user) => {
                        let rules = user.to_rules();
                        let parts: Vec<RespValue> = rules.into_iter()
                            .map(|r| RespValue::BulkString(Some(Bytes::from(r))))
                            .collect();
                        Ok(RespValue::Array(parts))
                    }
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::AclDelUser(names) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                let count = acl.deluser(&names_ref)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::AclList => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let list = acl.list()?;
                let parts: Vec<RespValue> = list.into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::AclCat(category) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let cmds = acl.cat(category.as_deref())?;
                let parts: Vec<RespValue> = cmds.into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(parts))
            }
            Command::AclWhoAmI => {
                Ok(RespValue::SimpleString("default".to_string()))
            }
            Command::AclLog(arg) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                if let Some(arg) = arg {
                    if arg.to_ascii_uppercase() == "RESET" {
                        acl.log_reset()?;
                        Ok(RespValue::SimpleString("OK".to_string()))
                    } else {
                        let count = arg.parse::<usize>().map_err(|_| {
                            AppError::Command("ACL LOG count 必须是整数".to_string())
                        })?;
                        let logs = acl.log(Some(count))?;
                        let parts: Vec<RespValue> = logs.into_iter()
                            .map(|entry| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(Some(Bytes::from("reason"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.reason))),
                                    RespValue::BulkString(Some(Bytes::from("context"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.context))),
                                    RespValue::BulkString(Some(Bytes::from("object"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.object))),
                                    RespValue::BulkString(Some(Bytes::from("username"))),
                                    RespValue::BulkString(Some(Bytes::from(entry.username))),
                                ])
                            })
                            .collect();
                        Ok(RespValue::Array(parts))
                    }
                } else {
                    let logs = acl.log(None)?;
                    let parts: Vec<RespValue> = logs.into_iter()
                        .map(|entry| {
                            RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from("reason"))),
                                RespValue::BulkString(Some(Bytes::from(entry.reason))),
                                RespValue::BulkString(Some(Bytes::from("context"))),
                                RespValue::BulkString(Some(Bytes::from(entry.context))),
                                RespValue::BulkString(Some(Bytes::from("object"))),
                                RespValue::BulkString(Some(Bytes::from(entry.object))),
                                RespValue::BulkString(Some(Bytes::from("username"))),
                                RespValue::BulkString(Some(Bytes::from(entry.username))),
                            ])
                        })
                        .collect();
                    Ok(RespValue::Array(parts))
                }
            }
            Command::AclGenPass(bits) => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                let pass = acl.genpass(bits)?;
                Ok(RespValue::BulkString(Some(Bytes::from(pass))))
            }
            Command::AclSave => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                acl.save("users.acl")?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::AclLoad => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                acl.load("users.acl")?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::AclDryRun { username, command } => {
                let acl = self.acl.as_ref().ok_or_else(|| {
                    AppError::Command("ACL 未启用".to_string())
                })?;
                if command.is_empty() {
                    return Err(AppError::Command("ACL DRYRUN 需要命令参数".to_string()));
                }
                let cmd_name = &command[0];
                let keys: Vec<&str> = command[1..].iter().map(|s| s.as_str()).collect();
                match acl.check_command(&username, cmd_name, &keys) {
                    Ok(true) => Ok(RespValue::SimpleString("OK".to_string())),
                    Ok(false) => Ok(RespValue::Error(format!(
                        "ERR User {} has no permissions to run the '{}' command",
                        username, cmd_name
                    ))),
                    Err(e) => Err(e),
                }
            }
            Command::ClientSetName(_) | Command::ClientGetName | Command::ClientList | Command::ClientId
            | Command::ClientInfo | Command::ClientKill { .. } | Command::ClientPause(_, _)
            | Command::ClientUnpause | Command::ClientNoEvict(_) | Command::ClientNoTouch(_)
            | Command::ClientReply(_) | Command::ClientUnblock(_, _)
            | Command::ClientTracking { .. } | Command::ClientCaching(_) | Command::ClientGetRedir
            | Command::ClientTrackingInfo => {
                Err(AppError::Command("CLIENT 应在连接层处理".to_string()))
            }
            Command::Quit => {
                Err(AppError::Command("QUIT 应在连接层处理".to_string()))
            }
            Command::Eval(script, keys, args) => executor_admin::execute_eval(self, script, keys, args),
            Command::EvalSha(sha1, keys, args) => executor_admin::execute_eval_sha(self, sha1, keys, args),
            Command::ScriptLoad(script) => executor_admin::execute_script_load(self, script),
            Command::ScriptExists(sha1s) => executor_admin::execute_script_exists(self, sha1s),
            Command::ScriptFlush => executor_admin::execute_script_flush(self),
            Command::ScriptDebug(mode) => {
                match mode.to_ascii_uppercase().as_str() {
                    "YES" | "SYNC" | "NO" => Ok(RespValue::SimpleString("OK".to_string())),
                    _ => Err(AppError::Command("SCRIPT DEBUG 模式必须是 YES、SYNC 或 NO".to_string())),
                }
            }
            Command::ScriptHelp => {
                let help = vec![
                    "SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "DEBUG (YES|SYNC|NO)",
                    "    Set the debug mode for subsequent scripts.",
                    "EXISTS <sha1> [<sha1> ...]",
                    "    Return information about the existence of the scripts in the script cache.",
                    "FLUSH [ASYNC|SYNC]",
                    "    Flush the Lua scripts cache. Very dangerous on replicas.",
                    "HELP",
                    "    Print this help.",
                    "LOAD <script>",
                    "    Load a script into the scripts cache without executing it.",
                ];
                let arr: Vec<RespValue> = help.iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(*s))))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::FunctionLoad(code, replace) => executor_admin::execute_function_load(self, code, replace),
            Command::FunctionDelete(lib) => executor_admin::execute_function_delete(self, lib),
            Command::FunctionList(pattern, withcode) => executor_admin::execute_function_list(self, pattern, withcode),
            Command::FunctionDump => executor_admin::execute_function_dump(self),
            Command::FunctionRestore(data, policy) => executor_admin::execute_function_restore(self, data, policy),
            Command::FunctionStats => executor_admin::execute_function_stats(self),
            Command::FunctionFlush(async_mode) => executor_admin::execute_function_flush(self, async_mode),
            Command::FCall(name, keys, args) => executor_admin::execute_f_call(self, name, keys, args),
            Command::FCallRO(name, keys, args) => executor_admin::execute_f_call_r_o(self, name, keys, args),
            Command::EvalRO(script, keys, args) => executor_admin::execute_eval_r_o(self, script, keys, args),
            Command::EvalShaRO(sha1, keys, args) => executor_admin::execute_eval_sha_r_o(self, sha1, keys, args),
            Command::Save => {
                Err(AppError::Command("SAVE 应在连接层处理".to_string()))
            }
            Command::BgSave => {
                Err(AppError::Command("BGSAVE 应在连接层处理".to_string()))
            }
            Command::SlowLogGet(count) => executor_admin::execute_slow_log_get(self, count),
            Command::SlowLogLen => executor_admin::execute_slow_log_len(self),
            Command::SlowLogReset => executor_admin::execute_slow_log_reset(self),
            Command::ObjectEncoding(key) => {
                match self.storage.object_encoding(&key)? {
                    Some(enc) => Ok(RespValue::BulkString(Some(Bytes::from(enc)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectRefCount(key) => {
                match self.storage.object_refcount(&key)? {
                    Some(count) => Ok(RespValue::Integer(count)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectIdleTime(key) => {
                match self.storage.object_idletime(&key)? {
                    Some(secs) => Ok(RespValue::Integer(secs as i64)),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::ObjectHelp => {
                let help = crate::storage::StorageEngine::object_help();
                let arr: Vec<RespValue> = help
                    .into_iter()
                    .map(|s| RespValue::BulkString(Some(Bytes::from(s))))
                    .collect();
                Ok(RespValue::Array(arr))
            }
            Command::DebugSetActiveExpire(enabled) => {
                self.storage.set_active_expire(enabled);
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::DebugSleep(seconds) => {
                std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::DebugObject(key) => executor_admin::execute_debug_object(self, key),
            Command::Echo(msg) => {
                Ok(RespValue::BulkString(Some(Bytes::from(msg))))
            }
            Command::Time => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = now.as_secs() as i64;
                let micros = (now.as_micros() % 1_000_000) as i64;
                Ok(RespValue::Array(vec![
                    RespValue::Integer(secs),
                    RespValue::Integer(micros),
                ]))
            }
            Command::RandomKey => {
                match self.storage.randomkey()? {
                    Some(key) => Ok(RespValue::BulkString(Some(Bytes::from(key)))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Touch(keys) => executor_admin::execute_touch(self, keys),
            Command::ExpireAt(key, timestamp) => executor_admin::execute_expire_at(self, key, timestamp),
            Command::PExpireAt(key, timestamp) => executor_admin::execute_p_expire_at(self, key, timestamp),
            Command::ExpireTime(key) => executor_admin::execute_expire_time(self, key),
            Command::PExpireTime(key) => executor_admin::execute_p_expire_time(self, key),
            Command::RenameNx(key, newkey) => executor_admin::execute_rename_nx(self, key, newkey),
            Command::SwapDb(idx1, idx2) => executor_admin::execute_swap_db(self, idx1, idx2),
            Command::FlushDb => executor_admin::execute_flush_db(self),
            Command::Shutdown(_) => {
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::LastSave => {
                Ok(RespValue::Integer(self.storage.get_last_save_time() as i64))
            }
            Command::SubStr(key, start, end) => {
                // SUBSTR 是 GETRANGE 的别名
                match self.storage.getrange(&key, start, end)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Lcs(key1, key2, len, idx, _minmatchlen, withmatchlen) => executor_string::execute_lcs(self, key1, key2, len, idx, _minmatchlen, withmatchlen),
            Command::Lmove(source, dest, left_from, left_to) => {
                match self.storage.lmove(&source, &dest, left_from, left_to)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Rpoplpush(source, dest) => {
                match self.storage.rpoplpush(&source, &dest)? {
                    Some(value) => Ok(RespValue::BulkString(Some(value))),
                    None => Ok(RespValue::BulkString(None)),
                }
            }
            Command::Lmpop(keys, left, count) => executor_list::execute_lmpop(self, keys, left, count),
            Command::Sort(key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key) => executor_admin::execute_sort(self, key, by_pattern, get_patterns, limit_offset, limit_count, asc, alpha, store_key),
            Command::Unlink(keys) => executor_admin::execute_unlink(self, keys),
            Command::Copy(source, destination, replace) => executor_admin::execute_copy(self, source, destination, replace),
            Command::Dump(key) => executor_admin::execute_dump(self, key),
            Command::Restore(key, ttl_ms, serialized, replace) => executor_admin::execute_restore(self, key, ttl_ms, serialized, replace),
            Command::MGet(keys) => executor_string::execute_m_get(self, keys),
            Command::MSet(pairs) => executor_string::execute_m_set(self, pairs),
            Command::Incr(key) => executor_string::execute_incr(self, key),
            Command::Decr(key) => executor_string::execute_decr(self, key),
            Command::IncrBy(key, delta) => executor_string::execute_incr_by(self, key, delta),
            Command::DecrBy(key, delta) => executor_string::execute_decr_by(self, key, delta),
            Command::Append(key, value) => executor_string::execute_append(self, key, value),
            Command::SetNx(key, value) => executor_string::execute_set_nx(self, key, value),
            Command::SetExCmd(key, value, seconds) => executor_string::execute_set_ex_cmd(self, key, value, seconds),
            Command::PSetEx(key, value, ms) => executor_string::execute_p_set_ex(self, key, value, ms),
            Command::GetSet(key, value) => executor_string::execute_get_set(self, key, value),
            Command::GetDel(key) => executor_string::execute_get_del(self, key),
            Command::GetEx(key, opt) => executor_string::execute_get_ex(self, key, opt),
            Command::MSetNx(pairs) => executor_string::execute_m_set_nx(self, pairs),
            Command::IncrByFloat(key, delta) => executor_string::execute_incr_by_float(self, key, delta),
            Command::SetRange(key, offset, value) => executor_string::execute_set_range(self, key, offset, value),
            Command::GetRange(key, start, end) => executor_string::execute_get_range(self, key, start, end),
            Command::StrLen(key) => executor_string::execute_str_len(self, key),
            Command::LPush(key, values) => executor_list::execute_l_push(self, key, values),
            Command::RPush(key, values) => executor_list::execute_r_push(self, key, values),
            Command::LPop(key) => executor_list::execute_l_pop(self, key),
            Command::RPop(key) => executor_list::execute_r_pop(self, key),
            Command::LLen(key) => executor_list::execute_l_len(self, key),
            Command::LRange(key, start, stop) => executor_list::execute_l_range(self, key, start, stop),
            Command::LIndex(key, index) => executor_list::execute_l_index(self, key, index),
            Command::LSet(key, index, value) => executor_list::execute_l_set(self, key, index, value),
            Command::LInsert(key, pos, pivot, value) => executor_list::execute_l_insert(self, key, pos, pivot, value),
            Command::LRem(key, count, value) => executor_list::execute_l_rem(self, key, count, value),
            Command::LTrim(key, start, stop) => executor_list::execute_l_trim(self, key, start, stop),
            Command::LPos(key, value, rank, count, maxlen) => executor_list::execute_l_pos(self, key, value, rank, count, maxlen),
            Command::BLPop(keys, _timeout) => executor_list::execute_b_l_pop(self, keys, _timeout),
            Command::BRPop(keys, _timeout) => executor_list::execute_b_r_pop(self, keys, _timeout),
            Command::BLmove(source, dest, left_from, left_to, _timeout) => {
                // 非阻塞版本：直接尝试移动（用于事务和 AOF 重放）
                if let Ok(Some(value)) = self.storage.lmove(&source, &dest, left_from, left_to) {
                    self.append_to_aof(&Command::Lmove(source.clone(), dest.clone(), left_from, left_to));
                    Ok(RespValue::BulkString(Some(value)))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::BLmpop(keys, left, count, _timeout) => executor_list::execute_b_lmpop(self, keys, left, count, _timeout),
            Command::BRpoplpush(source, dest, _timeout) => {
                // 非阻塞版本：直接尝试移动（用于事务和 AOF 重放）
                if let Ok(Some(value)) = self.storage.rpoplpush(&source, &dest) {
                    self.append_to_aof(&Command::Rpoplpush(source.clone(), dest.clone()));
                    Ok(RespValue::BulkString(Some(value)))
                } else {
                    Ok(RespValue::BulkString(None))
                }
            }
            Command::HSet(key, pairs) => executor_hash::execute_h_set(self, key, pairs),
            Command::HGet(key, field) => executor_hash::execute_h_get(self, key, field),
            Command::HDel(key, fields) => executor_hash::execute_h_del(self, key, fields),
            Command::HExists(key, field) => executor_hash::execute_h_exists(self, key, field),
            Command::HGetAll(key) => executor_hash::execute_h_get_all(self, key),
            Command::HLen(key) => executor_hash::execute_h_len(self, key),
            Command::HMSet(key, pairs) => executor_hash::execute_h_m_set(self, key, pairs),
            Command::HMGet(key, fields) => executor_hash::execute_h_m_get(self, key, fields),
            Command::HIncrBy(key, field, delta) => executor_hash::execute_h_incr_by(self, key, field, delta),
            Command::HIncrByFloat(key, field, delta) => executor_hash::execute_h_incr_by_float(self, key, field, delta),
            Command::HKeys(key) => executor_hash::execute_h_keys(self, key),
            Command::HVals(key) => executor_hash::execute_h_vals(self, key),
            Command::HSetNx(key, field, value) => executor_hash::execute_h_set_nx(self, key, field, value),
            Command::HRandField(key, count, with_values) => executor_hash::execute_h_rand_field(self, key, count, with_values),
            Command::HScan(key, cursor, pattern, count) => executor_hash::execute_h_scan(self, key, cursor, pattern, count),
            Command::HExpire(key, fields, seconds) => executor_hash::execute_h_expire(self, key, fields, seconds),
            Command::HPExpire(key, fields, ms) => executor_hash::execute_h_p_expire(self, key, fields, ms),
            Command::HExpireAt(key, fields, ts) => executor_hash::execute_h_expire_at(self, key, fields, ts),
            Command::HPExpireAt(key, fields, ts) => executor_hash::execute_h_p_expire_at(self, key, fields, ts),
            Command::HTtl(key, fields) => executor_hash::execute_h_ttl(self, key, fields),
            Command::HPTtl(key, fields) => executor_hash::execute_h_p_ttl(self, key, fields),
            Command::HExpireTime(key, fields) => executor_hash::execute_h_expire_time(self, key, fields),
            Command::HPExpireTime(key, fields) => executor_hash::execute_h_p_expire_time(self, key, fields),
            Command::HPersist(key, fields) => executor_hash::execute_h_persist(self, key, fields),
            Command::SAdd(key, members) => executor_set::execute_s_add(self, key, members),
            Command::SRem(key, members) => executor_set::execute_s_rem(self, key, members),
            Command::SMembers(key) => executor_set::execute_s_members(self, key),
            Command::SIsMember(key, member) => executor_set::execute_s_is_member(self, key, member),
            Command::SCard(key) => executor_set::execute_s_card(self, key),
            Command::SInter(keys) => executor_set::execute_s_inter(self, keys),
            Command::SUnion(keys) => executor_set::execute_s_union(self, keys),
            Command::SDiff(keys) => executor_set::execute_s_diff(self, keys),
            Command::SPop(key, count) => executor_set::execute_s_pop(self, key, count),
            Command::SRandMember(key, count) => executor_set::execute_s_rand_member(self, key, count),
            Command::SMove(source, destination, member) => executor_set::execute_s_move(self, source, destination, member),
            Command::SInterStore(destination, keys) => executor_set::execute_s_inter_store(self, destination, keys),
            Command::SUnionStore(destination, keys) => executor_set::execute_s_union_store(self, destination, keys),
            Command::SDiffStore(destination, keys) => executor_set::execute_s_diff_store(self, destination, keys),
            Command::SScan(key, cursor, pattern, count) => executor_set::execute_s_scan(self, key, cursor, pattern, count),
            Command::ZAdd(key, pairs) => executor_zset::execute_z_add(self, key, pairs),
            Command::ZRem(key, members) => executor_zset::execute_z_rem(self, key, members),
            Command::ZScore(key, member) => executor_zset::execute_z_score(self, key, member),
            Command::ZRank(key, member) => executor_zset::execute_z_rank(self, key, member),
            Command::ZRange(key, start, stop, with_scores) => executor_zset::execute_z_range(self, key, start, stop, with_scores),
            Command::ZRangeByScore(key, min, max, with_scores) => executor_zset::execute_z_range_by_score(self, key, min, max, with_scores),
            Command::ZCard(key) => executor_zset::execute_z_card(self, key),
            Command::ZRevRange(key, start, stop, with_scores) => executor_zset::execute_z_rev_range(self, key, start, stop, with_scores),
            Command::ZRevRank(key, member) => executor_zset::execute_z_rev_rank(self, key, member),
            Command::ZIncrBy(key, increment, member) => executor_zset::execute_z_incr_by(self, key, increment, member),
            Command::ZCount(key, min, max) => executor_zset::execute_z_count(self, key, min, max),
            Command::ZPopMin(key, count) => executor_zset::execute_z_pop_min(self, key, count),
            Command::ZPopMax(key, count) => executor_zset::execute_z_pop_max(self, key, count),
            Command::ZUnionStore(destination, keys, weights, aggregate) => executor_zset::execute_z_union_store(self, destination, keys, weights, aggregate),
            Command::ZInterStore(destination, keys, weights, aggregate) => executor_zset::execute_z_inter_store(self, destination, keys, weights, aggregate),
            Command::ZScan(key, cursor, pattern, count) => executor_zset::execute_z_scan(self, key, cursor, pattern, count),
            Command::ZRangeByLex(key, min, max) => executor_zset::execute_z_range_by_lex(self, key, min, max),
            Command::SInterCard(keys, limit) => {
                let count = self.storage.sintercard(&keys, limit)?;
                Ok(RespValue::Integer(count as i64))
            }
            Command::SMisMember(key, members) => {
                let result = self.storage.smismember(&key, &members)?;
                let arr: Vec<RespValue> = result.into_iter().map(|v| RespValue::Integer(v)).collect();
                Ok(RespValue::Array(arr))
            }
            Command::ZRandMember(key, count, with_scores) => executor_zset::execute_z_rand_member(self, key, count, with_scores),
            Command::ZDiff(keys, with_scores) => executor_zset::execute_z_diff(self, keys, with_scores),
            Command::ZDiffStore(destination, keys) => executor_zset::execute_z_diff_store(self, destination, keys),
            Command::ZInter(keys, weights, aggregate, with_scores) => executor_zset::execute_z_inter(self, keys, weights, aggregate, with_scores),
            Command::ZUnion(keys, weights, aggregate, with_scores) => executor_zset::execute_z_union(self, keys, weights, aggregate, with_scores),
            Command::ZRangeStore(dst, src, min, max, by_score, by_lex, rev, limit_offset, limit_count) => executor_zset::execute_z_range_store(self, dst, src, min, max, by_score, by_lex, rev, limit_offset, limit_count),
            Command::ZMpop(keys, min_or_max, count) => executor_zset::execute_z_mpop(self, keys, min_or_max, count),
            Command::ZRevRangeByScore(key, max, min, with_scores, limit_offset, limit_count) => executor_zset::execute_z_rev_range_by_score(self, key, max, min, with_scores, limit_offset, limit_count),
            Command::ZRevRangeByLex(key, max, min, limit_offset, limit_count) => executor_zset::execute_z_rev_range_by_lex(self, key, max, min, limit_offset, limit_count),
            Command::ZMScore(key, members) => executor_zset::execute_z_m_score(self, key, members),
            Command::ZLexCount(key, min, max) => executor_zset::execute_z_lex_count(self, key, min, max),
            Command::ZRangeUnified(key, min, max, by_score, by_lex, rev, with_scores, limit_offset, limit_count) => {
                let pairs = self.storage.zrange_unified(&key, &min, &max, by_score, by_lex, rev, with_scores, limit_offset, limit_count)?;
                let mut resp_values = Vec::new();
                for (member, score) in pairs {
                    resp_values.push(RespValue::BulkString(Some(Bytes::from(member))));
                    if with_scores {
                        resp_values.push(RespValue::BulkString(Some(Bytes::from(format!("{}", score)))));
                    }
                }
                Ok(RespValue::Array(resp_values))
            }
            // 阻塞命令在非阻塞上下文中由 server.rs 处理
            Command::BZMpop(_, _, _, _) | Command::BZPopMin(_, _) | Command::BZPopMax(_, _) => {
                Ok(RespValue::BulkString(None))
            }
            Command::Keys(pattern) => executor_admin::execute_keys(self, pattern),
            Command::Scan(cursor, pattern, count) => executor_admin::execute_scan(self, cursor, pattern, count),
            Command::Rename(key, newkey) => executor_admin::execute_rename(self, key, newkey),
            Command::Type(key) => executor_admin::execute_type(self, key),
            Command::Persist(key) => executor_admin::execute_persist(self, key),
            Command::PExpire(key, ms) => executor_admin::execute_p_expire(self, key, ms),
            Command::PTtl(key) => executor_admin::execute_p_ttl(self, key),
            Command::DbSize => {
                let size = self.storage.dbsize()?;
                Ok(RespValue::Integer(size as i64))
            }
            Command::Info(section) => executor_admin::execute_info(self, section),
            Command::Subscribe(_) | Command::Unsubscribe(_) | Command::PSubscribe(_) | Command::PUnsubscribe(_) => {
                // Pub/Sub 命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("pub/sub 命令应在连接层处理".to_string()))
            }
            Command::Publish(channel, message) => {
                // PUBLISH 需要 PubSubManager，但当前执行器未持有它。
                // 为保持兼容，返回提示性错误（实际在 server.rs 中处理）
                Err(AppError::Command(format!(
                    "PUBLISH 命令应在连接层处理 (channel={}, message={})",
                    channel,
                    String::from_utf8_lossy(&message),
                )))
            }
            Command::PubSubChannels(_) | Command::PubSubNumSub(_) | Command::PubSubNumPat => {
                // PUBSUB 内省命令需要 PubSubManager，在连接层处理
                Err(AppError::Command(
                    "PUBSUB 命令应在连接层处理".to_string(),
                ))
            }
            Command::SSubscribe(_) | Command::SUnsubscribe(_) => {
                // 分片 Pub/Sub 命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("分片 pub/sub 命令应在连接层处理".to_string()))
            }
            Command::SPublish(channel, message) => {
                // SPUBLISH 需要 PubSubManager，但当前执行器未持有它。
                Err(AppError::Command(format!(
                    "SPUBLISH 命令应在连接层处理 (channel={}, message={})",
                    channel,
                    String::from_utf8_lossy(&message),
                )))
            }
            Command::PubSubShardChannels(_) | Command::PubSubShardNumSub(_) => {
                // PUBSUB 分片内省命令需要 PubSubManager，在连接层处理
                Err(AppError::Command(
                    "PUBSUB 分片命令应在连接层处理".to_string(),
                ))
            }
            Command::Multi | Command::Exec | Command::Discard | Command::Watch(_) => {
                // 事务命令在 server.rs 中直接处理，不应到达此处
                Err(AppError::Command("事务命令应在连接层处理".to_string()))
            }
            Command::Unwatch => {
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::ReplConf { args } => {
                if args.len() >= 2 && args[0].to_uppercase() == "ACK" {
                    if let Ok(offset) = args[1].parse::<i64>() {
                        if let Some(ref _repl) = self.replication {
                            log::debug!("收到副本 ACK, offset: {}", offset);
                        }
                    }
                }
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Sync => {
                // SYNC 等价于 PSYNC ? -1（强制全量同步）
                let repl = self.replication.as_ref().ok_or_else(|| {
                    AppError::Command("复制管理器未初始化".to_string())
                })?;
                let master_replid = repl.get_master_replid();
                let master_offset = repl.get_master_repl_offset();
                Ok(RespValue::SimpleString(format!(
                    "FULLRESYNC {} {}",
                    master_replid, master_offset
                )))
            }
            Command::Psync { replid, offset } => {
                let repl = self.replication.as_ref().ok_or_else(|| {
                    AppError::Command("复制管理器未初始化".to_string())
                })?;
                let master_replid = repl.get_master_replid();
                
                if offset >= 0 && replid == master_replid {
                    // 尝试增量同步：验证 offset 在 backlog 范围内
                    if let Some(_backlog_data) = repl.get_backlog_from_offset(offset) {
                        return Ok(RespValue::SimpleString(format!(
                            "CONTINUE {} {}",
                            master_replid, offset
                        )));
                    }
                    // offset 不在 backlog 范围内，降级为全量同步
                    log::info!("PSYNC offset {} 不在 backlog 范围内，降级为全量同步", offset);
                }
                
                // 全量同步
                let master_offset = repl.get_master_repl_offset();
                Ok(RespValue::SimpleString(format!(
                    "FULLRESYNC {} {}",
                    master_replid, master_offset
                )))
            }
            Command::Role => {
                let repl = self.replication.as_ref().ok_or_else(|| {
                    AppError::Command("复制管理器未初始化".to_string())
                })?;
                match repl.get_role() {
                    crate::replication::ReplicationRole::Master => {
                        let offset = repl.get_master_repl_offset();
                        let replicas = repl.get_connected_replicas();
                        let mut replica_arr = Vec::new();
                        for r in replicas {
                            replica_arr.push(RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from(r.addr))),
                                RespValue::Integer(r.port as i64),
                                RespValue::Integer(r.offset),
                            ]));
                        }
                        Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from("master"))),
                            RespValue::Integer(offset),
                            RespValue::Array(replica_arr),
                        ]))
                    }
                    crate::replication::ReplicationRole::Slave => {
                        let (host, port) = repl.get_master_host_port();
                        let offset = repl.get_master_repl_offset();
                        Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from("slave"))),
                            RespValue::BulkString(Some(Bytes::from(host.unwrap_or_default()))),
                            RespValue::Integer(port.unwrap_or(0) as i64),
                            RespValue::BulkString(Some(Bytes::from("connect"))),
                            RespValue::Integer(offset),
                        ]))
                    }
                }
            }
            Command::ReplicaOf { host, port } => {
                let repl = self.replication.as_ref().ok_or_else(|| {
                    AppError::Command("复制管理器未初始化".to_string())
                })?;
                repl.set_replicaof(host.clone(), port);
                let repl_clone = repl.clone();
                let storage_clone = self.storage.clone();
                tokio::spawn(async move {
                    if let Err(e) = repl_clone.start_replication(storage_clone, host, port).await {
                        log::error!("复制任务失败: {}", e);
                    }
                });
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::ReplicaOfNoOne => {
                let repl = self.replication.as_ref().ok_or_else(|| {
                    AppError::Command("复制管理器未初始化".to_string())
                })?;
                repl.set_replicaof_no_one();
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            Command::Wait { numreplicas, .. } => {
                // 非阻塞上下文（事务/AOF 重放）：直接返回当前满足条件的副本数
                match &self.replication {
                    Some(repl) => {
                        let offset = repl.get_master_repl_offset();
                        let count = repl.count_replicas_at_offset(offset);
                        Ok(RespValue::Integer(std::cmp::min(count, numreplicas)))
                    }
                    None => Ok(RespValue::Integer(0)),
                }
            }
            Command::Failover { .. } => {
                // FAILOVER 需要在连接层异步处理
                Err(AppError::Command("FAILOVER 应在连接层处理".to_string()))
            }
            Command::FailoverAbort => {
                // FAILOVER ABORT 也在连接层处理
                Err(AppError::Command("FAILOVER ABORT 应在连接层处理".to_string()))
            }
            Command::SentinelMasters
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
            | Command::SentinelMyId => {
                Err(AppError::Command("SENTINEL 应在连接层处理".to_string()))
            }
            Command::Unknown(cmd_name) => {
                Ok(RespValue::Error(format!(
                    "ERR unknown command '{}'",
                    cmd_name,
                )))
            }
        }
    }
}

impl std::fmt::Debug for CommandExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandExecutor")
            .field("storage", &self.storage)
            .field("aof_enabled", &self.aof.is_some())
            .finish()
    }
}

impl Clone for CommandExecutor {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            aof: self.aof.clone(),
            script_engine: self.script_engine.clone(),
            slowlog: self.slowlog.clone(),
            acl: self.acl.clone(),
            replication: self.replication.clone(),
            latency: self.latency.clone(),
            keyspace_notifier: self.keyspace_notifier.clone(),
            aof_use_rdb_preamble: self.aof_use_rdb_preamble.clone(),
        }
    }
}
